package graft

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"

	graft_testing "github.com/mizosoft/graft/testing"
	"gotest.tools/v3/assert"
)

func TestWalMultipleSegments(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w.Close()

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			// Fill multiple segments.
			entryCount := 30
			entries := make([]*pb.LogEntry, entryCount)
			for i := range entryCount {
				entries[i] = &pb.LogEntry{
					Term: 1,
					Data: []byte(strings.Repeat("entry data that is long enough to force segment creation", 3)),
				}
			}

			// Append in batches to create multiple segments.
			nextIndex, err := w.Append(state, entries)
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(30))

			assert.Assert(t, len(w.segments) > 1, len(w.segments))

			// All entries can be retrieved.
			allEntries, err := w.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, len(allEntries), entryCount)

			// Check getting individual entries across segments.
			for i := range entryCount {
				entry, err := w.GetEntry(int64(i))
				assert.NilError(t, err)
				assert.Assert(t, entry != nil)
			}
		})
	}
}

func TestWalErrorCases(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			defer w.Close()
			assert.NilError(t, err)

			assertPanics(t, "ends past the last index -1", func() { _, _ = w.GetEntry(100) })

			// Test using closed WAL
			w.Close()

			_, err = w.GetEntry(0)
			assert.Assert(t, err != nil)
		})
	}
}

func TestWalCorruptedCRC(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			defer w.Close()
			assert.NilError(t, err)

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			entry := &pb.LogEntry{
				Term: 1,
				Data: []byte("test"),
			}

			_, err = w.Append(state, []*pb.LogEntry{entry})
			assert.NilError(t, err)
			assert.Equal(t, len(w.segments), 1)

			// Corrupt the file.
			files, err := os.ReadDir(dir)
			assert.NilError(t, err)
			assert.Equal(t, len(files), 1)

			filePath := filepath.Join(dir, files[0].Name())
			fileData, err := os.ReadFile(filePath)
			assert.NilError(t, err)

			fileData[w.tail().nextOffset-1] ^= 0xFF // Flip all bits at the last position.
			err = os.WriteFile(filePath, fileData, 0644)
			assert.NilError(t, err)

			// Open log with corrupt segment.
			w, err = openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.Assert(t, err != nil, "Expected error when opening corrupted WAL")
		})
	}
}

func TestWalTruncateMultipleSegments(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w.Close()

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			// Fill multiple segments.
			entryCount := 30
			entries := make([]*pb.LogEntry, entryCount)
			for i := range entryCount {
				entries[i] = &pb.LogEntry{
					Term: 1,
					Data: []byte(strings.Repeat("they see me rollin' they hatin'", 3)),
				}
			}

			nextIndex, err := w.Append(state, entries[0:10])
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(10))
			nextIndex, err = w.Append(state, entries[10:20])
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(20))
			nextIndex, err = w.Append(state, entries[20:entryCount])
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(30))

			segmentCount := len(w.segments)
			assert.Assert(t, segmentCount > 1, segmentCount)

			// Truncate from an entry in the first segment.
			err = w.TruncateEntriesFrom(5)
			assert.NilError(t, err)
			assert.Assert(t, len(w.segments) < segmentCount)

			// Check remaining entries.
			assert.Equal(t, w.EntryCount(), int64(5))
			remaining, err := w.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, 5, len(remaining))

			// Append after truncation.
			newEntries := []*pb.LogEntry{
				{Term: 2, Data: []byte("e5")},
				{Term: 2, Data: []byte("e6")},
			}
			_, err = w.Append(state, newEntries)
			assert.NilError(t, err)
			assert.Equal(t, w.EntryCount(), int64(7))

			finalEntries, err := w.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, len(finalEntries), 7)

			for i := range 5 {
				assert.Assert(t, proto.Equal(entries[i], finalEntries[i]))
			}
			assert.Assert(t, proto.Equal(newEntries[0], finalEntries[5]))
			assert.Assert(t, proto.Equal(newEntries[1], finalEntries[6]))
		})
	}
}

// TODO may want to move to graft_testing.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func TestWalMismatchingSegNumber(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w1, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w1.Close()

			lastIndex, err := w1.Append(nil, []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd1")},
			})
			assert.NilError(t, err)
			assert.Equal(t, lastIndex, int64(1))
			assert.Assert(t, len(w1.segments) == 1, len(w1.segments))

			_, fname := path.Split(w1.tail().fpath)
			assert.Equal(t, fname, "log_0_0.dat")

			w1.Close()

			if err := os.Rename(w1.tail().fpath, path.Join(dir, "log_1_0.dat")); err != nil {
				assert.NilError(t, err)
			}

			w2, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w2.Close()

			_, fname = path.Split(w2.tail().fpath)
			assert.Equal(t, fname, "log_0_0.dat")
			assert.Equal(t, w2.tail().number, 0)
			assert.Assert(t, fileExists(path.Join(dir, "log_0_0.dat")))
			assert.Assert(t, !fileExists(path.Join(dir, "log_1_0.dat")))
		})
	}
}

func TestWalMismatchingFirstIndex(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w1, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w1.Close()

			lastIndex, err := w1.Append(nil, []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd1")},
			})
			assert.NilError(t, err)
			assert.Equal(t, lastIndex, int64(1))
			assert.Assert(t, len(w1.segments) == 1, len(w1.segments))

			_, fname := path.Split(w1.tail().fpath)
			assert.Equal(t, fname, "log_0_0.dat")

			w1.Close()

			if err := os.Rename(w1.tail().fpath, path.Join(dir, "log_0_1.dat")); err != nil {
				assert.NilError(t, err)
			}

			w2, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w1.Close()

			_, fname = path.Split(w2.tail().fpath)
			assert.Equal(t, fname, "log_0_0.dat")
			assert.Equal(t, w2.tail().firstIndex, int64(0))
			assert.Assert(t, fileExists(path.Join(dir, "log_0_0.dat")))
			assert.Assert(t, !fileExists(path.Join(dir, "log_0_1.dat")))
		})
	}
}

func TestWalGetEntriesFromTo(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w.Close()

			entryCount := 40
			var entries []*pb.LogEntry
			for range entryCount / 2 {
				term := int64(len(entries))
				entries = append(entries, &pb.LogEntry{
					Term: term,
					Data: []byte("abc"),
				})
			}

			checkEntries := func(retrieved []*pb.LogEntry, expected []*pb.LogEntry) {
				assert.Equal(t, len(retrieved), len(expected))
				for j, entry := range expected {
					assert.Assert(t, proto.Equal(retrieved[j], entry), retrieved[j].String(), entry.String())
				}
			}

			for i := range entries {
				state := &pb.PersistedState{CurrentTerm: int64(i), VotedFor: "s1"}
				nextIndex, err := w.Append(state, []*pb.LogEntry{entries[i]})
				assert.NilError(t, err)
				assert.Equal(t, nextIndex, int64(i+1))

				retrieved, err := w.GetEntries(0, int64(i))
				assert.NilError(t, err)
				checkEntries(retrieved, entries[:i+1])
			}

			// Put a bunch of states to make segments with not entries.
			segCount := len(w.segments)
			lastIndex, err := w.LastEntryIndex()
			assert.NilError(t, err)
			lastTerm, err := w.GetEntryTerm(lastIndex)
			assert.NilError(t, err)
			for segCount == len(w.segments) {
				err := w.SaveState(&pb.PersistedState{CurrentTerm: lastTerm, VotedFor: "s1"})
				assert.NilError(t, err)
				lastTerm++
			}

			for range entryCount / 2 {
				term := int64(len(entries))
				entries = append(entries, &pb.LogEntry{
					Term: term,
					Data: []byte("abc"),
				})
			}

			for i := entryCount / 2; i < entryCount; i++ {
				state := &pb.PersistedState{CurrentTerm: int64(i), VotedFor: "s1"}
				nextIndex, err := w.Append(state, []*pb.LogEntry{entries[i]})
				assert.NilError(t, err)
				assert.Equal(t, nextIndex, int64(i+1))

				retrieved, err := w.GetEntries(0, int64(i))
				assert.NilError(t, err)
				checkEntries(retrieved, entries[:i+1])
			}

			lastIndex, err = w.LastEntryIndex()
			assert.NilError(t, err)
			for i := range entries {
				retrieved, err := w.GetEntries(int64(i), lastIndex)
				assert.NilError(t, err)
				checkEntries(retrieved, entries[i:])
			}

			// Try all valid index sequences.
			for i := 0; i < entryCount; i++ {
				for j := i; j < entryCount; j++ {
					retrieved, err := w.GetEntries(int64(i), int64(j))
					assert.NilError(t, err)
					checkEntries(retrieved, entries[i:j+1])
				}
			}
		})
	}
}

func TestWalSnapshotChecksumIsSet(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{Dir: dir, MemoryMapped: memoryMapped, SegmentSize: 512})
			assert.NilError(t, err)
			defer w.Close()

			data := []byte("Squirtle")
			metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
			writer, err := w.CreateSnapshot(metadata)
			assert.NilError(t, err)

			_, err = writer.WriteAt(data, 0)
			assert.NilError(t, err)
			assert.NilError(t, writer.Commit())

			retrieved, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)
			assert.Assert(t, retrieved.Checksum != 0)
		})
	}
}

func TestWalSnapshotChecksumVerifiedOnReadAll(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{Dir: dir, MemoryMapped: memoryMapped, SegmentSize: 512})
			assert.NilError(t, err)
			defer w.Close()

			data := []byte("Bulbasaur")
			metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
			writer, err := w.CreateSnapshot(metadata)
			assert.NilError(t, err)
			_, err = writer.WriteAt(data, 0)
			assert.NilError(t, err)
			assert.NilError(t, writer.Commit())

			// Corrupt the snapshot file.
			snapPath := path.Join(dir, snapshotFilename(metadata))
			f, err := os.OpenFile(snapPath, os.O_RDWR, 0644)
			assert.NilError(t, err)
			_, err = f.WriteAt([]byte{0xFF}, 0)
			assert.NilError(t, err)
			assert.NilError(t, f.Close())

			retrieved, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)
			snap, err := w.OpenSnapshot(retrieved)
			assert.NilError(t, err)
			defer snap.Close()

			_, err = snap.ReadAll()
			assert.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestWalSnapshotChecksumVerifiedOnReader(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{Dir: dir, MemoryMapped: memoryMapped, SegmentSize: 512})
			assert.NilError(t, err)
			defer w.Close()

			data := []byte("Charmander")
			metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
			writer, err := w.CreateSnapshot(metadata)
			assert.NilError(t, err)
			_, err = writer.WriteAt(data, 0)
			assert.NilError(t, err)
			assert.NilError(t, writer.Commit())

			// Corrupt the snapshot file.
			snapPath := path.Join(dir, snapshotFilename(metadata))
			f, err := os.OpenFile(snapPath, os.O_RDWR, 0644)
			assert.NilError(t, err)
			_, err = f.WriteAt([]byte{0xFF}, 0)
			assert.NilError(t, err)
			assert.NilError(t, f.Close())

			retrieved, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)
			snap, err := w.OpenSnapshot(retrieved)
			assert.NilError(t, err)
			defer snap.Close()

			_, err = io.ReadAll(snap.Reader())
			assert.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestWalTruncateEntriesTo(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w.Close()

			rnd := rand.New(rand.NewSource(0))
			entryCount := int64(100)
			entries := make([]*pb.LogEntry, entryCount)
			for i := range entries {
				commandSize := rnd.Intn(64)
				entries[i] = &pb.LogEntry{
					Index: int64(i),
					Term:  int64(i),
					Data:  []byte(strings.Repeat("a", commandSize)),
				}
			}

			for _, entry := range entries {
				_, err = w.Append(nil, []*pb.LogEntry{entry})
				assert.NilError(t, err)
			}

			assert.Assert(t, len(w.segments) > 1)

			for i := int64(0); i < entryCount; i++ {
				err = w.TruncateEntriesTo(i)
				assert.NilError(t, err)

				firstIndex, err := w.FirstEntryIndex()
				assert.NilError(t, err)
				if i < entryCount-1 {
					assert.Equal(t, firstIndex, i+1)

					retrievedEntries, err := w.GetEntriesFrom(firstIndex)
					assert.NilError(t, err)
					for j, entry := range entries[i+1:] {
						assert.Assert(t, proto.Equal(retrievedEntries[j], entry))
					}
				} else {
					assert.Equal(t, firstIndex, int64(-1))
				}
			}
		})
	}
}

func TestWalAppendAfterTruncate(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w.Close()

			entriesCount := 10
			entries := make([]*pb.LogEntry, entriesCount)
			for i := 0; i < entriesCount; i++ {
				entries[i] = &pb.LogEntry{
					Term: 1,
					Data: []byte(strings.Repeat("long message to create multiple segments", 10)),
				}
			}

			_, err = w.Append(nil, entries)
			assert.NilError(t, err)
			assert.Assert(t, len(w.segments) > 1)

			err = w.TruncateEntriesTo(entries[entriesCount-1].Index)
			assert.NilError(t, err)

			lastIndex := entries[entriesCount-1].Index
			_, err = w.Append(nil, entries)
			assert.NilError(t, err)
			assert.Equal(t, entries[0].Index, lastIndex+1)

			retrievedEntries, err := w.GetEntriesFrom(lastIndex + 1)
			assert.NilError(t, err)
			assert.Equal(t, len(retrievedEntries), entriesCount)
			for i := 0; i < entriesCount; i++ {
				assert.Assert(t, proto.Equal(retrievedEntries[i], entries[i]))
			}
		})
	}
}

// A failure after the rename must remove the renamed file: os.File.Name still reports the .tmp path, and cleaning that
// up instead leaves a snapshot on disk that nothing references and nothing ever deletes.
func TestWalSnapshotCommitFailureAfterRenameLeavesNoFile(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w.Close()

			// Too large for a segment, so saveSnapshotMetadata fails with ErrLargeRecord - after the rename.
			metadata := &pb.SnapshotMetadata{
				LastIncludedIndex: 1,
				LastIncludedTerm:  1,
				ConfigUpdate: &pb.ConfigUpdate{
					New:   []*pb.NodeConfig{{Id: "s1", Url: strings.Repeat("x", 1024)}},
					Phase: pb.ConfigUpdate_APPLIED,
				},
			}
			writer, err := w.CreateSnapshot(metadata)
			assert.NilError(t, err)
			_, err = writer.WriteAt([]byte("abc"), 0)
			assert.NilError(t, err)

			assert.ErrorIs(t, writer.Commit(), ErrLargeRecord)

			entries, err := os.ReadDir(dir)
			assert.NilError(t, err)
			for _, e := range entries {
				assert.Assert(t, !strings.HasPrefix(e.Name(), "snap_"), "orphaned snapshot file left behind: %s", e.Name())
			}

			retrievedMetadata, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)
			graft_testing.AssertNil(t, retrievedMetadata)
		})
	}
}

// Append returns with everything it wrote synced, including segments it moved past, whether its entries or the state
// saved after them spilled into the next.
func TestWalAppendLeavesNoSegmentUnsynced(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		for _, spill := range []struct {
			name        string
			count       int
			votedFor    string
			stateSpills bool
		}{{"state", 6, strings.Repeat("v", 400), true}, {"entries", 20, "", false}} {
			t.Run(fmt.Sprintf("MemoryMapped=%t/%s", memoryMapped, spill.name), func(t *testing.T) {
				w, err := openWal(WalOptions{Dir: t.TempDir(), SegmentSize: 1024, MemoryMapped: memoryMapped})
				assert.NilError(t, err)
				defer w.Close()

				entries := make([]*pb.LogEntry, spill.count)
				for i := range entries {
					entries[i] = &pb.LogEntry{Term: 1, Data: []byte(strings.Repeat("a", 100))}
				}
				_, err = w.Append(&pb.PersistedState{CurrentTerm: 1, VotedFor: spill.votedFor}, entries)
				assert.NilError(t, err)
				assert.Assert(t, len(w.segments) > 1, "nothing spilled into a new segment")
				assert.Equal(t, w.tail().entryCount() == 0, spill.stateSpills, "a different record spilled")
				for _, s := range w.segments {
					assert.Assert(t, !s.dirty, "segment %d was left unsynced", s.number)
				}
			})
		}
	}
}

// Every public method that writes returns with what it wrote synced.
func TestWalWritesLeaveNoSegmentUnsynced(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		for _, write := range []struct {
			name string
			run  func(w *wal) error
		}{
			{"SaveState", func(w *wal) error {
				return w.SaveState(&pb.PersistedState{CurrentTerm: 2, CommitIndex: -1})
			}},
			{"Append with no entries", func(w *wal) error {
				_, err := w.Append(&pb.PersistedState{CurrentTerm: 2, CommitIndex: -1}, nil)
				return err
			}},
			{"TruncateEntriesFrom", func(w *wal) error { return w.TruncateEntriesFrom(2) }},
			{"TruncateEntriesTo", func(w *wal) error { return w.TruncateEntriesTo(2) }},
			{"Commit", func(w *wal) error {
				writer, err := w.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 1})
				if err != nil {
					return err
				}
				if _, err := writer.WriteAt([]byte("Snorlax"), 0); err != nil {
					return err
				}
				return writer.Commit()
			}},
		} {
			t.Run(fmt.Sprintf("MemoryMapped=%t/%s", memoryMapped, write.name), func(t *testing.T) {
				w, err := openWal(WalOptions{Dir: t.TempDir(), SegmentSize: 1024, MemoryMapped: memoryMapped})
				assert.NilError(t, err)
				defer w.Close()

				entries := make([]*pb.LogEntry, 5)
				for i := range entries {
					entries[i] = &pb.LogEntry{Term: 1, Data: []byte("Snorlax")}
				}
				_, err = w.Append(&pb.PersistedState{CurrentTerm: 1, CommitIndex: -1}, entries)
				assert.NilError(t, err)
				assert.NilError(t, write.run(w))
				for _, s := range w.segments {
					assert.Assert(t, !s.dirty, "segment %d was left unsynced", s.number)
				}
			})
		}
	}
}
