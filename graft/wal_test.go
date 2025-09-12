package graft

import (
	"fmt"
	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mizosoft/graft/testutil"
	"gotest.tools/v3/assert"
)

func TestWalNewWalOpen(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWal(WalOptions{
		Dir:         dir,
		SegmentSize: 1024,
	})
	assert.NilError(t, err)
	defer wal.Close()

	assert.Equal(t, wal.EntryCount(), int64(0))
}

func TestWalEmptyWalReopen(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWal(WalOptions{
		Dir:         dir,
		SegmentSize: 1024,
	})
	assert.NilError(t, err)
	wal.Close()

	wal, err = openWal(WalOptions{
		Dir:         dir,
		SegmentSize: 1024,
	})
	assert.NilError(t, err)
	defer wal.Close()

	assert.Equal(t, wal.EntryCount(), int64(0))
	state, err := wal.RetrieveState()
	assert.NilError(t, err)
	testutil.AssertNil(t, state)
	snapshot, err := wal.LastSnapshotMetadata()
	assert.NilError(t, err)
	testutil.AssertNil(t, snapshot)
}

func TestWalAppendSingleEntry(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			entry := &pb.LogEntry{
				Term: 1,
				Data: []byte("cmd"),
			}

			nextIndex, err := w.Append(state, []*pb.LogEntry{entry})
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(1))
			assert.Equal(t, w.EntryCount(), int64(1))

			retrievedState, err := w.RetrieveState()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(state, retrievedState))

			// Retrieve entry and verify.
			retrievedEntry, err := w.GetEntry(0)
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(retrievedEntry, entry))
			assert.Equal(t, retrievedEntry.Index, int64(0))
		})
	}
}

func TestWalAppendMultipleEntries(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd1")},
				{Term: 1, Data: []byte("cmd2")},
				{Term: 1, Data: []byte("cmd3")},
			}

			nextIndex, err := w.Append(state, entries)
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(3))

			// Retrieve and verify entries.
			retrievedEntries, err := w.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, len(retrievedEntries), len(entries))
			for i, entry := range entries {
				assert.Assert(t, proto.Equal(retrievedEntries[i], entry))
				assert.Equal(t, retrievedEntries[i].Index, int64(i))
			}
		})
	}
}

func TestWalGetEntriesFromMiddle(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("entry 0")},
				{Term: 1, Data: []byte("entry 1")},
				{Term: 1, Data: []byte("entry 2")},
				{Term: 1, Data: []byte("entry 3")},
				{Term: 1, Data: []byte("entry 4")},
			}

			_, err = w.Append(state, entries)
			assert.NilError(t, err)

			retrievedEntries, err := w.GetEntriesFrom(2)
			assert.NilError(t, err)
			assert.Equal(t, len(retrievedEntries), 3)
			assert.Assert(t, proto.Equal(retrievedEntries[0], entries[2]))
			assert.Equal(t, retrievedEntries[0].Index, int64(2))
			assert.Assert(t, proto.Equal(retrievedEntries[1], entries[3]))
			assert.Equal(t, retrievedEntries[1].Index, int64(3))
			assert.Assert(t, proto.Equal(retrievedEntries[2], entries[4]))
			assert.Equal(t, retrievedEntries[2].Index, int64(4))
		})
	}
}

func TestWalSetState(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			// Initial state
			state1 := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			err = w.SaveState(state1)
			assert.NilError(t, err)

			state, err := w.RetrieveState()
			assert.Assert(t, proto.Equal(state1, state))

			// Updated state
			state2 := &pb.PersistedState{
				CurrentTerm: 2,
				VotedFor:    "s1",
				CommitIndex: 5,
			}

			err = w.SaveState(state2)
			assert.NilError(t, err)
			state, err = w.RetrieveState()
			assert.Assert(t, proto.Equal(state2, state))
		})
	}
}

func TestWalTruncateEntries(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd0")},
				{Term: 1, Data: []byte("cmd1")},
				{Term: 1, Data: []byte("cmd2")},
				{Term: 1, Data: []byte("cmd3")},
				{Term: 1, Data: []byte("cmd4")},
			}

			_, err = w.Append(state, entries)
			assert.NilError(t, err)
			assert.Equal(t, w.EntryCount(), int64(5))

			err = w.TruncateEntriesFrom(3)
			assert.NilError(t, err)
			assert.Equal(t, w.EntryCount(), int64(3))

			retrievedEntries, err := w.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, len(retrievedEntries), 3)
			for i := range 3 {
				assert.Assert(t, proto.Equal(entries[i], retrievedEntries[i]))
			}

			// State is preserved after truncation.
			retrievedState, err := w.RetrieveState()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(state, retrievedState))
		})
	}
}

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

func TestWalReopen(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w1, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)

			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 5,
			}

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("entry 0")},
				{Term: 1, Data: []byte("entry 1")},
				{Term: 1, Data: []byte("entry 2")},
			}

			_, err = w1.Append(state, entries)
			assert.NilError(t, err)
			w1.Close()

			// Reopen.
			w2, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w2.Close()

			// Verify state was recovered.
			retrievedState, err := w2.RetrieveState()
			assert.Assert(t, proto.Equal(retrievedState, state))

			// Verify entries were recovered.
			assert.Equal(t, w2.EntryCount(), int64(3))

			recoveredEntries, err := w2.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, len(recoveredEntries), len(entries))
			for i, entry := range entries {
				assert.Assert(t, proto.Equal(entry, recoveredEntries[i]))
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

			_, err = w.GetEntry(100)
			assert.Assert(t, err != nil)

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

			fileData[w.tail().lastOffset-1] ^= 0xFF // Flip all bits at the last position.
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

func TestWalOverwriteState(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			// Set initial state.
			state1 := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}
			err = w.SaveState(state1)
			assert.NilError(t, err)

			// Update state.
			state2 := &pb.PersistedState{
				CurrentTerm: 2,
				VotedFor:    "s2",
				CommitIndex: 5,
			}
			err = w.SaveState(state2)
			assert.NilError(t, err)

			// Verify latest state is returned.
			retrievedState, err := w.RetrieveState()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(retrievedState, state2))

			// Reopen.
			w.Close()
			assert.NilError(t, err)

			w2, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w2.Close()

			// Verify latest state is recovered.
			retrievedState, err = w2.RetrieveState()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(retrievedState, state2))
		})
	}
}

func TestWalTruncateAfterStateUpdate(t *testing.T) {
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

			// Set initial state.
			state := &pb.PersistedState{
				CurrentTerm: 1,
				VotedFor:    "s1",
				CommitIndex: 0,
			}
			nextIndex, err := w1.Append(state, []*pb.LogEntry{
				{Term: 1, Data: []byte("a")},
				{Term: 2, Data: []byte("b")},
			})
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(2))

			err = w1.TruncateEntriesFrom(0)
			assert.NilError(t, err)

			w1.Close()

			// Reopen.
			w2, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w2.Close()

			retrievedState, err := w2.RetrieveState()
			assert.Assert(t, proto.Equal(retrievedState, state))
		})
	}
}

func TestWalAppendAfterTruncating(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			nextIndex, err := w.Append(nil, []*pb.LogEntry{
				{Term: 1, Data: []byte("a")},
				{Term: 2, Data: []byte("b")},
			})
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(2))

			err = w.TruncateEntriesFrom(1)
			assert.NilError(t, err)

			nextIndex, err = w.Append(nil, []*pb.LogEntry{
				{Term: 3, Data: []byte("c")},
				{Term: 4, Data: []byte("d")},
			})
			assert.NilError(t, err)
			assert.Equal(t, nextIndex, int64(3))

			retrievedEntries, err := w.GetEntriesFrom(0)
			assert.NilError(t, err)
			assert.Equal(t, len(retrievedEntries), 3, len(retrievedEntries))

			expectedEntries := []*pb.LogEntry{
				{Term: 1, Index: 0, Data: []byte("a")},
				{Term: 3, Index: 1, Data: []byte("c")},
				{Term: 4, Index: 2, Data: []byte("d")},
			}
			for i := 0; i < len(expectedEntries); i++ {
				assert.Assert(t, proto.Equal(retrievedEntries[i], expectedEntries[i]))
			}
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

func TestWalHeadEntry(t *testing.T) {
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

			entry, err := w.HeadEntry()
			assert.NilError(t, err)
			testutil.AssertNil(t, entry)

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd1")},
				{Term: 2, Data: []byte("cmd2")},
			}

			lastIndex, err := w.Append(nil, entries)
			assert.NilError(t, err)
			assert.Equal(t, lastIndex, int64(2))

			entry, err = w.HeadEntry()
			assert.NilError(t, err)
			assert.Equal(t, entry.Index, int64(0))
			assert.Assert(t, proto.Equal(entry, entries[0]))
		})
	}
}

func TestWalTailEntry(t *testing.T) {
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

			entry, err := w.TailEntry()
			assert.NilError(t, err)
			testutil.AssertNil(t, entry)

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd1")},
				{Term: 2, Data: []byte("cmd2")},
			}

			lastIndex, err := w.Append(nil, entries)
			assert.NilError(t, err)
			assert.Equal(t, lastIndex, int64(2))

			entry, err = w.TailEntry()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(entry, entries[1]))
			assert.Equal(t, entry.Index, int64(1))
		})
	}
}

func TestWalTailEntryWithEmptySegments(t *testing.T) {
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

			entry, err := w.TailEntry()
			assert.NilError(t, err)
			testutil.AssertNil(t, entry)

			entries := []*pb.LogEntry{
				{Term: 1, Data: []byte("cmd1")},
				{Term: 1, Data: []byte("cmd2")},
			}
			lastIndex, err := w.Append(nil, entries)
			assert.NilError(t, err)
			assert.Equal(t, lastIndex, int64(2))

			// Create a couple of segments without entries at the end.
			for i := range 50 {
				err = w.SaveState(&pb.PersistedState{CurrentTerm: int64(i), VotedFor: "s1", CommitIndex: 0})
				assert.NilError(t, err)
			}

			assert.Assert(t, len(w.segments) > 2, len(w.segments))

			entry, err = w.TailEntry()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(entry, entries[1]))
			assert.Equal(t, entry.Index, int64(1))
		})
	}
}

// TODO may want to move to testutil.
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

func TestWalSaveAndRetrieveSnapshot(t *testing.T) {
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

			retrievedMetadata, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)
			assert.Assert(t, retrievedMetadata == nil)

			data := []byte("Pikachu")
			metadata := &pb.SnapshotMetadata{
				LastAppliedIndex: 1,
				LastAppliedTerm:  1,
				ConfigUpdate: &pb.ConfigUpdate{
					Old: []*pb.NodeConfig{
						{
							Id:      "s1",
							Address: "127.0.0.1:1234",
						},
					},
					New: []*pb.NodeConfig{
						{
							Id:      "s2",
							Address: "127.0.0.2:1234",
						},
					},
					Phase: pb.ConfigUpdate_LEARNING,
				},
			}
			writer, err := w.CreateSnapshot(metadata)
			assert.NilError(t, err)

			n, err := writer.WriteAt(data[0:2], 0)
			assert.NilError(t, err)
			assert.Equal(t, n, 2)

			n, err = writer.WriteAt(data[2:], 2)
			assert.NilError(t, err)
			assert.Equal(t, n, len(data)-2)

			committedMetadata, err := writer.Commit()
			assert.NilError(t, err)
			assert.Equal(t, committedMetadata.Size, int64(len(data)))

			metadata.Size = int64(len(data))
			assert.Assert(t, proto.Equal(committedMetadata, metadata))

			retrievedMetadata, err = w.LastSnapshotMetadata()
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(retrievedMetadata, metadata))

			snapshot, err := w.OpenSnapshot(metadata)
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(snapshot.Metadata(), metadata))

			retrievedData, err := snapshot.ReadAll()
			assert.NilError(t, err)
			assert.Equal(t, string(retrievedData), string(data))
		})
	}
}

func TestWalDiscardedSnapshot(t *testing.T) {
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

			writer, err := w.CreateSnapshot(&pb.SnapshotMetadata{
				LastAppliedIndex: 1,
				LastAppliedTerm:  1,
				ConfigUpdate: &pb.ConfigUpdate{
					Old: []*pb.NodeConfig{
						{
							Id:      "s1",
							Address: "127.0.0.1:1234",
						},
					},
					New: []*pb.NodeConfig{
						{
							Id:      "s2",
							Address: "127.0.0.2:1234",
						},
					},
					Phase: pb.ConfigUpdate_LEARNING,
				},
			})
			assert.NilError(t, err)

			n, err := writer.WriteAt([]byte("abc"), 0)
			assert.NilError(t, err)
			assert.Equal(t, n, 3)

			err = writer.Close()
			assert.NilError(t, err)

			retrievedMetadata, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)
			testutil.AssertNil(t, retrievedMetadata)
		})
	}
}

func TestWalDiscardedSnapshotAfterCommittedSnapshot(t *testing.T) {
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

			metadata1 := &pb.SnapshotMetadata{
				LastAppliedIndex: 1,
				LastAppliedTerm:  1,
				ConfigUpdate: &pb.ConfigUpdate{
					Old: []*pb.NodeConfig{
						{
							Id:      "s1",
							Address: "127.0.0.1:1234",
						},
					},
					New: []*pb.NodeConfig{
						{
							Id:      "s2",
							Address: "127.0.0.2:1234",
						},
					},
					Phase: pb.ConfigUpdate_LEARNING,
				},
			}

			writer1, err := w.CreateSnapshot(metadata1)
			assert.NilError(t, err)

			n, err := writer1.WriteAt([]byte("abc"), 0)
			assert.NilError(t, err)
			assert.Equal(t, n, 3)

			_, err = writer1.Commit()
			assert.NilError(t, err)

			snapshot, err := w.OpenSnapshot(metadata1)
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(snapshot.Metadata(), metadata1))

			retrievedData, err := snapshot.ReadAll()
			assert.Equal(t, string(retrievedData), "abc")

			metadata2 := cloneMsg(metadata1)
			metadata2.LastAppliedIndex, metadata2.LastAppliedTerm = 2, 2
			writer2, err := w.CreateSnapshot(metadata1)
			assert.NilError(t, err)

			n, err = writer2.WriteAt([]byte("bde"), 0)
			assert.NilError(t, err)
			assert.Equal(t, n, 3)

			// Discard.
			err = writer2.Close()
			assert.NilError(t, err)

			retrievedMetadata, err := w.LastSnapshotMetadata()
			assert.NilError(t, err)

			metadata1.Size = int64(len("abc"))
			assert.Assert(t, proto.Equal(retrievedMetadata, metadata1))

			snapshot, err = w.OpenSnapshot(metadata1)
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(snapshot.Metadata(), metadata1))

			retrievedData, err = snapshot.ReadAll()
			assert.Equal(t, string(retrievedData), "abc")
		})
	}
}

func TestWalRetrieveSnapshotOnReopen(t *testing.T) {
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

			metadata := &pb.SnapshotMetadata{
				LastAppliedIndex: 1,
				LastAppliedTerm:  1,
				ConfigUpdate: &pb.ConfigUpdate{
					Old: []*pb.NodeConfig{
						{
							Id:      "s1",
							Address: "127.0.0.1:1234",
						},
					},
					New: []*pb.NodeConfig{
						{
							Id:      "s2",
							Address: "127.0.0.2:1234",
						},
					},
					Phase: pb.ConfigUpdate_LEARNING,
				},
			}
			data := []byte("Pikachu")

			writer, err := w1.CreateSnapshot(metadata)
			assert.NilError(t, err)

			n, err := writer.WriteAt(data, 0)
			assert.NilError(t, err)
			assert.Equal(t, n, len(data))

			_, err = writer.Commit()
			assert.NilError(t, err)

			w1.Close()

			w2, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  512,
			})
			assert.NilError(t, err)
			defer w2.Close()

			retrievedSnapshot, err := w2.OpenSnapshot(metadata)
			assert.NilError(t, err)
			assert.Assert(t, proto.Equal(retrievedSnapshot.Metadata(), metadata))

			retrievedData, err := retrievedSnapshot.ReadAll()
			assert.Equal(t, string(retrievedData), string(data))
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

func TestWalFirstAndLastEntryIndex(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			dir := t.TempDir()
			w, err := openWal(WalOptions{
				Dir:          dir,
				MemoryMapped: memoryMapped,
				SegmentSize:  1024,
			})
			assert.NilError(t, err)
			defer w.Close()

			firstIndex, err := w.FirstEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, int64(-1), firstIndex)
			lastIndex, err := w.LastEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, int64(-1), lastIndex)

			_, err = w.Append(nil, []*pb.LogEntry{
				{
					Term: 2,
					Data: []byte("foo"),
				},
				{
					Term: 3,
					Data: []byte("bar"),
				},
			})
			assert.NilError(t, err)

			firstIndex, err = w.FirstEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, int64(0), firstIndex)
			lastIndex, err = w.LastEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, int64(1), lastIndex)

			err = w.TruncateEntriesTo(0)
			assert.NilError(t, err)

			_, err = w.Append(nil, []*pb.LogEntry{
				{
					Term: 4,
					Data: []byte("bar"),
				},
			})
			assert.NilError(t, err)

			firstIndex, err = w.FirstEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, int64(1), firstIndex)
			lastIndex, err = w.LastEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, int64(2), lastIndex)
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
