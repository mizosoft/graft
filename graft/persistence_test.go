package graft

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

func persistenceBackends(t *testing.T) map[string]Persistence {
	t.Helper()

	w, err := OpenWal(WalOptions{Dir: t.TempDir(), SegmentSize: 1024})
	assert.NilError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return map[string]Persistence{
		"memory": MemoryPersistence(),
		"wal":    w,
	}
}

func appendCommands(t *testing.T, p Persistence, count int) {
	t.Helper()

	entries := make([]*pb.LogEntry, count)
	for i := range entries {
		entries[i] = &pb.LogEntry{
			Term: 1,
			Type: pb.LogEntry_COMMAND,
			Data: []byte{byte(i)},
		}
	}
	_, err := p.Append(&pb.PersistedState{CurrentTerm: 1, CommitIndex: -1}, entries)
	assert.NilError(t, err)
}

func TestGetEntriesBoundsAreInclusive(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 10)

			single, err := p.GetEntries(3, 3)
			assert.NilError(t, err)
			assert.Equal(t, len(single), 1, "GetEntries(i, i) must return one entry")
			assert.Equal(t, single[0].Index, int64(3))

			span, err := p.GetEntries(2, 5)
			assert.NilError(t, err)
			assert.Equal(t, len(span), 4, "GetEntries(2, 5) must return indices 2..5")
			assert.Equal(t, span[0].Index, int64(2))
			assert.Equal(t, span[len(span)-1].Index, int64(5))

			all, err := p.GetEntries(0, 9)
			assert.NilError(t, err)
			assert.Equal(t, len(all), 10)
		})
	}
}

// A range ending past the log, or running backwards, is a bug in the caller, so it panics where it's found.
func TestLogAccessPastTheEndPanics(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 5)

			assertPanics(t, "ends past the last index 4", func() { _, _ = p.GetEntries(0, 5) })
			assertPanics(t, "ends past the last index 4", func() { _, _ = p.GetEntry(5) })
			assertPanics(t, "ends past the last index 4", func() { _, _ = p.GetEntriesFrom(5) })
			assertPanics(t, "ends past the last index 4", func() { _ = p.TruncateEntriesFrom(5) })
			assertPanics(t, "invalid index range [3, 2]", func() { _, _ = p.GetEntries(3, 2) })
		})
	}
}

// Before the first index is where a compaction lands between a caller's bounds check and its read.
func TestLogAccessBeforeTheFirstIndexIsCompacted(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 10)
			assert.NilError(t, p.TruncateEntriesTo(4))

			_, err := p.GetEntries(3, 6)
			assert.ErrorIs(t, err, ErrCompacted)
			_, err = p.GetEntry(4)
			assert.ErrorIs(t, err, ErrCompacted)
			_, err = p.GetEntryTerm(4)
			assert.ErrorIs(t, err, ErrCompacted)
			_, err = p.GetEntriesFrom(0)
			assert.ErrorIs(t, err, ErrCompacted)
			assert.ErrorIs(t, p.TruncateEntriesFrom(2), ErrCompacted)

			entries, err := p.GetEntries(5, 9)
			assert.NilError(t, err)
			assert.Equal(t, entries[0].Index, int64(5))
		})
	}
}

// Compaction can be requested again, or beyond the log: before the first index does nothing, past the last compacts
// everything, and the log keeps its position.
func TestTruncateEntriesToClamps(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 10)
			assert.NilError(t, p.TruncateEntriesTo(4))
			assert.NilError(t, p.TruncateEntriesTo(2))
			first, err := p.FirstEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, first, int64(5))

			assert.NilError(t, p.TruncateEntriesTo(100))
			assert.Equal(t, p.EntryCount(), int64(0))
			_, err = p.GetEntry(9)
			assert.ErrorIs(t, err, ErrCompacted)
		})
	}
}

// Compacting the whole log must not rewind where new entries append. The wal keeps
// a segment behind to hold the position; memoryPersistence tracks nextIndex.
func TestAppendContinuesAfterFullCompaction(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 10)
			assert.NilError(t, p.TruncateEntriesTo(9))

			first, err := p.FirstEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, first, int64(-1), "log should now be empty")

			appendCommands(t, p, 1)
			last, err := p.LastEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, last, int64(10), "next entry must be 10, not 0 - reusing an index a snapshot already covers corrupts the log")
		})
	}
}

func TestGetEntriesAfterCompaction(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 10)
			assert.NilError(t, p.TruncateEntriesTo(4))

			first, err := p.FirstEntryIndex()
			assert.NilError(t, err)
			assert.Equal(t, first, int64(5), "entries 0..4 should be gone")

			// Absolute indices must keep working after the head is dropped.
			entries, err := p.GetEntries(5, 7)
			assert.NilError(t, err)
			assert.Equal(t, len(entries), 3)
			assert.Equal(t, entries[0].Index, int64(5))
			assert.Equal(t, entries[2].Index, int64(7))

			entry, err := p.GetEntry(9)
			assert.NilError(t, err)
			assert.Equal(t, entry.Index, int64(9))

			_, err = p.GetEntries(4, 6)
			assert.Assert(t, err != nil, "from below FirstEntryIndex must fail")
		})
	}
}

// A local snapshot and one installed by the leader can commit in either order. The older one landing last must neither
// become current nor delete the newer one as its predecessor.
func TestSnapshotCommitRejectsOlder(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			older, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 1})
			assert.NilError(t, err)
			_, err = older.WriteAt([]byte("three"), 0)
			assert.NilError(t, err)

			assert.NilError(t, commitSnapshot(p, 5, "five"))
			assert.ErrorIs(t, older.Commit(), ErrSnapshotOutOfDate)
			assertCurrentSnapshot(t, p, 5, "five")
		})
	}
}

// A snapshot that isn't newer than the current one would be rejected on commit, so it isn't written at all.
func TestSnapshotCreateRejectsNotNewer(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			assert.NilError(t, commitSnapshot(p, 5, "five"))
			for _, index := range []int64{3, 5} {
				_, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
				assert.ErrorIs(t, err, ErrSnapshotOutOfDate)
			}
			assertCurrentSnapshot(t, p, 5, "five")
		})
	}
}

// Two writers can hold the same metadata at once, e.g. a local snapshot and a leader's install at the same index.
func TestSnapshotWritersWithSameMetadataCoexist(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			metadata := &pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1}
			w1, err := p.CreateSnapshot(metadata)
			assert.NilError(t, err)
			w2, err := p.CreateSnapshot(proto.Clone(metadata).(*pb.SnapshotMetadata))
			assert.NilError(t, err, "a second writer for the same snapshot couldn't be created")

			_, err = w1.WriteAt([]byte("one"), 0)
			assert.NilError(t, err)
			_, err = w2.WriteAt([]byte("two"), 0)
			assert.NilError(t, err)

			assert.NilError(t, w1.Commit())
			assert.ErrorIs(t, w2.Commit(), ErrSnapshotOutOfDate)
			assertCurrentSnapshot(t, p, 5, "one")
		})
	}
}

// A write can jump back over bytes written already, and the snapshot reads back as finally written.
func TestSnapshotWritesCanJumpBack(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			writer, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1})
			assert.NilError(t, err)
			for _, write := range []struct {
				data string
				off  int64
			}{{"aaaa", 0}, {"bbbb", 4}, {"cc", 2}} {
				_, err := writer.WriteAt([]byte(write.data), write.off)
				assert.NilError(t, err)
			}
			assert.NilError(t, writer.Commit())
			assertCurrentSnapshot(t, p, 1, "aaccbbbb")
		})
	}
}

// A writer that a newer commit makes stale fails from its next write on, rather than writing to the end first.
func TestSnapshotWritersAreDiscardedByNewerCommits(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			older, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 1})
			assert.NilError(t, err)
			newer, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: 7, LastIncludedTerm: 1})
			assert.NilError(t, err)

			assert.NilError(t, commitSnapshot(p, 5, "five"))
			_, err = older.WriteAt([]byte("three"), 0)
			assert.ErrorIs(t, err, ErrSnapshotOutOfDate)
			assert.ErrorIs(t, older.Commit(), ErrSnapshotOutOfDate)

			_, err = newer.WriteAt([]byte("seven"), 0)
			assert.NilError(t, err)
			assert.NilError(t, newer.Commit())
			assertCurrentSnapshot(t, p, 7, "seven")
		})
	}
}

// Closing the persistence fails the snapshot writers still open.
func TestSnapshotWritersFailOnceClosed(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			writer, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1})
			assert.NilError(t, err)
			assert.NilError(t, p.Close())

			_, err = writer.WriteAt([]byte("one"), 0)
			assert.ErrorIs(t, err, ErrClosed)
			assert.ErrorIs(t, writer.Commit(), ErrClosed)
		})
	}
}

func TestConcurrentSnapshotCommitsLeaveNewestCurrent(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			const n = 16
			errs := make(chan error, n)
			for i := range n {
				go func() { errs <- commitSnapshot(p, int64(i), fmt.Sprint(i)) }()
			}
			for range n {
				if err := <-errs; err != nil {
					assert.ErrorIs(t, err, ErrSnapshotOutOfDate)
				}
			}
			assertCurrentSnapshot(t, p, n-1, fmt.Sprint(n-1))
		})
	}
}

// Graft reads the log on the apply goroutine while appending on the raft side and truncating on the compaction worker.
func TestConcurrentLogAccess(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			const n = 200
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := range n {
					_, err := p.Append(&pb.PersistedState{CurrentTerm: 1}, []*pb.LogEntry{{Term: 1, Data: []byte{byte(i)}}})
					assert.Check(t, err)
					if i%50 == 49 {
						assert.Check(t, p.TruncateEntriesTo(int64(i-25)))
					}
				}
			}()

			for {
				select {
				case <-done:
					return
				default:
				}

				first, err := p.FirstEntryIndex()
				assert.NilError(t, err)
				last, err := p.LastEntryIndex()
				assert.NilError(t, err)
				if first < 0 || last < first {
					continue
				}

				entries, err := p.GetEntries(first, last)
				if errors.Is(err, ErrCompacted) {
					continue // Compacted between the reads above and this one.
				}
				assert.NilError(t, err)
				for i, e := range entries {
					assert.Equal(t, e.Index, first+int64(i))
				}
			}
		})
	}
}

// getMetadata runs under the lock, and commitMetadata under the write lock, on commit and on open alike, so a commit
// can't delete a file between Open's check and its open.
func TestFileSnapshotterCallsBackUnderLock(t *testing.T) {
	var mut sync.RWMutex
	var current *pb.SnapshotMetadata
	files, err := OpenFileSnapshotter(t.TempDir(), &mut, func() (*pb.SnapshotMetadata, error) {
		if mut.TryLock() {
			mut.Unlock()
			t.Error("got metadata without the lock")
		}
		return current, nil
	}, func(metadata *pb.SnapshotMetadata) error {
		if mut.TryRLock() {
			mut.RUnlock()
			t.Error("committed metadata without the write lock")
		}
		current = metadata
		return nil
	}, nil)
	assert.NilError(t, err)

	metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
	writer, err := files.Create(metadata)
	assert.NilError(t, err)
	_, err = writer.WriteAt([]byte("abc"), 0)
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())

	snapshot, err := files.Open(metadata)
	assert.NilError(t, err)
	defer snapshot.Close()
	data, err := snapshot.ReadAll()
	assert.NilError(t, err)
	assert.Equal(t, string(data), "abc")
}

// Create and Open only read the metadata, so they don't wait for the Persistence's own readers.
func TestFileSnapshotterSharesTheLockWithReaders(t *testing.T) {
	var mut sync.RWMutex
	files := testFileSnapshotter(t, t.TempDir(), &mut, nil)
	metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}

	mut.RLock()
	defer mut.RUnlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if writer, err := files.Create(metadata); assert.Check(t, err) {
			assert.Check(t, writer.Close())
		}
		_, err := files.Open(metadata)
		assert.Check(t, errors.Is(err, ErrNoSuchSnapshot))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Create or Open waited for a reader")
	}
}

// Only snapshots being written end in .tmp, so they can't be mistaken for the current one.
func TestFileSnapshotterFileNames(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)

	writer, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 2})
	assert.NilError(t, err)
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_1_2.1.tmp"})
	assert.NilError(t, writer.Commit())
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_1_2.snap"})
}

// A writer discarded for a newer snapshot takes its temp file with it.
func TestFileSnapshotterDeletesDiscardedWritersFiles(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)

	_, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 1})
	assert.NilError(t, err)
	writer, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1})
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_5_1.snap"})
}

// Opening a FileSnapshotter removes the snapshot files that aren't current, left by writes that never committed or
// deletes that failed.
func TestFileSnapshotterRemovesLeftovers(t *testing.T) {
	dir := t.TempDir()
	for _, file := range []string{"log_0_0.dat", "snap_3_1.snap", "snap_5_1.snap", "snap_5_1.2.tmp", "snap_7_1.1.tmp"} {
		assert.NilError(t, os.WriteFile(path.Join(dir, file), nil, 0644))
	}
	testFileSnapshotter(t, dir, &sync.RWMutex{}, &pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1})
	assert.DeepEqual(t, fileNames(t, dir), []string{"log_0_0.dat", "snap_5_1.snap"})
}

// A current snapshot whose file is missing fails the open, before any leftover is removed.
func TestFileSnapshotterFailsWithoutTheCurrentFile(t *testing.T) {
	dir := t.TempDir()
	assert.NilError(t, os.WriteFile(path.Join(dir, "snap_5_1.2.tmp"), nil, 0644))
	current := &pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1}
	_, err := OpenFileSnapshotter(dir, &sync.RWMutex{}, func() (*pb.SnapshotMetadata, error) { return current, nil },
		func(*pb.SnapshotMetadata) error { return nil }, nil)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_5_1.2.tmp"})
}

// testFileSnapshotter creates a FileSnapshotter in dir that keeps the current snapshot's metadata in memory, starting
// from current.
func testFileSnapshotter(t *testing.T, dir string, lock *sync.RWMutex, current *pb.SnapshotMetadata) *FileSnapshotter {
	t.Helper()
	files, err := OpenFileSnapshotter(dir, lock, func() (*pb.SnapshotMetadata, error) { return current, nil },
		func(metadata *pb.SnapshotMetadata) error {
			current = metadata
			return nil
		}, nil)
	assert.NilError(t, err)
	return files
}

func fileNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	assert.NilError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// A commit deletes the snapshot it replaces even while that one is being read, which keeps its data.
func TestFileSnapshotterDeletesSnapshotsBeingRead(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)
	commit := func(index int64, data string) {
		t.Helper()
		writer, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
		assert.NilError(t, err)
		_, err = writer.WriteAt([]byte(data), 0)
		assert.NilError(t, err)
		assert.NilError(t, writer.Commit())
	}

	commit(1, "one")
	snapshot, err := files.Open(&pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1})
	assert.NilError(t, err)
	defer snapshot.Close()

	commit(2, "two")
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_2_1.snap"})
	data, err := snapshot.ReadAll()
	assert.NilError(t, err)
	assert.Equal(t, string(data), "one")
}

// Snapshot files are created like segments: with os.Create's mode, under the umask.
func TestSnapshotFilesAreCreatedLikeSegments(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)
	metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
	writer, err := files.Create(metadata)
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())

	ref, err := os.Create(path.Join(dir, "ref"))
	assert.NilError(t, err)
	assert.NilError(t, ref.Close())
	want, err := os.Stat(ref.Name())
	assert.NilError(t, err)
	got, err := os.Stat(path.Join(dir, snapshotFilename(metadata)))
	assert.NilError(t, err)
	assert.Equal(t, got.Mode().Perm(), want.Mode().Perm())
}
