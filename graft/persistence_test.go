package graft

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

// Both Persistence backends must agree on GetEntries' bounds. They did not: wal
// treats `to` as inclusive, memoryPersistence returned log[from:to]. The apply
// loop cannot be correct against both, so the contract is pinned on the interface
// and checked here.

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
			assert.NilError(t, commitSnapshot(p, 5, "five"))
			assert.ErrorIs(t, commitSnapshot(p, 3, "three"), ErrSnapOutOfDate)
			assertCurrentSnapshot(t, p, 5, "five")
		})
	}
}

// Same index means same name on disk, so committing it would replace the current file and then delete it as the
// predecessor.
func TestSnapshotCommitRejectsSameIndex(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			assert.NilError(t, commitSnapshot(p, 5, "first"))
			assert.ErrorIs(t, commitSnapshot(p, 5, "second"), ErrSnapOutOfDate)
			assertCurrentSnapshot(t, p, 5, "first")
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
			assert.ErrorIs(t, w2.Commit(), ErrSnapOutOfDate)
			assertCurrentSnapshot(t, p, 5, "one")
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
					assert.ErrorIs(t, err, ErrSnapOutOfDate)
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
