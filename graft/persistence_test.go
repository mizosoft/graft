package graft

import (
	"errors"
	"testing"

	"github.com/mizosoft/graft/pb"
	"gotest.tools/v3/assert"
)

func errAsIndexOutOfRange(err error) (IndexOutOfRangeError, bool) {
	var target IndexOutOfRangeError
	ok := errors.As(err, &target)
	return target, ok
}

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

func TestGetEntriesRejectsOutOfRange(t *testing.T) {
	for name, p := range persistenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			appendCommands(t, p, 5)

			// Past the tail. The apply loop relies on this being an
			// IndexOutOfRangeError so it can treat a concurrent truncation as
			// "skip to the next event" rather than as a fatal fault.
			_, err := p.GetEntries(0, 5)
			assert.Assert(t, err != nil, "to beyond LastEntryIndex must fail")
			_, ok := errAsIndexOutOfRange(err)
			assert.Assert(t, ok, "want IndexOutOfRangeError, got %T: %v", err, err)

			_, err = p.GetEntries(3, 2)
			assert.Assert(t, err != nil, "from > to must fail")
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
				if _, ok := errAsIndexOutOfRange(err); ok {
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
