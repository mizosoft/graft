package persistencetest

import (
	"errors"
	"strings"
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

// A persistence starts with no state, entries or snapshot.
func testFreshPersistenceIsEmpty(t *testing.T, factory *persistenceFactory) {
	assertEmpty(t, factory.open())
}

// The state last saved, on its own or along with entries, is the one retrieved.
func testSavedStateIsRetrieved(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	for _, state := range []*pb.PersistedState{
		{CurrentTerm: 1, VotedFor: "Pikachu", CommitIndex: -1},
		{CurrentTerm: 2, VotedFor: "Eevee", CommitIndex: 3},
	} {
		assert.NilError(t, p.SaveState(state))
		assertState(t, p, state)
	}

	appendEntries(t, p, 1)
	assertState(t, p, appendState)
}

// Appending with no state appends the entries alone, leaving the saved state, or the lack of one, as it is.
func testAppendWithoutStateKeepsTheState(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	mew := &pb.LogEntry{Term: 1, Data: []byte("Mew")}
	next, err := p.Append(nil, []*pb.LogEntry{proto.CloneOf(mew)})
	assert.NilError(t, err)
	assert.Equal(t, next, int64(1))
	assertState(t, p, nil)

	state := &pb.PersistedState{CurrentTerm: 2, VotedFor: "Mewtwo", CommitIndex: 0}
	assert.NilError(t, p.SaveState(state))
	mewtwo := &pb.LogEntry{Term: 2, Data: []byte("Mewtwo")}
	next, err = p.Append(nil, []*pb.LogEntry{proto.CloneOf(mewtwo)})
	assert.NilError(t, err)
	assert.Equal(t, next, int64(2))
	assertState(t, p, state)

	mew.Index, mewtwo.Index = 0, 1
	got, err := p.GetEntriesFrom(0)
	assert.NilError(t, err)
	assertEntries(t, got, []*pb.LogEntry{mew, mewtwo})
}

// Entries are indexed from 0 in the order they're appended, and Append returns the index of the next one.
func testAppendIndexesEntriesInOrder(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	first, entries := appendEntries(t, p, 3)
	assert.Equal(t, first, int64(0))
	next, more := appendEntries(t, p, 2)
	assert.Equal(t, next, int64(3))
	entries = append(entries, more...)

	got, err := p.GetEntriesFrom(0)
	assert.NilError(t, err)
	assertEntries(t, got, entries)
	assertLogHolds(t, p, 0, 4)
}

func testGetEntriesBoundsAreInclusive(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	_, entries := appendEntries(t, p, 10)

	single, err := p.GetEntries(3, 3)
	assert.NilError(t, err)
	assertEntries(t, single, entries[3:4])
	span, err := p.GetEntries(2, 5)
	assert.NilError(t, err)
	assertEntries(t, span, entries[2:6])
	all, err := p.GetEntries(0, 9)
	assert.NilError(t, err)
	assertEntries(t, all, entries)

	tail, err := p.GetEntriesFrom(7)
	assert.NilError(t, err)
	assertEntries(t, tail, entries[7:])
	entry, err := p.GetEntry(4)
	assert.NilError(t, err)
	assertEntries(t, []*pb.LogEntry{entry}, entries[4:5])
	term, err := p.GetEntryTerm(4)
	assert.NilError(t, err)
	assert.Equal(t, term, entries[4].Term)
}

// Truncating from an index drops it and everything after it, the log continues from there, and the state stays.
func testTruncateEntriesFromDropsTheTail(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	_, entries := appendEntries(t, p, 5)
	state := &pb.PersistedState{CurrentTerm: 3, VotedFor: "Snorlax", CommitIndex: 2}
	assert.NilError(t, p.SaveState(state))
	assert.NilError(t, p.TruncateEntriesFrom(3))
	assertState(t, p, state)
	assertLogHolds(t, p, 0, 2)
	got, err := p.GetEntriesFrom(0)
	assert.NilError(t, err)
	assertEntries(t, got, entries[:3])

	next, more := appendEntries(t, p, 2)
	assert.Equal(t, next, int64(3))
	got, err = p.GetEntriesFrom(0)
	assert.NilError(t, err)
	assertEntries(t, got, append(entries[:3:3], more...))
}

// Compacting up to an index drops it and everything before it, and leaves the rest where it was, to be read and
// appended to as before. Compacting everything leaves the log empty.
func testTruncateEntriesToCompactsTheHead(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	const count = 100
	entries := make([]*pb.LogEntry, count)
	for i := range entries {
		entries[i] = &pb.LogEntry{
			Index: int64(i),
			Term:  int64(i + 1),
			Type:  pb.LogEntry_Type(i % 3),
			Data:  []byte(strings.Repeat("a", i*7%64)),
		}
		next, err := p.Append(appendState, []*pb.LogEntry{proto.CloneOf(entries[i])})
		assert.NilError(t, err)
		assert.Equal(t, next, int64(i+1))
	}

	for i := int64(0); i < count-1; i++ {
		assert.NilError(t, p.TruncateEntriesTo(i))
		assertLogHolds(t, p, i+1, count-1)
		got, err := p.GetEntriesFrom(i + 1)
		assert.NilError(t, err)
		assertEntries(t, got, entries[i+1:])
		end := min(i+3, count-1)
		got, err = p.GetEntries(i+1, end)
		assert.NilError(t, err)
		assertEntries(t, got, entries[i+1:end+1])
		for _, j := range []int64{i + 1, count - 1} {
			entry, err := p.GetEntry(j)
			assert.NilError(t, err)
			assertEntries(t, []*pb.LogEntry{entry}, entries[j:j+1])
			term, err := p.GetEntryTerm(j)
			assert.NilError(t, err)
			assert.Equal(t, term, entries[j].Term)
		}
	}

	next, more := appendEntries(t, p, 1)
	assert.Equal(t, next, int64(count))
	assertLogHolds(t, p, count-1, count)
	got, err := p.GetEntriesFrom(count - 1)
	assert.NilError(t, err)
	assertEntries(t, got, append(entries[count-1:], more...))

	assert.NilError(t, p.TruncateEntriesTo(count))
	assert.Equal(t, p.EntryCount(), int64(0))
	first, err := p.FirstEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, first, int64(-1))
}

// Compaction can be requested again, or beyond the log: before the first index does nothing, and past the last compacts
// everything.
func testTruncateEntriesToClamps(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	appendEntries(t, p, 10)
	assert.NilError(t, p.TruncateEntriesTo(4))
	assert.NilError(t, p.TruncateEntriesTo(2))
	first, err := p.FirstEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, first, int64(5))

	assert.NilError(t, p.TruncateEntriesTo(100))
	assert.Equal(t, p.EntryCount(), int64(0))
	_, err = p.GetEntry(9)
	assert.ErrorIs(t, err, graft.ErrCompacted)
}

// Compacting the whole log doesn't rewind where entries append: reusing an index a snapshot covers corrupts the log.
func testAppendContinuesAfterFullCompaction(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	appendEntries(t, p, 10)
	assert.NilError(t, p.TruncateEntriesTo(9))
	first, err := p.FirstEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, first, int64(-1))

	next, _ := appendEntries(t, p, 1)
	assert.Equal(t, next, int64(10))
	last, err := p.LastEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, last, int64(10))
}

// A range ending past the log, or running backwards, is a bug in the caller, so it panics where it's found.
func testLogAccessPastTheEndPanics(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	appendEntries(t, p, 5)

	assertPanics(t, "ends past the last index 4", func() { _, _ = p.GetEntries(0, 5) })
	assertPanics(t, "ends past the last index 4", func() { _, _ = p.GetEntry(5) })
	assertPanics(t, "ends past the last index 4", func() { _, _ = p.GetEntriesFrom(5) })
	assertPanics(t, "ends past the last index 4", func() { _ = p.TruncateEntriesFrom(5) })
	assertPanics(t, "invalid index range [3, 2]", func() { _, _ = p.GetEntries(3, 2) })
}

// Before the first index is where a compaction lands between a caller's bounds check and its read. From the first index
// on, the log reads as before.
func testLogAccessBeforeTheFirstIndexIsCompacted(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	_, entries := appendEntries(t, p, 10)
	assert.NilError(t, p.TruncateEntriesTo(4))

	_, err := p.GetEntries(3, 6)
	assert.ErrorIs(t, err, graft.ErrCompacted)
	_, err = p.GetEntry(4)
	assert.ErrorIs(t, err, graft.ErrCompacted)
	_, err = p.GetEntryTerm(4)
	assert.ErrorIs(t, err, graft.ErrCompacted)
	_, err = p.GetEntriesFrom(0)
	assert.ErrorIs(t, err, graft.ErrCompacted)
	assert.ErrorIs(t, p.TruncateEntriesFrom(2), graft.ErrCompacted)

	got, err := p.GetEntries(5, 9)
	assert.NilError(t, err)
	assertEntries(t, got, entries[5:])
}

// Graft reads the log on the apply goroutine while appending on the raft side and compacting on the compaction worker.
func testConcurrentLogAccess(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	const n = 200
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range n {
			_, err := p.Append(appendState, []*pb.LogEntry{{Term: 1, Data: []byte{byte(i)}}})
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
		if errors.Is(err, graft.ErrCompacted) {
			continue // Compacted between the reads above and this one.
		}
		assert.NilError(t, err)
		for i, entry := range entries {
			assert.Equal(t, entry.Index, first+int64(i))
		}
	}
}
