package persistencetest

import (
	"testing"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

func testEmptyPersistenceReopensEmpty(t *testing.T, factory *persistenceFactory) {
	assert.NilError(t, factory.open().Close())
	assertEmpty(t, factory.open())
}

// The state last saved survives reopening along with the log, whether it was saved with entries or on its own, and
// whatever entries were appended after it with no state.
func testStateAndLogSurviveReopening(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	_, entries := appendEntries(t, p, 3)
	assert.NilError(t, p.Close())

	p = factory.open()
	assertState(t, p, appendState)
	state := &pb.PersistedState{CurrentTerm: 2, VotedFor: "Psyduck", CommitIndex: 1}
	assert.NilError(t, p.SaveState(state))
	golduck := &pb.LogEntry{Term: 2, Data: []byte("Golduck")}
	_, err := p.Append(nil, []*pb.LogEntry{proto.CloneOf(golduck)})
	assert.NilError(t, err)
	golduck.Index = 3
	entries = append(entries, golduck)
	assert.NilError(t, p.Close())

	p = factory.open()
	assertState(t, p, state)
	assertLogHolds(t, p, 0, 3)
	got, err := p.GetEntriesFrom(0)
	assert.NilError(t, err)
	assertEntries(t, got, entries)
}

// Entries appended with no state reopen with no state saved, rather than an empty one.
func testAppendWithoutStateSurvivesReopening(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	mew := &pb.LogEntry{Term: 1, Data: []byte("Mew")}
	_, err := p.Append(nil, []*pb.LogEntry{proto.CloneOf(mew)})
	assert.NilError(t, err)
	assert.NilError(t, p.Close())

	p = factory.open()
	assertState(t, p, nil)
	mew.Index = 0
	got, err := p.GetEntriesFrom(0)
	assert.NilError(t, err)
	assertEntries(t, got, []*pb.LogEntry{mew})
}

// The state outlives the entries it was saved with.
func testStateSurvivesEmptyingTheLog(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	appendEntries(t, p, 2)
	assert.NilError(t, p.TruncateEntriesFrom(0))
	assert.NilError(t, p.Close())

	p = factory.open()
	assertState(t, p, appendState)
	assert.Equal(t, p.EntryCount(), int64(0))
}

// Compacted entries stay compacted, rather than coming back from wherever they were kept.
func testCompactionSurvivesReopening(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	_, entries := appendEntries(t, p, 10)
	assert.NilError(t, p.TruncateEntriesTo(4))
	assert.NilError(t, p.Close())

	p = factory.open()
	first, err := p.FirstEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, first, int64(5))
	got, err := p.GetEntriesFrom(5)
	assert.NilError(t, err)
	assertEntries(t, got, entries[5:])
}

// The current snapshot, all of its metadata and its data, outlives the persistence it was committed to.
func testCurrentSnapshotSurvivesReopening(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	writer, err := p.CreateSnapshot(configChangeMetadata(5))
	assert.NilError(t, err)
	_, err = writer.WriteAt([]byte("five"), 0)
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())
	assert.NilError(t, p.Close())

	assertCurrentSnapshot(t, factory.open(), writer.Metadata(), "five")
}
