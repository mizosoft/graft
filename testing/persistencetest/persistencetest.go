// Package persistencetest checks a graft.Persistence against the contract graft relies on, through the interface alone,
// the way testing/fstest checks a file system. A backend runs the suite from its own tests:
//
//	func TestMyPersistence(t *testing.T) {
//		persistencetest.Run(t, persistencetest.Backend{
//			Open: func(dir string) (graft.Persistence, error) { return mypersistence.Open(dir) },
//		})
//	}
//
// What a backend does beneath the interface, such as how it lays out its files, is for its own tests to check.
package persistencetest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

// Backend is a Persistence implementation to run the suite against.
type Backend struct {
	// Open opens the persistence kept in dir, which starts out empty. The suite closes what it opens.
	Open func(dir string) (graft.Persistence, error)

	// Volatile is set for a persistence that keeps nothing across opens, which skips the tests that reopen it.
	Volatile bool

	// Skip names tests not to run, each with the reason, such as the issue tracking a known gap.
	Skip map[string]string
}

// Run runs the suite against backend, each test as a subtest with a directory of its own.
func Run(t *testing.T, backend Backend) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if reason, ok := backend.Skip[test.name]; ok {
				t.Skip(reason)
			}
			test.run(t, &persistenceFactory{t: t, backend: backend, dir: t.TempDir()})
		})
	}
}

var tests = []struct {
	name string
	run  func(t *testing.T, factory *persistenceFactory)
}{
	{"FreshPersistenceIsEmpty", testFreshPersistenceIsEmpty},
	{"SavedStateIsRetrieved", testSavedStateIsRetrieved},
	{"AppendWithoutStateKeepsTheState", testAppendWithoutStateKeepsTheState},
	{"AppendIndexesEntriesInOrder", testAppendIndexesEntriesInOrder},
	{"GetEntriesBoundsAreInclusive", testGetEntriesBoundsAreInclusive},
	{"TruncateEntriesFromDropsTheTail", testTruncateEntriesFromDropsTheTail},
	{"TruncateEntriesToCompactsTheHead", testTruncateEntriesToCompactsTheHead},
	{"TruncateEntriesToClamps", testTruncateEntriesToClamps},
	{"AppendContinuesAfterFullCompaction", testAppendContinuesAfterFullCompaction},
	{"LogAccessPastTheEndPanics", testLogAccessPastTheEndPanics},
	{"LogAccessBeforeTheFirstIndexIsCompacted", testLogAccessBeforeTheFirstIndexIsCompacted},
	{"ConcurrentLogAccess", testConcurrentLogAccess},
	{"CommittedSnapshotIsCurrent", testCommittedSnapshotIsCurrent},
	{"ClosedWriterCommitsNothing", testClosedWriterCommitsNothing},
	{"CommitRejectsOlderSnapshots", testCommitRejectsOlderSnapshots},
	{"CreateRejectsSnapshotsNoNewerThanCurrent", testCreateRejectsSnapshotsNoNewerThanCurrent},
	{"WritersOfTheSameSnapshotCoexist", testWritersOfTheSameSnapshotCoexist},
	{"SnapshotWritesCanJumpBack", testSnapshotWritesCanJumpBack},
	{"SnapshotWritesCantLeaveGaps", testSnapshotWritesCantLeaveGaps},
	{"ClosedWritersRefuseUse", testClosedWritersRefuseUse},
	{"NewerCommitsDiscardStaleWriters", testNewerCommitsDiscardStaleWriters},
	{"ClosingFailsOpenWriters", testClosingFailsOpenWriters},
	{"ConcurrentCommitsLeaveTheNewestCurrent", testConcurrentCommitsLeaveTheNewestCurrent},
	{"EmptyPersistenceReopensEmpty", testEmptyPersistenceReopensEmpty},
	{"StateAndLogSurviveReopening", testStateAndLogSurviveReopening},
	{"ReopenedLogReadsAndCompactsByIndex", testReopenedLogReadsAndCompactsByIndex},
	{"ReopenedLogTruncatesAndAppends", testReopenedLogTruncatesAndAppends},
	{"AppendWithoutStateSurvivesReopening", testAppendWithoutStateSurvivesReopening},
	{"StateSurvivesEmptyingTheLog", testStateSurvivesEmptyingTheLog},
	{"CompactionSurvivesReopening", testCompactionSurvivesReopening},
	{"CurrentSnapshotSurvivesReopening", testCurrentSnapshotSurvivesReopening},
}

// persistenceFactory opens a test's persistences from its directory, and closes them when the test is done.
type persistenceFactory struct {
	t       *testing.T
	backend Backend
	dir     string
	opened  bool
}

// open opens the persistence kept in the test's directory. Opening it again skips the test for a volatile backend.
func (f *persistenceFactory) open() graft.Persistence {
	f.t.Helper()
	if f.opened && f.backend.Volatile {
		f.t.Skip("keeps nothing across opens")
	}
	f.opened = true
	p, err := f.backend.Open(f.dir)
	assert.NilError(f.t, err)
	f.t.Cleanup(func() { _ = p.Close() }) // Closed already by some tests.
	return p
}

// appendState is what appendEntries saves along with the entries.
var appendState = &pb.PersistedState{CurrentTerm: 1, CommitIndex: -1}

// appendEntries appends count entries in one batch, each with a term and type of its own, and returns the index of the
// first along with the entries as they should read back.
func appendEntries(t *testing.T, p graft.Persistence, count int) (first int64, entries []*pb.LogEntry) {
	t.Helper()
	entries = make([]*pb.LogEntry, count)
	appended := make([]*pb.LogEntry, count)
	for i := range entries {
		entries[i] = &pb.LogEntry{
			Term: int64(i + 1),
			Type: pb.LogEntry_Type(i % 3),
			Data: []byte(fmt.Sprint("entry ", i)),
		}
		appended[i] = proto.CloneOf(entries[i])
	}
	next, err := p.Append(appendState, appended)
	assert.NilError(t, err)
	first = next - int64(count)
	for i, entry := range entries {
		entry.Index = first + int64(i)
	}
	return first, entries
}

// appendEach appends count entries to a fresh log one Append at a time, each with a term, type and size of its own, and
// returns them as they should read back.
func appendEach(t *testing.T, p graft.Persistence, count int) []*pb.LogEntry {
	t.Helper()
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
	return entries
}

// assertReadsByIndex asserts that the log holds exactly entries, read from and at each of their indexes.
func assertReadsByIndex(t *testing.T, p graft.Persistence, entries []*pb.LogEntry) {
	t.Helper()
	last := entries[len(entries)-1].Index
	assertLogHolds(t, p, entries[0].Index, last)
	for i, entry := range entries {
		got, err := p.GetEntriesFrom(entry.Index)
		assert.NilError(t, err)
		assertEntries(t, got, entries[i:])
		end := min(entry.Index+2, last)
		got, err = p.GetEntries(entry.Index, end)
		assert.NilError(t, err)
		assertEntries(t, got, entries[i:i+int(end-entry.Index)+1])
		single, err := p.GetEntry(entry.Index)
		assert.NilError(t, err)
		assertEntries(t, []*pb.LogEntry{single}, entries[i:i+1])
		term, err := p.GetEntryTerm(entry.Index)
		assert.NilError(t, err)
		assert.Equal(t, term, entry.Term)
	}
}

// assertEntries asserts that got holds want's entries, field for field.
func assertEntries(t *testing.T, got, want []*pb.LogEntry) {
	t.Helper()
	assert.Equal(t, len(got), len(want))
	for i := range want {
		assert.Assert(t, proto.Equal(got[i], want[i]), "got entry %v, want %v", got[i], want[i])
	}
}

// assertLogHolds asserts that the log holds the entries from first through last.
func assertLogHolds(t *testing.T, p graft.Persistence, first, last int64) {
	t.Helper()
	gotFirst, err := p.FirstEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, gotFirst, first)
	gotLast, err := p.LastEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, gotLast, last)
	assert.Equal(t, p.EntryCount(), last-first+1)
}

// assertPanics asserts that f panics with a message mentioning want, as graft.CheckRange does.
func assertPanics(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		assert.Assert(t, r != nil, "expected a panic mentioning %q", want)
		message, ok := r.(string)
		assert.Assert(t, ok, "want a string panic, got %T: %v", r, r)
		assert.Assert(t, strings.Contains(message, want), "panic %q doesn't mention %q", message, want)
	}()
	f()
}

func assertState(t *testing.T, p graft.Persistence, want *pb.PersistedState) {
	t.Helper()
	got, err := p.RetrieveState()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(got, want), "got state %v, want %v", got, want)
}

func assertEmpty(t *testing.T, p graft.Persistence) {
	t.Helper()
	state, err := p.RetrieveState()
	assert.NilError(t, err)
	assert.Assert(t, state == nil, "has a state: %v", state)
	assert.Equal(t, p.EntryCount(), int64(0))
	first, err := p.FirstEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, first, int64(-1))
	last, err := p.LastEntryIndex()
	assert.NilError(t, err)
	assert.Equal(t, last, int64(-1))
	metadata, err := p.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, metadata == nil, "has a snapshot: %v", metadata)
}

// createSnapshot creates a writer for the snapshot at index, in term 1.
func createSnapshot(t *testing.T, p graft.Persistence, index int64) graft.SnapshotWriter {
	t.Helper()
	writer, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
	assert.NilError(t, err)
	return writer
}

// writeSnapshot creates a writer for the snapshot at index, in term 1, and writes data to it.
func writeSnapshot(t *testing.T, p graft.Persistence, index int64, data string) graft.SnapshotWriter {
	t.Helper()
	writer := createSnapshot(t, p, index)
	_, err := writer.WriteAt([]byte(data), 0)
	assert.NilError(t, err)
	return writer
}

// commitSnapshot is writeSnapshot then Commit, returning the committed metadata.
func commitSnapshot(t *testing.T, p graft.Persistence, index int64, data string) *pb.SnapshotMetadata {
	t.Helper()
	writer := writeSnapshot(t, p, index, data)
	assert.NilError(t, writer.Commit())
	return writer.Metadata()
}

// configChangeMetadata is metadata for the snapshot at index, in term 2, taken during a configuration change. It sets
// every field a caller does.
func configChangeMetadata(index int64) *pb.SnapshotMetadata {
	return &pb.SnapshotMetadata{
		LastIncludedIndex: index,
		LastIncludedTerm:  2,
		ConfigUpdate: &pb.ConfigUpdate{
			Old:   []*pb.NodeConfig{{Id: "Pikachu", Url: "pikachu:1"}},
			New:   []*pb.NodeConfig{{Id: "Pikachu", Url: "pikachu:1"}, {Id: "Raichu", Url: "raichu:1"}},
			Phase: pb.ConfigUpdate_JOINT,
		},
		ConfigUpdateIndex: index - 2,
	}
}

// assertCurrentSnapshot asserts that want describes the current snapshot, as recorded and as opened, and that it reads
// as data.
func assertCurrentSnapshot(t *testing.T, p graft.Persistence, want *pb.SnapshotMetadata, data string) {
	t.Helper()
	current, err := p.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(current, want), "got current snapshot %v, want %v", current, want)

	snapshot, err := p.OpenSnapshot(current)
	assert.NilError(t, err, "the current snapshot can't be opened")
	defer snapshot.Close()
	assert.Assert(t, proto.Equal(snapshot.Metadata(), want), "opened snapshot %v, want %v", snapshot.Metadata(), want)
	got, err := snapshot.ReadAll()
	assert.NilError(t, err)
	assert.Equal(t, string(got), data)
}
