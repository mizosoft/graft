package badger

import (
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/mizosoft/graft/pb"
	"github.com/mizosoft/graft/testutil"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

func TestWalNewWalOpen(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenBadgerPersistence(dir, nil)
	assert.NilError(t, err)
	defer wal.Close()

	assert.NilError(t, err)
	assert.Equal(t, wal.EntryCount(), int64(0))
}

func TestWalEmptyWalReopen(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenBadgerPersistence(dir, nil)
	assert.NilError(t, err)
	wal.Close()

	wal, err = OpenBadgerPersistence(dir, nil)
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
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}

func TestWalAppendMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}

func TestWalGetEntriesFromMiddle(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}

func TestWalSetState(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}

func TestWalTruncateEntries(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}

func TestWalReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := OpenBadgerPersistence(dir, nil)
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
	w2, err := OpenBadgerPersistence(dir, nil)
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
}

func TestWalErrorCases(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
	defer w.Close()
	assert.NilError(t, err)

	_, err = w.GetEntry(100)
	assert.Assert(t, err != nil)

	// Test using closed WAL
	w.Close()

	_, err = w.GetEntry(0)
	assert.Assert(t, err != nil)
}

func TestWalOverwriteState(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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

	w2, err := OpenBadgerPersistence(dir, nil)
	assert.NilError(t, err)
	defer w2.Close()

	// Verify latest state is recovered.
	retrievedState, err = w2.RetrieveState()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(retrievedState, state2))
}

func TestWalTruncateAfterStateUpdate(t *testing.T) {
	dir := t.TempDir()
	w1, err := OpenBadgerPersistence(dir, nil)
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
	w2, err := OpenBadgerPersistence(dir, nil)
	assert.NilError(t, err)
	defer w2.Close()

	retrievedState, err := w2.RetrieveState()
	assert.Assert(t, proto.Equal(retrievedState, state))
}

func TestWalAppendAfterTruncating(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}

// TODO may want to move to testutil.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func TestWalSaveAndRetrieveSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
	assert.NilError(t, err)
	defer w.Close()

	retrievedMetadata, err := w.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, retrievedMetadata == nil)

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

	n, err := writer.WriteAt([]byte("Pikachu"), 0)
	assert.NilError(t, err)
	assert.Equal(t, n, 7)

	_, err = writer.Commit()
	assert.NilError(t, err)

	metadata.Size = int64(7)
	retrievedMetadata, err = w.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(retrievedMetadata, metadata))

	snapshot, err := w.OpenSnapshot(metadata)
	assert.NilError(t, err)

	retrievedData, err := snapshot.ReadAll()
	assert.Equal(t, string(retrievedData), "Pikachu")
}

func TestWalRetrieveSnapshotOnReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := OpenBadgerPersistence(dir, nil)
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

	writer, err := w1.CreateSnapshot(metadata)
	assert.NilError(t, err)

	n, err := writer.WriteAt([]byte("Pikachu"), 0)
	assert.NilError(t, err)
	assert.Equal(t, n, 7)

	_, err = writer.Commit()
	assert.NilError(t, err)

	w1.Close()

	w2, err := OpenBadgerPersistence(dir, nil)
	assert.NilError(t, err)
	defer w2.Close()

	retrievedMetadata, err := w2.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(retrievedMetadata, metadata))

	snapshot, err := w2.OpenSnapshot(metadata)
	assert.NilError(t, err)

	retrievedData, err := snapshot.ReadAll()
	assert.Equal(t, string(retrievedData), "Pikachu")
}

func TestWalTruncateEntriesTo(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
		_, err := w.Append(nil, []*pb.LogEntry{entry})
		assert.NilError(t, err)
	}

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
}

func TestWalFirstAndLastEntryIndex(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
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
}
