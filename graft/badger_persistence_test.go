package graft

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/mizosoft/graft/pb"
	"github.com/mizosoft/graft/testutil"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

func TestBadgerNewWalOpen(t *testing.T) {
	dir := t.TempDir()
	persistence, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer persistence.Close()

	assert.Equal(t, persistence.EntryCount(), int64(0))
}

func TestBadgerEmptyWalReopen(t *testing.T) {
	dir := t.TempDir()
	persistence, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	persistence.Close()

	persistence, err = OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer persistence.Close()

	assert.Equal(t, persistence.EntryCount(), int64(0))
	testutil.AssertNil(t, persistence.RetrieveState())
	testutil.AssertNil(t, persistence.RetrieveSnapshot())
}

func TestBadgerAppendSingleEntry(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entry := &pb.LogEntry{
		Term: 1,
		Data: []byte("cmd"),
	}

	nextIndex := p.Append(state, []*pb.LogEntry{entry})
	assert.Equal(t, nextIndex, int64(1))
	assert.Equal(t, p.EntryCount(), int64(1))
	assert.Assert(t, proto.Equal(state, p.RetrieveState()))

	// Retrieve entry and verify.
	retrievedEntry := p.GetEntry(0)
	assert.Assert(t, proto.Equal(retrievedEntry, entry))
	assert.Equal(t, retrievedEntry.Index, int64(0))
}

func TestBadgerAppendMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p.Close()

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

	nextIndex := p.Append(state, entries)
	assert.Equal(t, nextIndex, int64(3))
	assert.Equal(t, p.EntryCount(), int64(3))

	// Retrieve and verify entries.
	retrievedEntries := p.GetEntriesFrom(0)
	assert.Equal(t, len(retrievedEntries), len(entries))
	for i, entry := range entries {
		assert.Assert(t, proto.Equal(retrievedEntries[i], entry))
		assert.Equal(t, retrievedEntries[i].Index, int64(i))
	}
}

func TestBadgerGetEntriesFromMiddle(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p.Close()

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

	p.Append(state, entries)

	retrievedEntries := p.GetEntriesFrom(2)
	assert.Equal(t, len(retrievedEntries), 3)
	assert.Assert(t, proto.Equal(retrievedEntries[0], entries[2]))
	assert.Equal(t, retrievedEntries[0].Index, int64(2))
	assert.Assert(t, proto.Equal(retrievedEntries[1], entries[3]))
	assert.Equal(t, retrievedEntries[1].Index, int64(3))
	assert.Assert(t, proto.Equal(retrievedEntries[2], entries[4]))
	assert.Equal(t, retrievedEntries[2].Index, int64(4))
}

func TestBadgerSetState(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p.Close()

	// Initial state
	state1 := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	p.SaveState(state1)
	assert.Assert(t, proto.Equal(state1, p.RetrieveState()))

	// Updated state
	state2 := &pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s1",
		CommitIndex: 5,
	}

	p.SaveState(state2)
	assert.Assert(t, proto.Equal(state2, p.RetrieveState()))
}

func TestBadgerTruncateEntries(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p.Close()

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

	p.Append(state, entries)
	assert.Equal(t, p.EntryCount(), int64(5))

	p.TruncateEntriesFrom(3)
	assert.Equal(t, p.EntryCount(), int64(3))

	retrievedEntries := p.GetEntriesFrom(0)
	assert.Equal(t, len(retrievedEntries), 3)
	for i := range 3 {
		assert.Assert(t, proto.Equal(entries[i], retrievedEntries[i]))
	}

	// State is preserved after truncation.
	assert.Assert(t, proto.Equal(state, p.RetrieveState()))
}

func TestBadgerReopen(t *testing.T) {
	dir := t.TempDir()
	p1, err := OpenBadgerPersistence(dir)
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

	p1.Append(state, entries)
	p1.Close()

	// Reopen.
	p2, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p2.Close()

	// Verify state was recovered.
	assert.Assert(t, proto.Equal(p2.RetrieveState(), state))

	// Verify entries were recovered.
	assert.Equal(t, p2.EntryCount(), int64(3))

	recoveredEntries := p2.GetEntriesFrom(0)
	assert.Equal(t, len(recoveredEntries), len(entries))
	for i, entry := range entries {
		assert.Assert(t, proto.Equal(entry, recoveredEntries[i]))
	}
}

func TestBadgerOverwriteState(t *testing.T) {
	dir := t.TempDir()
	p1, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p1.Close()

	// Set initial state.
	state1 := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	p1.SaveState(state1)

	// Update state.
	state2 := &pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s2",
		CommitIndex: 5,
	}
	p1.SaveState(state2)

	// Verify latest state is returned.
	assert.Assert(t, proto.Equal(p1.RetrieveState(), state2))

	// Reopen.
	p1.Close()
	assert.NilError(t, err)

	p2, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p2.Close()

	// Verify latest state is recovered.
	assert.Assert(t, proto.Equal(p2.RetrieveState(), state2))
}

func TestBadgerAppendAfterTruncating(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer p.Close()

	nextIndex := p.Append(nil, []*pb.LogEntry{
		{Term: 1, Data: []byte("a")},
		{Term: 2, Data: []byte("b")},
	})
	assert.Equal(t, nextIndex, int64(2))

	p.TruncateEntriesFrom(1)

	nextIndex = p.Append(nil, []*pb.LogEntry{
		{Term: 3, Data: []byte("c")},
		{Term: 4, Data: []byte("d")},
	})
	assert.Equal(t, nextIndex, int64(3))

	retrievedEntries := p.GetEntriesFrom(0)
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

func TestBadgerSaveAndRetrieveSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer w.Close()

	snapshot := w.RetrieveSnapshot()
	assert.Assert(t, snapshot == nil)

	snapshot = NewSnapshot(&pb.SnapshotMetadata{
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
	}, []byte("Pikachu"))
	w.SaveSnapshot(snapshot)

	retrievedSnapshot := w.RetrieveSnapshot()
	assert.Assert(t, proto.Equal(retrievedSnapshot.Metadata(), snapshot.Metadata()))
	assert.Equal(t, string(retrievedSnapshot.Data()), string(snapshot.Data()))
}

func TestBadgerRetrieveSnapshotOnReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer w1.Close()

	snapshot := NewSnapshot(&pb.SnapshotMetadata{
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
	}, []byte("Pikachu"))
	w1.SaveSnapshot(snapshot)
	assert.NilError(t, err)

	w1.Close()

	w2, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer w2.Close()
	retrievedSnapshot := w2.RetrieveSnapshot()
	assert.Assert(t, proto.Equal(retrievedSnapshot.Metadata(), snapshot.Metadata()))
	assert.Equal(t, string(retrievedSnapshot.Data()), string(snapshot.Data()))
}

func TestBadgerTruncateEntriesTo(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer w.Close()

	rnd := rand.New(rand.NewSource(0))
	entryCount := int64(100)
	entries := make([]*pb.LogEntry, entryCount)
	for i := range entries {
		commandSize := rnd.Intn(128)
		entries[i] = &pb.LogEntry{
			Index: int64(i),
			Term:  int64(i),
			Data:  []byte(strings.Repeat("a", commandSize)),
		}
	}

	for _, entry := range entries {
		w.Append(nil, []*pb.LogEntry{entry})
	}

	for i := int64(0); i < entryCount; i++ {
		w.TruncateEntriesTo(i)

		firstIndex := w.FirstEntryIndex()
		if i < entryCount-1 {
			assert.Equal(t, firstIndex, i+1)

			retrievedEntries := w.GetEntriesFrom(firstIndex)
			for j, entry := range entries[i+1:] {
				assert.Assert(t, proto.Equal(retrievedEntries[j], entry))
			}
		} else {
			assert.Equal(t, firstIndex, int64(-1))
		}
	}
}

func TestBadgerFirstAndLastEntryIndex(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir)
	assert.NilError(t, err)
	defer w.Close()

	assert.Equal(t, int64(-1), w.FirstEntryIndex())
	assert.Equal(t, int64(-1), w.LastEntryIndex())

	w.Append(nil, []*pb.LogEntry{
		{
			Term: 2,
			Data: []byte("foo"),
		},
		{
			Term: 3,
			Data: []byte("bar"),
		},
	})

	assert.Equal(t, int64(0), w.FirstEntryIndex())
	assert.Equal(t, int64(1), w.LastEntryIndex())

	w.TruncateEntriesTo(0)

	w.Append(nil, []*pb.LogEntry{
		{
			Term: 4,
			Data: []byte("bar"),
		},
	})

	assert.Equal(t, int64(1), w.FirstEntryIndex())
	assert.Equal(t, int64(2), w.LastEntryIndex())
}
