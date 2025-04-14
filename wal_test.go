package graft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mizosoft/graft/pb"
	"github.com/mizosoft/graft/testutil"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

func TestWalNewWalOpen(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer wal.Close()

	assert.Equal(t, wal.EntryCount(), 0)

	e, err := wal.GetEntry(0)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	testutil.AssertNil(t, e)

	e, err = wal.GetEntry(100)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	testutil.AssertNil(t, e)

	es, err := wal.GetEntriesFrom(0)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	assert.Equal(t, len(es), 0)

	_, err = wal.GetEntriesFrom(100)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	assert.Equal(t, len(es), 0)
	testutil.AssertNil(t, wal.GetState())
}

func TestWalEmptyWalReopen(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWal(dir, 1024)
	assert.NilError(t, err)
	wal.Close()

	wal, err = openWal(dir, 1024)
	assert.NilError(t, err)
	defer wal.Close()

	assert.Equal(t, wal.EntryCount(), 0)

	e, err := wal.GetEntry(0)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	testutil.AssertNil(t, e)

	e, err = wal.GetEntry(100)
	assert.Equal(t, err, errEntryIndexOutOfRange)
	testutil.AssertNil(t, e)

	es, err := wal.GetEntriesFrom(0)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	assert.Equal(t, len(es), 0)

	_, err = wal.GetEntriesFrom(100)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)
	assert.Equal(t, es, 0)

	testutil.AssertNil(t, wal.GetState())
}

func TestWalCloseWal(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWal(dir, 1024)
	assert.NilError(t, err)

	err = wal.Close()
	assert.NilError(t, err)
	err = wal.Close()
	assert.NilError(t, err)
}

func TestWalAppendSingleEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entry := &pb.LogEntry{
		Term:    1,
		Command: []byte("cmd"),
	}

	nextIndex, err := w.Append(state, []*pb.LogEntry{entry})
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 1)
	assert.Equal(t, w.EntryCount(), 1)
	assert.Assert(t, proto.Equal(state, w.GetState()))

	// Retrieve entry and verify.
	retrievedEntry, err := w.GetEntry(0)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(entry, retrievedEntry))
}

func TestWalAppendMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
		{Term: 1, Command: []byte("cmd2")},
		{Term: 1, Command: []byte("cmd3")},
	}

	nextIndex, err := w.Append(state, entries)
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 3)
	assert.Equal(t, w.EntryCount(), 3)

	// Retrieve and verify entries.
	retrievedEntries, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(retrievedEntries), len(entries))
	for i, entry := range entries {
		assert.Assert(t, proto.Equal(retrievedEntries[i], entry))
	}
}

func TestWalGetEntriesFromMiddle(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("entry 0")},
		{Term: 1, Command: []byte("entry 1")},
		{Term: 1, Command: []byte("entry 2")},
		{Term: 1, Command: []byte("entry 3")},
		{Term: 1, Command: []byte("entry 4")},
	}

	_, err = w.Append(state, entries)
	assert.NilError(t, err)

	retrievedEntries, err := w.GetEntriesFrom(2)
	assert.NilError(t, err)
	assert.Equal(t, len(retrievedEntries), 3)
	assert.Assert(t, proto.Equal(entries[2], retrievedEntries[0]))
	assert.Assert(t, proto.Equal(entries[3], retrievedEntries[1]))
	assert.Assert(t, proto.Equal(entries[4], retrievedEntries[2]))
}

func TestWalSetState(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	// Initial state
	state1 := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	err = w.SetState(state1)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(state1, w.GetState()))

	// Updated state
	state2 := &pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s1",
		CommitIndex: 5,
	}

	err = w.SetState(state2)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(state2, w.GetState()))
}

func TestWalTruncateEntries(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd0")},
		{Term: 1, Command: []byte("cmd1")},
		{Term: 1, Command: []byte("cmd2")},
		{Term: 1, Command: []byte("cmd3")},
		{Term: 1, Command: []byte("cmd4")},
	}

	_, err = w.Append(state, entries)
	assert.NilError(t, err)
	assert.Equal(t, w.EntryCount(), 5)

	err = w.TruncateEntriesFrom(3)
	assert.NilError(t, err)
	assert.Equal(t, w.EntryCount(), 3)

	retrievedEntries, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(retrievedEntries), 3)
	for i := range 3 {
		assert.Assert(t, proto.Equal(entries[i], retrievedEntries[i]))
	}

	// State is preserved after truncation.
	assert.Assert(t, proto.Equal(state, w.GetState()))
}

func TestWalMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
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
			Term:    1,
			Command: []byte("entry data that is long enough to force segment creation"),
		}
	}

	// Append in batches to create multiple segments.
	nextIndex, err := w.Append(state, entries[0:10])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 10)
	nextIndex, err = w.Append(state, entries[10:20])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 20)
	nextIndex, err = w.Append(state, entries[20:entryCount])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 30)

	assert.Assert(t, len(w.segments) > 1, len(w.segments))

	// All entries can be retrieved.
	allEntries, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(allEntries), entryCount)

	// Check getting individual entries across segments.
	for i := range entryCount {
		entry, err := w.GetEntry(i)
		assert.NilError(t, err)
		assert.Assert(t, entry != nil)
	}
}

func TestWalReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 1024)
	assert.NilError(t, err)

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 5,
	}

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("entry 0")},
		{Term: 1, Command: []byte("entry 1")},
		{Term: 1, Command: []byte("entry 2")},
	}

	_, err = w1.Append(state, entries)
	assert.NilError(t, err)
	w1.Close()

	// Reopen.
	w2, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w2.Close()

	// Verify state was recovered.
	assert.Assert(t, proto.Equal(w2.GetState(), state))

	// Verify entries were recovered.
	assert.Equal(t, w2.EntryCount(), 3)

	recoveredEntries, err := w2.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(recoveredEntries), len(entries))
	for i, entry := range entries {
		assert.Assert(t, proto.Equal(entry, recoveredEntries[i]))
	}
}

func TestWalErrorCases(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	_, err = w.GetEntry(100)
	assert.ErrorIs(t, err, errEntryIndexOutOfRange)

	// TestWal using closed WAL
	w.Close()

	_, err = w.GetEntry(0)
	assert.ErrorIs(t, err, errClosed)

	_, err = w.Append(nil, nil)
	assert.ErrorIs(t, err, errClosed)

	err = w.SetState(nil)
	assert.ErrorIs(t, err, errClosed)

	err = w.TruncateEntriesFrom(0)
	assert.ErrorIs(t, err, errClosed)
}

func TestWalCorruptedCRC(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entry := &pb.LogEntry{
		Term:    1,
		Command: []byte("test"),
	}

	_, err = w.Append(state, []*pb.LogEntry{entry})
	assert.NilError(t, err)

	w.Close()

	// Corrupt the file.
	files, err := os.ReadDir(dir)
	assert.NilError(t, err)
	assert.Equal(t, len(files), 1)

	filePath := filepath.Join(dir, files[0].Name())
	fileData, err := os.ReadFile(filePath)
	assert.NilError(t, err)

	// Corrupt file data.
	fileData[len(fileData)-1] ^= 0xFF // Flip all bits at this position
	err = os.WriteFile(filePath, fileData, 0644)
	assert.NilError(t, err)

	// Open log with corrupt segment.
	_, err = openWal(dir, 1024)
	assert.Assert(t, err != nil, "Expected error when opening corrupted WAL")
}

func TestWalOverwriteState(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	// Set initial state.
	state1 := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	err = w.SetState(state1)
	assert.NilError(t, err)

	// Update state.
	state2 := &pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s2",
		CommitIndex: 5,
	}
	err = w.SetState(state2)
	assert.NilError(t, err)

	// Verify latest state is returned.
	assert.Assert(t, proto.Equal(w.GetState(), state2))

	// Reopen.
	err = w.Close()
	assert.NilError(t, err)

	w2, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w2.Close()

	// Verify latest state is recovered.
	assert.Assert(t, proto.Equal(w2.GetState(), state2))
}

func TestWalAppendSegmentAfterStateUpdate(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 10) // Make sure state update appends a new segment.
	assert.NilError(t, err)
	defer w1.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "some cool server",
		CommitIndex: 1,
	}
	w1.SetState(state)
	assert.Assert(t, proto.Equal(w1.GetState(), state))
	assert.Equal(t, len(w1.segments), 2)

	// Last segment retains state.
	assert.Assert(t, proto.Equal(w1.tail.lastState, state))

	w1.Close()

	// Last segment retains state after reopen.
	w2, err := openWal(dir, 10)
	assert.NilError(t, err)
	defer w2.Close()
	assert.Assert(t, proto.Equal(w2.GetState(), state))
	assert.Equal(t, len(w2.segments), 2)
	assert.Assert(t, proto.Equal(w2.tail.lastState, state))
}

func TestWalTruncateAfterStateUpdate(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w1.Close()

	// Set initial state.
	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	nextIndex, err := w1.Append(state, []*pb.LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 2, Command: []byte("b")},
	})
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 2)

	err = w1.TruncateEntriesFrom(0)
	assert.NilError(t, err)

	w1.Close()

	// Reopen.
	w2, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w2.Close()

	assert.Assert(t, proto.Equal(w2.GetState(), state))
}

func TestWalAppendAfterTruncating(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	nextIndex, err := w.Append(nil, []*pb.LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 2, Command: []byte("b")},
	})
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 2)

	err = w.TruncateEntriesFrom(1)
	assert.NilError(t, err)

	nextIndex, err = w.Append(nil, []*pb.LogEntry{
		{Term: 3, Command: []byte("c")},
		{Term: 4, Command: []byte("d")},
	})
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 3)

	retrievedEntries, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(retrievedEntries), 3, len(retrievedEntries))

	expectedEntries := []*pb.LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 3, Command: []byte("c")},
		{Term: 4, Command: []byte("d")},
	}
	for i := 0; i < len(expectedEntries); i++ {
		assert.Assert(t, proto.Equal(retrievedEntries[i], expectedEntries[i]))
	}
}

func TestWalTruncateMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
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
			Term:    1,
			Command: []byte("they see me rollin' they hatin'"),
		}
	}

	// Append in batches to create multiple segments.
	nextIndex, err := w.Append(state, entries[0:10])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 10)
	nextIndex, err = w.Append(state, entries[10:20])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 20)
	nextIndex, err = w.Append(state, entries[20:entryCount])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, 30)

	segmentCount := len(w.segments)
	assert.Assert(t, segmentCount > 1, segmentCount)

	// Truncate from an entry in the first segment.
	err = w.TruncateEntriesFrom(5)
	assert.NilError(t, err)
	assert.Assert(t, len(w.segments) < segmentCount)

	// Check remaining entries.
	assert.Equal(t, 5, w.EntryCount())
	remaining, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, 5, len(remaining))

	// Append after truncation.
	newEntries := []*pb.LogEntry{
		{Term: 2, Command: []byte("e5")},
		{Term: 2, Command: []byte("e6")},
	}
	_, err = w.Append(state, newEntries)
	assert.NilError(t, err)
	assert.Equal(t, 7, w.EntryCount())

	finalEntries, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(finalEntries), 7)

	for i := range 5 {
		assert.Assert(t, proto.Equal(entries[i], finalEntries[i]))
	}
	assert.Assert(t, proto.Equal(newEntries[0], finalEntries[5]))
	assert.Assert(t, proto.Equal(newEntries[1], finalEntries[6]))
}

func TestWalStorageFailures(t *testing.T) {
	dir := t.TempDir()

	// Create a directory with restricted permissions.
	restrictedDir := filepath.Join(dir, "restricted")
	err := os.Mkdir(restrictedDir, 0400) // read-only
	assert.NilError(t, err)

	// Opening a WAL in a read-only directory fails.
	_, err = openWal(restrictedDir, 1024)
	assert.Assert(t, err != nil)

	w, err := openWal(dir, 1024)
	assert.NilError(t, err)

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	err = w.SetState(state)
	assert.NilError(t, err)

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
	}

	_, err = w.Append(state, entries)
	assert.NilError(t, err)

	// Test writing to closed segment file.
	f := w.tail.f
	f.Close()

	_, err = w.Append(state, entries)
	assert.Assert(t, err != nil)

	err = w.SetState(state)
	assert.Assert(t, err != nil)
}
