package graft

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
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

	assert.Equal(t, wal.EntryCount(), int64(0))

	e, err := wal.GetEntry(0)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	testutil.AssertNil(t, e)

	e, err = wal.GetEntry(100)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	testutil.AssertNil(t, e)

	es, err := wal.GetEntriesFrom(0)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	assert.Equal(t, len(es), 0)

	_, err = wal.GetEntriesFrom(100)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	assert.Equal(t, len(es), 0)
	testutil.AssertNil(t, wal.RetrieveState())
}

func TestWalEmptyWalReopen(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWal(dir, 1024)
	assert.NilError(t, err)
	wal.Close()

	wal, err = openWal(dir, 1024)
	assert.NilError(t, err)
	defer wal.Close()

	assert.Equal(t, wal.EntryCount(), int64(0))

	e, err := wal.GetEntry(0)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	testutil.AssertNil(t, e)

	e, err = wal.GetEntry(100)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	testutil.AssertNil(t, e)

	es, err := wal.GetEntriesFrom(0)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	assert.Equal(t, len(es), 0)

	_, err = wal.GetEntriesFrom(100)
	assert.ErrorType(t, err, indexOutOfRangeError{})
	assert.Equal(t, len(es), 0)

	testutil.AssertNil(t, wal.RetrieveState())
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
	assert.Equal(t, nextIndex, int64(1))
	assert.Equal(t, w.EntryCount(), int64(1))
	assert.Assert(t, proto.Equal(state, w.RetrieveState()))

	// Retrieve entry and verify.
	retrievedEntry, err := w.GetEntry(0)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(retrievedEntry, entry))
	assert.Equal(t, retrievedEntry.Index, int64(0))
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
	assert.Equal(t, nextIndex, int64(3))
	assert.Equal(t, w.EntryCount(), int64(3))

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
	assert.Assert(t, proto.Equal(retrievedEntries[0], entries[2]))
	assert.Equal(t, retrievedEntries[0].Index, int64(2))
	assert.Assert(t, proto.Equal(retrievedEntries[1], entries[3]))
	assert.Equal(t, retrievedEntries[1].Index, int64(3))
	assert.Assert(t, proto.Equal(retrievedEntries[2], entries[4]))
	assert.Equal(t, retrievedEntries[2].Index, int64(4))
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

	err = w.SaveState(state1)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(state1, w.RetrieveState()))

	// Updated state
	state2 := &pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s1",
		CommitIndex: 5,
	}

	err = w.SaveState(state2)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(state2, w.RetrieveState()))
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
	assert.Assert(t, proto.Equal(state, w.RetrieveState()))
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
	assert.Equal(t, nextIndex, int64(10))
	nextIndex, err = w.Append(state, entries[10:20])
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, int64(20))
	nextIndex, err = w.Append(state, entries[20:entryCount])
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
	assert.Assert(t, proto.Equal(w2.RetrieveState(), state))

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
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	_, err = w.GetEntry(100)
	assert.ErrorType(t, err, indexOutOfRangeError{})

	// TestWal using closed WAL
	w.Close()

	_, err = w.GetEntry(0)
	assert.ErrorIs(t, err, errClosed)

	_, err = w.Append(nil, nil)
	assert.ErrorIs(t, err, errClosed)

	err = w.SaveState(nil)
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
	assert.Assert(t, proto.Equal(w.RetrieveState(), state2))

	// Reopen.
	err = w.Close()
	assert.NilError(t, err)

	w2, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w2.Close()

	// Verify latest state is recovered.
	assert.Assert(t, proto.Equal(w2.RetrieveState(), state2))
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
	err = w1.SaveState(state)
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(w1.RetrieveState(), state))
	assert.Equal(t, len(w1.segments), 2)

	// Last segment retains state.
	assert.Assert(t, proto.Equal(w1.lastState, state))

	w1.Close()

	// Last segment retains state after reopen.
	w2, err := openWal(dir, 10)
	assert.NilError(t, err)
	defer w2.Close()
	assert.Assert(t, proto.Equal(w2.RetrieveState(), state))
	assert.Equal(t, len(w2.segments), 2)
	assert.Assert(t, proto.Equal(w2.lastState, state))
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
	assert.Equal(t, nextIndex, int64(2))

	err = w1.TruncateEntriesFrom(0)
	assert.NilError(t, err)

	w1.Close()

	// Reopen.
	w2, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w2.Close()

	assert.Assert(t, proto.Equal(w2.RetrieveState(), state))
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
	assert.Equal(t, nextIndex, int64(2))

	err = w.TruncateEntriesFrom(1)
	assert.NilError(t, err)

	nextIndex, err = w.Append(nil, []*pb.LogEntry{
		{Term: 3, Command: []byte("c")},
		{Term: 4, Command: []byte("d")},
	})
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, int64(3))

	retrievedEntries, err := w.GetEntriesFrom(0)
	assert.NilError(t, err)
	assert.Equal(t, len(retrievedEntries), 3, len(retrievedEntries))

	expectedEntries := []*pb.LogEntry{
		{Term: 1, Index: 0, Command: []byte("a")},
		{Term: 3, Index: 1, Command: []byte("c")},
		{Term: 4, Index: 2, Command: []byte("d")},
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
		{Term: 2, Command: []byte("e5")},
		{Term: 2, Command: []byte("e6")},
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
	defer w.Close()

	state := &pb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	err = w.SaveState(state)
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

	err = w.SaveState(&pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s2",
		CommitIndex: 0,
	})
	assert.Assert(t, err != nil)
}

func TestWalHeadEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	entry, err := w.HeadEntry()
	assert.NilError(t, err)
	testutil.AssertNil(t, entry)

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
		{Term: 2, Command: []byte("cmd2")},
	}

	lastIndex, err := w.Append(nil, entries)
	assert.NilError(t, err)
	assert.Equal(t, lastIndex, int64(2))

	entry, err = w.HeadEntry()
	assert.NilError(t, err)
	assert.Equal(t, entry.Index, int64(0))
	assert.Assert(t, proto.Equal(entry, entries[0]))
}

func TestWalTailEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 32)
	assert.NilError(t, err)
	defer w.Close()

	entry, err := w.TailEntry()
	assert.NilError(t, err)
	testutil.AssertNil(t, entry)

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
		{Term: 2, Command: []byte("cmd2")},
	}

	lastIndex, err := w.Append(nil, entries)
	assert.NilError(t, err)
	assert.Equal(t, lastIndex, int64(2))

	entry, err = w.TailEntry()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(entry, entries[1]))
	assert.Equal(t, entry.Index, int64(1))
}

func TestWalTailEntryWithEmptySegments(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 20)
	assert.NilError(t, err)
	defer w.Close()

	entry, err := w.TailEntry()
	assert.NilError(t, err)
	testutil.AssertNil(t, entry)

	entries := []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
		{Term: 1, Command: []byte("cmd2")},
	}
	lastIndex, err := w.Append(nil, entries)
	assert.NilError(t, err)
	assert.Equal(t, lastIndex, int64(2))

	err = w.SaveState(&pb.PersistedState{CurrentTerm: 1, VotedFor: "s1", CommitIndex: 0})
	assert.NilError(t, err)

	err = w.SaveState(&pb.PersistedState{CurrentTerm: 2, VotedFor: "s2", CommitIndex: 1})
	assert.NilError(t, err)

	assert.Assert(t, len(w.segments) > 2, len(w.segments))

	entry, err = w.TailEntry()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(entry, entries[1]))
	assert.Equal(t, entry.Index, int64(1))
}

func TestWalMismatchingSegNumber(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	lastIndex, err := w1.Append(nil, []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
	})
	assert.NilError(t, err)
	assert.Equal(t, lastIndex, int64(1))
	assert.Assert(t, len(w1.segments) == 1, len(w1.segments))

	_, fname := path.Split(w1.tail.fpath)
	assert.Equal(t, fname, "log_0_0.dat")

	w1.Close()

	if err := os.Rename(w1.tail.fpath, path.Join(dir, "log_1_0.dat")); err != nil {
		assert.NilError(t, err)
	}

	w2, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	_, fname = path.Split(w2.tail.fpath)
	assert.Equal(t, fname, "log_0_0.dat")
	_, fname = path.Split(w2.tail.f.Name())
	assert.Equal(t, fname, "log_0_0.dat")
	assert.Equal(t, w2.tail.number, 0)
}

func TestWalMismatchingFirstIndex(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	lastIndex, err := w1.Append(nil, []*pb.LogEntry{
		{Term: 1, Command: []byte("cmd1")},
	})
	assert.NilError(t, err)
	assert.Equal(t, lastIndex, int64(1))
	assert.Assert(t, len(w1.segments) == 1, len(w1.segments))

	_, fname := path.Split(w1.tail.fpath)
	assert.Equal(t, fname, "log_0_0.dat")

	w1.Close()

	if err := os.Rename(w1.tail.fpath, path.Join(dir, "log_0_1.dat")); err != nil {
		assert.NilError(t, err)
	}

	w2, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	_, fname = path.Split(w2.tail.fpath)
	assert.Equal(t, fname, "log_0_0.dat")
	_, fname = path.Split(w2.tail.f.Name())
	assert.Equal(t, fname, "log_0_0.dat")
	assert.Equal(t, w2.tail.firstIndex, int64(0))
}

func TestWalAppendCommands(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	state := &pb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	commands := [][]byte{[]byte("cmd1"), []byte("cmd2"), []byte("cmd3"), []byte("cmd4")}

	entries, err := w.AppendCommands(state, commands[0:2])
	assert.NilError(t, err)
	assert.Assert(t, len(entries) == 2, len(entries))

	entries2, err := w.AppendCommands(state, commands[2:])
	assert.NilError(t, err)
	assert.Assert(t, len(entries2) == 2, len(entries2))

	assert.Equal(t, w.EntryCount(), int64(4))

	entries = append(entries, entries2...)
	for i, entry := range entries {
		assert.Equal(t, entry.Term, state.CurrentTerm)
		assert.Equal(t, string(entry.Command), string(commands[i]))
	}
}

func TestWalGetEntriesFromTo(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	entryCount := 40
	var entries []*pb.LogEntry
	for range entryCount / 2 {
		term := int64(len(entries))
		entries = append(entries, &pb.LogEntry{
			Term:    term,
			Command: []byte(fmt.Sprintf("entry data %d that is long enough to force segment creation", term)),
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
	_, lastTerm := w.LastLogIndexAndTerm()
	for segCount == len(w.segments) {
		err := w.SaveState(&pb.PersistedState{CurrentTerm: lastTerm, VotedFor: "s1"})
		assert.NilError(t, err)
		lastTerm++
	}

	for range entryCount / 2 {
		term := int64(len(entries))
		entries = append(entries, &pb.LogEntry{
			Term:    term,
			Command: []byte(fmt.Sprintf("entry data %d that is long enough to force segment creation", term)),
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

	lastIndex, _ := w.LastLogIndexAndTerm()
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
}

func TestWalSaveAndRetrieveSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	snapshot, err := w.RetrieveSnapshot()
	assert.NilError(t, err)
	assert.Assert(t, snapshot == nil)

	snapshot = NewSnapshot(&pb.SnapshotMetadata{LastAppliedIndex: 1, LastAppliedTerm: 1}, []byte("Pikachu"))
	err = w.SaveSnapshot(snapshot)
	assert.NilError(t, err)

	retrievedSnapshot, err := w.RetrieveSnapshot()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(retrievedSnapshot.Metadata(), snapshot.Metadata()))
	assert.Equal(t, string(retrievedSnapshot.Data()), string(snapshot.Data()))
}

func TestWalRetrieveSnapshotOnReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	snapshot := NewSnapshot(&pb.SnapshotMetadata{LastAppliedIndex: 1, LastAppliedTerm: 1}, []byte("Pikachu"))
	err = w1.SaveSnapshot(snapshot)
	assert.NilError(t, err)

	w1.Close()

	w2, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w2.Close()
	retrievedSnapshot, err := w2.RetrieveSnapshot()
	assert.NilError(t, err)
	assert.Assert(t, proto.Equal(retrievedSnapshot.Metadata(), snapshot.Metadata()))
	assert.Equal(t, string(retrievedSnapshot.Data()), string(snapshot.Data()))
}

func TestWalTruncateEntriesTo(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	rnd := rand.New(rand.NewSource(0))
	entryCount := int64(100)
	entries := make([]*pb.LogEntry, entryCount)
	for i := range entries {
		commandSize := rnd.Intn(256)
		entries[i] = &pb.LogEntry{
			Index:   int64(i),
			Term:    int64(i),
			Command: []byte(strings.Repeat("a", commandSize)),
		}
	}

	for _, entry := range entries {
		_, err = w.Append(nil, []*pb.LogEntry{entry})
		assert.NilError(t, err)
	}

	assert.Assert(t, len(w.segments) > 1)

	for i := int64(0); i < entryCount; i++ {
		err := w.TruncateEntriesTo(i)
		assert.NilError(t, err)

		firstIndex, _ := w.FirstLogIndexAndTerm()
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
