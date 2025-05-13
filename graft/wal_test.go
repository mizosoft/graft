package graft

import (
	"fmt"
	"github.com/mizosoft/graft/raftpb"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mizosoft/graft/graftpb"
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
	testutil.AssertNil(t, wal.RetrieveState())
	testutil.AssertNil(t, wal.RetrieveSnapshot())
}

func TestWalAppendSingleEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entry := &raftpb.LogEntry{
		Term: 1,
		Data: []byte("cmd"),
	}

	nextIndex := w.Append(state, []*raftpb.LogEntry{entry})
	assert.Equal(t, nextIndex, int64(1))
	assert.Equal(t, w.EntryCount(), int64(1))
	assert.Assert(t, proto.Equal(state, w.RetrieveState()))

	// Retrieve entry and verify.
	retrievedEntry := w.GetEntry(0)
	assert.Assert(t, proto.Equal(retrievedEntry, entry))
	assert.Equal(t, retrievedEntry.Index, int64(0))
}

func TestWalAppendMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
		{Term: 1, Data: []byte("cmd2")},
		{Term: 1, Data: []byte("cmd3")},
	}

	nextIndex := w.Append(state, entries)
	assert.Equal(t, nextIndex, int64(3))
	assert.Equal(t, w.EntryCount(), int64(3))

	// Retrieve and verify entries.
	retrievedEntries := w.GetEntriesFrom(0)
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

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("entry 0")},
		{Term: 1, Data: []byte("entry 1")},
		{Term: 1, Data: []byte("entry 2")},
		{Term: 1, Data: []byte("entry 3")},
		{Term: 1, Data: []byte("entry 4")},
	}

	w.Append(state, entries)

	retrievedEntries := w.GetEntriesFrom(2)
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
	state1 := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	w.SaveState(state1)
	assert.Assert(t, proto.Equal(state1, w.RetrieveState()))

	// Updated state
	state2 := &graftpb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s1",
		CommitIndex: 5,
	}

	w.SaveState(state2)
	assert.Assert(t, proto.Equal(state2, w.RetrieveState()))
}

func TestWalTruncateEntries(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w.Close()

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd0")},
		{Term: 1, Data: []byte("cmd1")},
		{Term: 1, Data: []byte("cmd2")},
		{Term: 1, Data: []byte("cmd3")},
		{Term: 1, Data: []byte("cmd4")},
	}

	w.Append(state, entries)
	assert.Equal(t, w.EntryCount(), int64(5))

	w.TruncateEntriesFrom(3)
	assert.Equal(t, w.EntryCount(), int64(3))

	retrievedEntries := w.GetEntriesFrom(0)
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

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	// Fill multiple segments.
	entryCount := 30
	entries := make([]*raftpb.LogEntry, entryCount)
	for i := range entryCount {
		entries[i] = &raftpb.LogEntry{
			Term: 1,
			Data: []byte("entry data that is long enough to force segment creation"),
		}
	}

	// Append in batches to create multiple segments.
	nextIndex := w.Append(state, entries[0:10])
	assert.Equal(t, nextIndex, int64(10))
	nextIndex = w.Append(state, entries[10:20])
	assert.Equal(t, nextIndex, int64(20))
	nextIndex = w.Append(state, entries[20:entryCount])
	assert.Equal(t, nextIndex, int64(30))

	assert.Assert(t, len(w.segments) > 1, len(w.segments))

	// All entries can be retrieved.
	allEntries := w.GetEntriesFrom(0)
	assert.Equal(t, len(allEntries), entryCount)

	// Check getting individual entries across segments.
	for i := range entryCount {
		entry := w.GetEntry(int64(i))
		assert.Assert(t, entry != nil)
	}
}

func TestWalReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 1024)
	assert.NilError(t, err)

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 5,
	}

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("entry 0")},
		{Term: 1, Data: []byte("entry 1")},
		{Term: 1, Data: []byte("entry 2")},
	}

	w1.Append(state, entries)
	w1.Close()

	// Reopen.
	w2, err := openWal(dir, 1024)
	assert.NilError(t, err)
	defer w2.Close()

	// Verify state was recovered.
	assert.Assert(t, proto.Equal(w2.RetrieveState(), state))

	// Verify entries were recovered.
	assert.Equal(t, w2.EntryCount(), int64(3))

	recoveredEntries := w2.GetEntriesFrom(0)
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

	func() {
		defer func() {
			r := recover()
			assert.ErrorType(t, r.(error), indexOutOfRangeError{})
		}()

		w.GetEntry(100)
	}()

	// TestWal using closed WAL
	w.Close()

	func() {
		defer func() {
			r := recover()
			assert.ErrorIs(t, r.(error), errClosed)
		}()

		w.GetEntry(0)
	}()
}

func TestWalCorruptedCRC(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 1024)
	assert.NilError(t, err)

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	entry := &raftpb.LogEntry{
		Term: 1,
		Data: []byte("test"),
	}

	w.Append(state, []*raftpb.LogEntry{entry})
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
	state1 := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	w.SaveState(state1)

	// Update state.
	state2 := &graftpb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s2",
		CommitIndex: 5,
	}
	w.SaveState(state2)

	// Verify latest state is returned.
	assert.Assert(t, proto.Equal(w.RetrieveState(), state2))

	// Reopen.
	w.Close()
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

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "some cool server",
		CommitIndex: 1,
	}
	w1.SaveState(state)
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
	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	nextIndex := w1.Append(state, []*raftpb.LogEntry{
		{Term: 1, Data: []byte("a")},
		{Term: 2, Data: []byte("b")},
	})
	assert.NilError(t, err)
	assert.Equal(t, nextIndex, int64(2))

	w1.TruncateEntriesFrom(0)

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

	nextIndex := w.Append(nil, []*raftpb.LogEntry{
		{Term: 1, Data: []byte("a")},
		{Term: 2, Data: []byte("b")},
	})
	assert.Equal(t, nextIndex, int64(2))

	w.TruncateEntriesFrom(1)

	nextIndex = w.Append(nil, []*raftpb.LogEntry{
		{Term: 3, Data: []byte("c")},
		{Term: 4, Data: []byte("d")},
	})
	assert.Equal(t, nextIndex, int64(3))

	retrievedEntries := w.GetEntriesFrom(0)
	assert.Equal(t, len(retrievedEntries), 3, len(retrievedEntries))

	expectedEntries := []*raftpb.LogEntry{
		{Term: 1, Index: 0, Data: []byte("a")},
		{Term: 3, Index: 1, Data: []byte("c")},
		{Term: 4, Index: 2, Data: []byte("d")},
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

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}

	// Fill multiple segments.
	entryCount := 30
	entries := make([]*raftpb.LogEntry, entryCount)
	for i := range entryCount {
		entries[i] = &raftpb.LogEntry{
			Term: 1,
			Data: []byte("they see me rollin' they hatin'"),
		}
	}

	// Append in batches to create multiple segments.
	nextIndex := w.Append(state, entries[0:10])
	assert.Equal(t, nextIndex, int64(10))
	nextIndex = w.Append(state, entries[10:20])
	assert.Equal(t, nextIndex, int64(20))
	nextIndex = w.Append(state, entries[20:entryCount])
	assert.Equal(t, nextIndex, int64(30))

	segmentCount := len(w.segments)
	assert.Assert(t, segmentCount > 1, segmentCount)

	// Truncate from an entry in the first segment.
	w.TruncateEntriesFrom(5)
	assert.Assert(t, len(w.segments) < segmentCount)

	// Check remaining entries.
	assert.Equal(t, w.EntryCount(), int64(5))
	remaining := w.GetEntriesFrom(0)
	assert.Equal(t, 5, len(remaining))

	// Append after truncation.
	newEntries := []*raftpb.LogEntry{
		{Term: 2, Data: []byte("e5")},
		{Term: 2, Data: []byte("e6")},
	}
	w.Append(state, newEntries)
	assert.Equal(t, w.EntryCount(), int64(7))

	finalEntries := w.GetEntriesFrom(0)
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

	state := &graftpb.PersistedState{
		CurrentTerm: 1,
		VotedFor:    "s1",
		CommitIndex: 0,
	}
	w.SaveState(state)
	assert.NilError(t, err)

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
	}

	w.Append(state, entries)

	// Test writing to closed segment file.
	f := w.tail.f
	f.Close()

	w.Append(state, entries)

	w.SaveState(&graftpb.PersistedState{
		CurrentTerm: 2,
		VotedFor:    "s2",
		CommitIndex: 0,
	})
}

func TestWalHeadEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	entry := w.HeadEntry()
	testutil.AssertNil(t, entry)

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
		{Term: 2, Data: []byte("cmd2")},
	}

	lastIndex := w.Append(nil, entries)
	assert.Equal(t, lastIndex, int64(2))

	entry = w.HeadEntry()
	assert.Equal(t, entry.Index, int64(0))
	assert.Assert(t, proto.Equal(entry, entries[0]))
}

func TestWalTailEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 32)
	assert.NilError(t, err)
	defer w.Close()

	entry := w.TailEntry()
	testutil.AssertNil(t, entry)

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
		{Term: 2, Data: []byte("cmd2")},
	}

	lastIndex := w.Append(nil, entries)
	assert.Equal(t, lastIndex, int64(2))

	entry = w.TailEntry()
	assert.Assert(t, proto.Equal(entry, entries[1]))
	assert.Equal(t, entry.Index, int64(1))
}

func TestWalTailEntryWithEmptySegments(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 20)
	assert.NilError(t, err)
	defer w.Close()

	entry := w.TailEntry()
	testutil.AssertNil(t, entry)

	entries := []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
		{Term: 1, Data: []byte("cmd2")},
	}
	lastIndex := w.Append(nil, entries)
	assert.Equal(t, lastIndex, int64(2))

	w.SaveState(&graftpb.PersistedState{CurrentTerm: 1, VotedFor: "s1", CommitIndex: 0})

	w.SaveState(&graftpb.PersistedState{CurrentTerm: 2, VotedFor: "s2", CommitIndex: 1})

	assert.Assert(t, len(w.segments) > 2, len(w.segments))

	entry = w.TailEntry()
	assert.Assert(t, proto.Equal(entry, entries[1]))
	assert.Equal(t, entry.Index, int64(1))
}

func TestWalMismatchingSegNumber(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	lastIndex := w1.Append(nil, []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
	})
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

	lastIndex := w1.Append(nil, []*raftpb.LogEntry{
		{Term: 1, Data: []byte("cmd1")},
	})
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

func TestWalGetEntriesFromTo(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	entryCount := 40
	var entries []*raftpb.LogEntry
	for range entryCount / 2 {
		term := int64(len(entries))
		entries = append(entries, &raftpb.LogEntry{
			Term: term,
			Data: []byte(fmt.Sprintf("entry data %d that is long enough to force segment creation", term)),
		})
	}

	checkEntries := func(retrieved []*raftpb.LogEntry, expected []*raftpb.LogEntry) {
		assert.Equal(t, len(retrieved), len(expected))
		for j, entry := range expected {
			assert.Assert(t, proto.Equal(retrieved[j], entry), retrieved[j].String(), entry.String())
		}
	}

	for i := range entries {
		state := &graftpb.PersistedState{CurrentTerm: int64(i), VotedFor: "s1"}
		nextIndex := w.Append(state, []*raftpb.LogEntry{entries[i]})
		assert.Equal(t, nextIndex, int64(i+1))

		retrieved := w.GetEntries(0, int64(i))
		checkEntries(retrieved, entries[:i+1])
	}

	// Put a bunch of states to make segments with not entries.
	segCount := len(w.segments)
	_, lastTerm := w.LastLogIndexAndTerm()
	for segCount == len(w.segments) {
		w.SaveState(&graftpb.PersistedState{CurrentTerm: lastTerm, VotedFor: "s1"})
		lastTerm++
	}

	for range entryCount / 2 {
		term := int64(len(entries))
		entries = append(entries, &raftpb.LogEntry{
			Term: term,
			Data: []byte(fmt.Sprintf("entry data %d that is long enough to force segment creation", term)),
		})
	}

	for i := entryCount / 2; i < entryCount; i++ {
		state := &graftpb.PersistedState{CurrentTerm: int64(i), VotedFor: "s1"}
		nextIndex := w.Append(state, []*raftpb.LogEntry{entries[i]})
		assert.Equal(t, nextIndex, int64(i+1))

		retrieved := w.GetEntries(0, int64(i))
		checkEntries(retrieved, entries[:i+1])
	}

	lastIndex, _ := w.LastLogIndexAndTerm()
	for i := range entries {
		retrieved := w.GetEntries(int64(i), lastIndex)
		checkEntries(retrieved, entries[i:])
	}

	// Try all valid index sequences.
	for i := 0; i < entryCount; i++ {
		for j := i; j < entryCount; j++ {
			retrieved := w.GetEntries(int64(i), int64(j))
			checkEntries(retrieved, entries[i:j+1])
		}
	}
}

func TestWalSaveAndRetrieveSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w.Close()

	snapshot := w.RetrieveSnapshot()
	assert.Assert(t, snapshot == nil)

	snapshot = NewSnapshot(&graftpb.SnapshotMetadata{LastAppliedIndex: 1, LastAppliedTerm: 1}, []byte("Pikachu"))
	w.SaveSnapshot(snapshot)

	retrievedSnapshot := w.RetrieveSnapshot()
	assert.Assert(t, proto.Equal(retrievedSnapshot.Metadata(), snapshot.Metadata()))
	assert.Equal(t, string(retrievedSnapshot.Data()), string(snapshot.Data()))
}

func TestWalRetrieveSnapshotOnReopen(t *testing.T) {
	dir := t.TempDir()
	w1, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w1.Close()

	snapshot := NewSnapshot(&graftpb.SnapshotMetadata{LastAppliedIndex: 1, LastAppliedTerm: 1}, []byte("Pikachu"))
	w1.SaveSnapshot(snapshot)
	assert.NilError(t, err)

	w1.Close()

	w2, err := openWal(dir, 128)
	assert.NilError(t, err)
	defer w2.Close()
	retrievedSnapshot := w2.RetrieveSnapshot()
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
	entries := make([]*raftpb.LogEntry, entryCount)
	for i := range entries {
		commandSize := rnd.Intn(256)
		entries[i] = &raftpb.LogEntry{
			Index: int64(i),
			Term:  int64(i),
			Data:  []byte(strings.Repeat("a", commandSize)),
		}
	}

	for _, entry := range entries {
		w.Append(nil, []*raftpb.LogEntry{entry})
	}

	assert.Assert(t, len(w.segments) > 1)

	for i := int64(0); i < entryCount; i++ {
		w.TruncateEntriesTo(i)

		firstIndex, _ := w.FirstLogIndexAndTerm()
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
