package persistencetest

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

// A committed snapshot becomes the current one, with the metadata it was created with and its size recorded, and opens
// to what was written to it. No other snapshot opens.
func testCommittedSnapshotIsCurrent(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	metadata := configChangeMetadata(5)
	_, err := p.OpenSnapshot(metadata)
	assert.ErrorIs(t, err, graft.ErrNoSuchSnapshot)

	writer, err := p.CreateSnapshot(proto.CloneOf(metadata))
	assert.NilError(t, err)
	for _, write := range []struct {
		data string
		off  int64
	}{{"Char", 0}, {"mander", 4}} {
		n, err := writer.WriteAt([]byte(write.data), write.off)
		assert.NilError(t, err)
		assert.Equal(t, n, len(write.data))
	}
	assert.NilError(t, writer.Commit())

	committed := writer.Metadata()
	assert.Equal(t, committed.Size, int64(len("Charmander")))
	want := proto.CloneOf(metadata)
	want.Size, want.Checksum = committed.Size, committed.Checksum // The checksum is the backend's to choose.
	assert.Assert(t, proto.Equal(committed, want), "committed %v, want %v", committed, want)
	assertCurrentSnapshot(t, p, committed, "Charmander")

	snapshot, err := p.OpenSnapshot(committed)
	assert.NilError(t, err)
	data, err := io.ReadAll(snapshot.Reader())
	assert.NilError(t, err)
	assert.Equal(t, string(data), "Charmander")
	assert.NilError(t, snapshot.Close())

	for _, other := range []*pb.SnapshotMetadata{
		{LastIncludedIndex: 4, LastIncludedTerm: 2},
		{LastIncludedIndex: 5, LastIncludedTerm: 1},
	} {
		_, err = p.OpenSnapshot(other)
		assert.ErrorIs(t, err, graft.ErrNoSuchSnapshot, "opened %v", other)
	}
}

// A writer closed without committing leaves the current snapshot as it was.
func testClosedWriterCommitsNothing(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	assert.NilError(t, writeSnapshot(t, p, 3, "Eevee").Close())
	metadata, err := p.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, metadata == nil, "has a snapshot: %v", metadata)

	committed := commitSnapshot(t, p, 5, "five")
	assertCurrentSnapshot(t, p, committed, "five")
	assert.NilError(t, writeSnapshot(t, p, 7, "Ditto").Close())
	assertCurrentSnapshot(t, p, committed, "five")
}

// A local snapshot and one installed by the leader can commit in either order. The older one landing last must neither
// become current nor delete the newer one as its predecessor.
func testCommitRejectsOlderSnapshots(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	older := writeSnapshot(t, p, 3, "three")
	committed := commitSnapshot(t, p, 5, "five")
	assert.ErrorIs(t, older.Commit(), graft.ErrSnapshotOutOfDate)
	assertCurrentSnapshot(t, p, committed, "five")
}

// A snapshot that isn't newer than the current one would be rejected on commit, so it isn't written at all.
func testCreateRejectsSnapshotsNoNewerThanCurrent(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	committed := commitSnapshot(t, p, 5, "five")
	for _, index := range []int64{3, 5} {
		_, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
		assert.ErrorIs(t, err, graft.ErrSnapshotOutOfDate)
	}
	assertCurrentSnapshot(t, p, committed, "five")
}

// Two writers can hold the same metadata at once, e.g. a local snapshot and a leader's install at the same index.
func testWritersOfTheSameSnapshotCoexist(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	first := writeSnapshot(t, p, 5, "Mew")
	second := writeSnapshot(t, p, 5, "Mewtwo")
	assert.NilError(t, first.Commit())
	assert.ErrorIs(t, second.Commit(), graft.ErrSnapshotOutOfDate)
	assertCurrentSnapshot(t, p, first.Metadata(), "Mew")
}

// A write can jump back over bytes written already, and the snapshot reads back as finally written.
func testSnapshotWritesCanJumpBack(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	writer := writeSnapshot(t, p, 1, "aaaa")
	for _, write := range []struct {
		data string
		off  int64
	}{{"bbbb", 4}, {"cc", 2}} {
		_, err := writer.WriteAt([]byte(write.data), write.off)
		assert.NilError(t, err)
	}
	assert.NilError(t, writer.Commit())
	assertCurrentSnapshot(t, p, writer.Metadata(), "aaccbbbb")
}

// A write can't start past the end of what's been written.
func testSnapshotWritesCantLeaveGaps(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	writer := writeSnapshot(t, p, 1, "Jynx")
	_, err := writer.WriteAt([]byte("Ditto"), 5)
	assert.ErrorIs(t, err, graft.ErrOffsetOutOfRange)
	assert.NilError(t, writer.Commit())
	assertCurrentSnapshot(t, p, writer.Metadata(), "Jynx")
}

// A writer can't be written to or committed once it's closed or committed.
func testClosedWritersRefuseUse(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	closed := writeSnapshot(t, p, 1, "Jynx")
	assert.NilError(t, closed.Close())
	_, err := closed.WriteAt([]byte("Ditto"), 4)
	assert.ErrorIs(t, err, graft.ErrClosed)
	assert.ErrorIs(t, closed.Commit(), graft.ErrClosed)

	committed := writeSnapshot(t, p, 2, "Mewtwo")
	assert.NilError(t, committed.Commit())
	_, err = committed.WriteAt([]byte("Ditto"), 6)
	assert.ErrorIs(t, err, graft.ErrClosed)
	assert.ErrorIs(t, committed.Commit(), graft.ErrClosed)
	assertCurrentSnapshot(t, p, committed.Metadata(), "Mewtwo")
}

// A writer that a newer commit makes stale fails from its next write on, whether it has written anything yet or not,
// rather than writing to the end first. Newer writers carry on.
func testNewerCommitsDiscardStaleWriters(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	emptyOlder := createSnapshot(t, p, 3)
	startedOlder := writeSnapshot(t, p, 4, "fo")
	emptyNewer := createSnapshot(t, p, 7)
	startedNewer := writeSnapshot(t, p, 8, "ei")

	commitSnapshot(t, p, 5, "five")
	for _, write := range []struct {
		writer graft.SnapshotWriter
		data   string
		off    int64
	}{{emptyOlder, "three", 0}, {startedOlder, "ur", 2}} {
		_, err := write.writer.WriteAt([]byte(write.data), write.off)
		assert.ErrorIs(t, err, graft.ErrSnapshotOutOfDate)
		assert.ErrorIs(t, write.writer.Commit(), graft.ErrSnapshotOutOfDate)
	}

	_, err := emptyNewer.WriteAt([]byte("seven"), 0)
	assert.NilError(t, err)
	assert.NilError(t, emptyNewer.Commit())
	assertCurrentSnapshot(t, p, emptyNewer.Metadata(), "seven")
	_, err = startedNewer.WriteAt([]byte("ght"), 2)
	assert.NilError(t, err)
	assert.NilError(t, startedNewer.Commit())
	assertCurrentSnapshot(t, p, startedNewer.Metadata(), "eight")
}

// Closing the persistence fails the snapshot writers still open, whether they've written anything yet or not.
func testClosingFailsOpenWriters(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	empty := createSnapshot(t, p, 1)
	started := writeSnapshot(t, p, 2, "Psy")
	assert.NilError(t, p.Close())

	for _, write := range []struct {
		writer graft.SnapshotWriter
		data   string
		off    int64
	}{{empty, "one", 0}, {started, "duck", 3}} {
		_, err := write.writer.WriteAt([]byte(write.data), write.off)
		assert.ErrorIs(t, err, graft.ErrClosed)
		assert.ErrorIs(t, write.writer.Commit(), graft.ErrClosed)
	}
}

func testConcurrentCommitsLeaveTheNewestCurrent(t *testing.T, factory *persistenceFactory) {
	p := factory.open()
	const n = 16
	committed := make([]*pb.SnapshotMetadata, n)
	errs := make([]error, n)
	// Unlike commitSnapshot, this runs off the test's goroutine, and losing the race is expected: it returns errors
	// rather than failing.
	commit := func(index int64) (*pb.SnapshotMetadata, error) {
		writer, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
		if err != nil {
			return nil, err
		}
		if _, err := writer.WriteAt([]byte(fmt.Sprint(index)), 0); err != nil {
			return nil, errors.Join(err, writer.Close())
		}
		if err := writer.Commit(); err != nil {
			return nil, err
		}
		return writer.Metadata(), nil
	}
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			committed[i], errs[i] = commit(int64(i))
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			assert.ErrorIs(t, err, graft.ErrSnapshotOutOfDate)
		}
	}
	assertCurrentSnapshot(t, p, committed[n-1], fmt.Sprint(n-1))
}
