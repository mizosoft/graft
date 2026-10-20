package graft

import (
	"errors"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/mizosoft/graft/pb"
	"gotest.tools/v3/assert"
)

// getMetadata runs under the lock, and commitMetadata under the write lock, on commit and on open alike, so a commit
// can't delete a file between Open's check and its open.
func TestFileSnapshotterCallsBackUnderLock(t *testing.T) {
	var mut sync.RWMutex
	var current *pb.SnapshotMetadata
	files, err := OpenFileSnapshotter(t.TempDir(), &mut, func() (*pb.SnapshotMetadata, error) {
		if mut.TryLock() {
			mut.Unlock()
			t.Error("got metadata without the lock")
		}
		return current, nil
	}, func(metadata *pb.SnapshotMetadata) error {
		if mut.TryRLock() {
			mut.RUnlock()
			t.Error("committed metadata without the write lock")
		}
		current = metadata
		return nil
	}, nil)
	assert.NilError(t, err)

	metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
	writer, err := files.Create(metadata)
	assert.NilError(t, err)
	_, err = writer.WriteAt([]byte("abc"), 0)
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())

	snapshot, err := files.Open(metadata)
	assert.NilError(t, err)
	defer snapshot.Close()
	data, err := snapshot.ReadAll()
	assert.NilError(t, err)
	assert.Equal(t, string(data), "abc")
}

// Create and Open only read the metadata, so they don't wait for the Persistence's own readers.
func TestFileSnapshotterSharesTheLockWithReaders(t *testing.T) {
	var mut sync.RWMutex
	files := testFileSnapshotter(t, t.TempDir(), &mut, nil)
	metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}

	mut.RLock()
	defer mut.RUnlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if writer, err := files.Create(metadata); assert.Check(t, err) {
			assert.Check(t, writer.Close())
		}
		_, err := files.Open(metadata)
		assert.Check(t, errors.Is(err, ErrNoSuchSnapshot))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Create or Open waited for a reader")
	}
}

// Only snapshots being written end in .tmp, so they can't be mistaken for the current one.
func TestFileSnapshotterFileNames(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)

	writer, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 2})
	assert.NilError(t, err)
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_1_2.1.tmp"})
	assert.NilError(t, writer.Commit())
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_1_2.snap"})
}

// A writer discarded for a newer snapshot takes its temp file with it.
func TestFileSnapshotterDeletesDiscardedWritersFiles(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)

	_, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: 3, LastIncludedTerm: 1})
	assert.NilError(t, err)
	writer, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1})
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_5_1.snap"})
}

// Opening a FileSnapshotter removes the snapshot files that aren't current, left by writes that never committed or
// deletes that failed.
func TestFileSnapshotterRemovesLeftovers(t *testing.T) {
	dir := t.TempDir()
	for _, file := range []string{"log_0_0.dat", "snap_3_1.snap", "snap_5_1.snap", "snap_5_1.2.tmp", "snap_7_1.1.tmp"} {
		assert.NilError(t, os.WriteFile(path.Join(dir, file), nil, 0644))
	}
	testFileSnapshotter(t, dir, &sync.RWMutex{}, &pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1})
	assert.DeepEqual(t, fileNames(t, dir), []string{"log_0_0.dat", "snap_5_1.snap"})
}

// A current snapshot whose file is missing fails the open, before any leftover is removed.
func TestFileSnapshotterFailsWithoutTheCurrentFile(t *testing.T) {
	dir := t.TempDir()
	assert.NilError(t, os.WriteFile(path.Join(dir, "snap_5_1.2.tmp"), nil, 0644))
	current := &pb.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1}
	_, err := OpenFileSnapshotter(dir, &sync.RWMutex{}, func() (*pb.SnapshotMetadata, error) { return current, nil },
		func(*pb.SnapshotMetadata) error { return nil }, nil)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_5_1.2.tmp"})
}

// testFileSnapshotter creates a FileSnapshotter in dir that keeps the current snapshot's metadata in memory, starting
// from current.
func testFileSnapshotter(t *testing.T, dir string, lock *sync.RWMutex, current *pb.SnapshotMetadata) *FileSnapshotter {
	t.Helper()
	files, err := OpenFileSnapshotter(dir, lock, func() (*pb.SnapshotMetadata, error) { return current, nil },
		func(metadata *pb.SnapshotMetadata) error {
			current = metadata
			return nil
		}, nil)
	assert.NilError(t, err)
	return files
}

func fileNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	assert.NilError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// A commit deletes the snapshot it replaces even while that one is being read, which keeps its data.
func TestFileSnapshotterDeletesSnapshotsBeingRead(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)
	commit := func(index int64, data string) {
		t.Helper()
		writer, err := files.Create(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
		assert.NilError(t, err)
		_, err = writer.WriteAt([]byte(data), 0)
		assert.NilError(t, err)
		assert.NilError(t, writer.Commit())
	}

	commit(1, "one")
	snapshot, err := files.Open(&pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1})
	assert.NilError(t, err)
	defer snapshot.Close()

	commit(2, "two")
	assert.DeepEqual(t, fileNames(t, dir), []string{"snap_2_1.snap"})
	data, err := snapshot.ReadAll()
	assert.NilError(t, err)
	assert.Equal(t, string(data), "one")
}

// Snapshot files are created like segments: with os.Create's mode, under the umask.
func TestSnapshotFilesAreCreatedLikeSegments(t *testing.T) {
	dir := t.TempDir()
	files := testFileSnapshotter(t, dir, &sync.RWMutex{}, nil)
	metadata := &pb.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}
	writer, err := files.Create(metadata)
	assert.NilError(t, err)
	assert.NilError(t, writer.Commit())

	ref, err := os.Create(path.Join(dir, "ref"))
	assert.NilError(t, err)
	assert.NilError(t, ref.Close())
	want, err := os.Stat(ref.Name())
	assert.NilError(t, err)
	got, err := os.Stat(path.Join(dir, snapshotFilename(metadata)))
	assert.NilError(t, err)
	assert.Equal(t, got.Mode().Perm(), want.Mode().Perm())
}
