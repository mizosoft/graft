package graft

import (
	"errors"
	"testing"

	"github.com/mizosoft/graft/pb"
	"gotest.tools/v3/assert"
)

func commitSnapshot(p Persistence, index int64, data string) error {
	writer, err := p.CreateSnapshot(&pb.SnapshotMetadata{LastIncludedIndex: index, LastIncludedTerm: 1})
	if err != nil {
		return err
	}
	if _, err := writer.WriteAt([]byte(data), 0); err != nil {
		return errors.Join(err, writer.Close())
	}
	return writer.Commit()
}

func assertCurrentSnapshot(t *testing.T, p Persistence, index int64, data string) {
	t.Helper()

	metadata, err := p.LastSnapshotMetadata()
	assert.NilError(t, err)
	assert.Assert(t, metadata != nil)
	assert.Equal(t, metadata.LastIncludedIndex, index)

	snapshot, err := p.OpenSnapshot(metadata)
	assert.NilError(t, err, "the current snapshot's data is gone")
	defer snapshot.Close()
	got, err := snapshot.ReadAll()
	assert.NilError(t, err)
	assert.Equal(t, string(got), data)
}
