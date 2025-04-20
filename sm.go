package graft

import (
	"io"

	"github.com/mizosoft/graft/pb"
)

type StateMachine interface {
	Apply(entries []pb.LogEntry)

	SnapshotMetadata() pb.SnapshotMetadata

	Snapshot(persistence Persistence) (Snapshot, error)

	RestoreSnapshot(metadata pb.SnapshotMetadata, reader io.Reader) error
}

type Snapshot interface {
	Metadata() pb.SnapshotMetadata

	WriteTo(io.Writer) error

	Close() error
}
