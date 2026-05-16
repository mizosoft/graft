package server

import (
	"io"

	"github.com/mizosoft/graft"
)

type StateMachine[C any] interface {
	Apply(command Command[C]) any

	Restore(snapshot graft.Snapshot) error

	ShouldSnapshot() bool

	Snapshot(writer io.Writer) error
}
