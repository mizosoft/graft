package server

import "github.com/mizosoft/graft"

type StateMachine interface {
	Apply(command Command) any

	Restore(snapshot graft.Snapshot) error

	ShouldSnapshot() bool

	Snapshot() []byte
}
