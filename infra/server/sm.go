package server

type StateMachine interface {
	Apply(command *Command) any

	Restore(snapshot []byte)

	ShouldSnapshot() bool

	Snapshot() []byte
}
