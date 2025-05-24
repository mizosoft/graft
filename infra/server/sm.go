package server

type StateMachine interface {
	Apply(command *Command) any

	Restore(snapshot []byte)

	MaybeSnapshot() []byte
}
