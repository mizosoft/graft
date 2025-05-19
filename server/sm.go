package server

type StateMachine interface {
	Apply(command any) any

	Restore(snapshot []byte)

	MaybeSnapshot() []byte
}
