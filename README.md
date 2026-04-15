# Graft

A complete, extensible Raft implementation in Go with support for snapshotting (log compaction),
configuration updates, and batched command execution.

Built on top of the core Raft library, this repo includes three working distributed applications:

- **kvstore** — distributed key-value database with linearizable writes and optionally linearizable reads
  (non-linearizable reads are faster but may return stale data)
- **dlock** — distributed read-write lock with fair and non-fair semantics
- **msgq** — distributed multi-topic message queue

## Requirements

- Go 1.26+

## Building

```sh
go build ./...
```

## Testing

```sh
go test ./...
```

To run benchmarks:

```sh
go test ./benchmarks/... -bench=.
```

## Project structure

```
graft/       Core Raft consensus implementation
infra/       Shared server/client infrastructure
kvstore/     Distributed key-value store
dlock/       Distributed read-write lock
msgq/        Distributed message queue
badger/      Persistence layer (BadgerDB-backed)
benchmarks/  Performance benchmarks
testutil/    Shared test utilities
```

## License

MIT — see [LICENSE](LICENSE).
