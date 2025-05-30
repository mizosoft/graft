# Graft

A complete, extensible RAFT implementation with support for snapshotting (log compaction), configuration updates &
batched command execution.

The RAFT implementation is used to implement some interesting applications: 
 - A distributed key-value database with linearizable writes & optionally linearizable reads. Non-linearizable reads
   are significantly faster but might get stale data.
 - A distributed read-write lock with fair & non-fair semantics (my favourite).
 - A distributed multi-topic message queue.
