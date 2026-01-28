package graft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// TODO recheck the overuse of panics.

const UnknownLeader = "UNKNOWN"

type state int

const (
	stateNew state = iota
	stateStarting
	stateFollower
	stateCandidate
	stateLeader
	stateDead
)

var (
	ErrNotLeader    = errors.New("not leader")
	ErrDead         = errors.New("dead")
	ErrInitialized  = errors.New("already initialized")
	ErrNotSupported = errors.New("not supported")
	ErrIdConflict   = errors.New("ID conflict")
	ErrNoSuchId     = errors.New("no such ID")
)

func (s state) String() string {
	return [...]string{
		stateNew:       "New",
		stateStarting:  "Starting",
		stateFollower:  "Follower",
		stateCandidate: "Candidate",
		stateLeader:    "Leader",
		stateDead:      "Dead",
	}[s]
}

// TODO make this as default, allow users to override.
const defaultEntryBatchSize = 4096

const defaultSnapshotBufferSize = 1 * 1024 * 1024

// TODO limit broadcastWorker & make each worker send requests to any peer.

type Callbacks struct {
	Restore        func(snapshot Snapshot) error
	Apply          func(entry *pb.LogEntry) error
	ShouldSnapshot func() bool
	Snapshot       func(writer io.Writer) error
	Closed         func() error
}

func (c Callbacks) Copy() Callbacks {
	return Callbacks{
		Restore:        c.Restore,
		Apply:          c.Apply,
		ShouldSnapshot: c.ShouldSnapshot,
		Snapshot:       c.Snapshot,
		Closed:         c.Closed,
	}
}

func (c Callbacks) WithDefaults() Callbacks {
	c2 := c.Copy()
	if c2.Restore == nil {
		c2.Restore = func(snapshot Snapshot) error {
			return nil
		}
	}
	if c2.Apply == nil {
		c2.Apply = func(entry *pb.LogEntry) error {
			return nil
		}
	}
	if c2.ShouldSnapshot == nil {
		c2.ShouldSnapshot = func() bool {
			return false
		}
	}
	if c2.Snapshot == nil {
		c2.Snapshot = func(writer io.Writer) error {
			return ErrNotSupported
		}
	}
	if c2.Closed == nil {
		c2.Closed = func() error {
			return nil
		}
	}
	return c2
}

type entryIndexRange struct {
	from, to int64
}

type raftState struct {
	state       state
	currentTerm int64
	votedFor    string
	commitIndex int64
	lastApplied int64
}

type Graft struct {
	_ uncopyable
	io.Closer

	raftState

	id                string
	url               string
	leaderId          string
	peers             map[string]*peer
	electionTimer     *eventTimer[int64]
	heartbeatTimer    *eventTimer[struct{}]
	grpcServer        *grpc.Server
	mut               sync.Mutex
	applyChan         chan any
	deadChan          chan struct{}
	logCompactionChan chan int64
	broadcastChans    map[string]*broadcastChannel

	// The currently active config update. Might not have been committed yet.
	lastConfigUpdate *pb.ConfigUpdate

	// Index of lastConfigUpdate in the log. -1 if no update has been applied, or a synthetic one is applied with startingConfigUpdate.
	lastConfigUpdateIndex int64

	// The last config update committed to the raft log.
	lastCommittedConfigUpdate *pb.ConfigUpdate

	// The config update to initialize this node with. It is not appended to the log.
	startingConfigUpdate *pb.ConfigUpdate

	// Whether the current node is a leader and is not part of the new configuration and should leave when the configuration is committed.
	leaving bool

	lastHeartbeatTime  time.Time
	minElectionTimeout time.Duration
	rpcTimeouts        RpcTimeouts
	logger             *zap.SugaredLogger

	rpcCtx       context.Context
	rpcCtxCancel context.CancelFunc

	persistence Persistence

	// Callbacks for state machine integration.
	callbacks Callbacks
}

func (g *Graft) Id() string {
	return g.id
}

func (g *Graft) Url() string {
	return g.url
}

func (g *Graft) Persistence() Persistence {
	return g.persistence
}

func (g *Graft) String() string {
	return fmt.Sprintf(
		"{state: %v, currentTerm: %d, votedFor: %s, commitIndex: %d, lastApplied: %d, leaderId: %s, len(log)=%d}",
		g.state, g.currentTerm, g.votedFor, g.commitIndex, g.lastApplied, g.leaderId, g.persistence.EntryCount())
}

func New(config Config) (*Graft, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Create cancellable context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background())

	g := &Graft{
		id:  config.Id,
		url: config.ClusterUrls[config.Id],
		raftState: raftState{
			state:       stateNew,
			currentTerm: -1,
			votedFor:    UnknownLeader,
			commitIndex: -1,
			lastApplied: -1,
		},
		leaderId:              UnknownLeader,
		lastConfigUpdateIndex: -1,
		minElectionTimeout:    time.Duration(config.ElectionTimeoutMillis.Low) * time.Millisecond,
		rpcTimeouts:           config.RpcTimeoutsWithDefaults(),
		applyChan:             make(chan any, 1024), // Buffer the channel so that a slow client doesn't immediately block workflow.
		deadChan:              make(chan struct{}),
		logCompactionChan:     make(chan int64, 16*1024), // Buffer to avoid blocking snapshot operations.
		broadcastChans:        make(map[string]*broadcastChannel),
		peers:                 make(map[string]*peer),
		lastHeartbeatTime:     time.Now().Add(-time.Duration(config.ElectionTimeoutMillis.Low) * time.Millisecond), // Start with an expiring heartbeat.
		startingConfigUpdate: &pb.ConfigUpdate{
			Phase: pb.ConfigUpdate_APPLIED,
		},
		logger:       config.LoggerOrNoop().With(zap.String("name", "Graft"), zap.String("id", config.Id)).Sugar(),
		rpcCtx:       ctx,
		rpcCtxCancel: cancel,
		persistence:  config.Persistence,
		grpcServer:   grpc.NewServer(),
		callbacks:    Callbacks{}.WithDefaults(),
	}

	g.electionTimer = newRandomizedTimer[int64](
		time.Duration(config.ElectionTimeoutMillis.Low)*time.Millisecond,
		time.Duration(config.ElectionTimeoutMillis.High)*time.Millisecond,
		g.GetCurrentTerm)
	g.heartbeatTimer = newTimer[struct{}](
		time.Duration(config.HeartbeatMillis)*time.Millisecond,
		func() struct{} {
			return struct{}{}
		})

	for id, addr := range config.ClusterUrls {
		g.startingConfigUpdate.New = append(g.startingConfigUpdate.New, &pb.NodeConfig{
			Id:  id,
			Url: addr,
		})
	}

	return g, nil
}

func (g *Graft) GetCurrentTerm() int64 {
	g.mut.Lock()
	defer g.mut.Unlock()
	return g.currentTerm
}

func (g *Graft) fatal(err error) {
	g.logger.Panic(err)
}

func (g *Graft) Start(callbacks Callbacks) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != stateNew {
		return
	}
	g.state = stateStarting

	go func() {
		if err := g.initialize(callbacks); err != nil {
			g.fatal(err)
		}

		listener, err := net.Listen("tcp", g.url)
		if err != nil {
			g.fatal(err)
		}

		g.logger.Infof("Listening on %v", listener.Addr())

		pb.RegisterRaftServer(g.grpcServer, &server{g: g})
		if err := g.grpcServer.Serve(listener); err != nil {
			g.fatal(err)
		}
	}()
}

func (g *Graft) initialize(callbacks Callbacks) error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state > stateStarting {
		if g.state == stateDead {
			return ErrDead
		} else {
			return ErrInitialized
		}
	}

	state, err := g.persistence.RetrieveState()
	if err != nil {
		return err
	}

	if state == nil {
		// Set initial state.
		state = &pb.PersistedState{
			CurrentTerm: 0,
			VotedFor:    UnknownLeader,
			CommitIndex: -1,
		}

		if err := g.persistence.SaveState(state); err != nil {
			return err
		}
	}

	g.unguardedTransitionToFollower(state.CurrentTerm)
	g.votedFor = state.VotedFor
	g.commitIndex = state.CommitIndex

	g.unguardedInitConfigUpdate(g.startingConfigUpdate, -1)

	// Publish snapshot to applyChan if there is one.
	snapshotMetadata, err := g.persistence.LastSnapshotMetadata()
	if err != nil {
		return err
	}
	if snapshotMetadata != nil {
		g.applyChan <- snapshotMetadata
	}

	firstIndex, err := g.persistence.FirstEntryIndex()
	if err != nil {
		return err
	}

	// Apply committed entries to the state machine.
	commitIndex := g.commitIndex
	if firstIndex >= 0 && commitIndex >= firstIndex {
		g.applyChan <- entryIndexRange{firstIndex, commitIndex}
	}

	g.callbacks = callbacks.WithDefaults()

	go g.applyWorker()
	go g.electionWorker()
	go g.heartbeatWorker()
	go g.logCompactionWorker()
	for _, peer := range g.peers {
		go g.broadcastWorker(peer, g.broadcastChans[peer.id])
	}

	return nil
}

func (g *Graft) Close() error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == stateDead {
		return nil
	}
	g.state = stateDead

	g.rpcCtxCancel()

	g.electionTimer.stop()
	g.heartbeatTimer.stop()

	close(g.applyChan)
	close(g.logCompactionChan)
	for _, ch := range g.broadcastChans {
		ch.close()
	}

	for _, peer := range g.peers {
		peer.closeConn()
	}

	g.grpcServer.Stop()

	g.logger.Info("Graft closed")
	return nil
}

func (g *Graft) unguardedCountMajority(condition func(*peer) bool) bool {
	switch g.lastConfigUpdate.Phase {
	case pb.ConfigUpdate_LEARNING:
		satisfiedCount := 1 // Count self.
		learnerCount := 0
		for _, p := range g.peers {
			if p.learner {
				learnerCount++
			} else if condition(p) {
				satisfiedCount++
			}
		}

		if 2*satisfiedCount > len(g.peers)+1-learnerCount {
			return true
		}
	case pb.ConfigUpdate_JOINT:
		// Make sure we satisfy the two majorities.

		count1 := len(g.lastConfigUpdate.Old)
		count2 := len(g.lastConfigUpdate.New)

		satisfiedCount1 := 1 // Count self.
		satisfiedCount2 := 0
		if in(g.id, g.lastConfigUpdate.New) {
			satisfiedCount2++
		}

		for _, p := range g.peers {
			if condition(p) {
				if in(p.id, g.lastConfigUpdate.Old) {
					satisfiedCount1++
				}
				if in(p.id, g.lastConfigUpdate.New) && condition(p) {
					satisfiedCount2++
				}

				if 2*satisfiedCount1 > count1 && 2*satisfiedCount2 > count2 {
					return true
				}
			}
		}
	case pb.ConfigUpdate_APPLIED:
		count := len(g.peers)
		satisfiedCount := 0
		if in(g.id, g.lastConfigUpdate.New) {
			count++
			satisfiedCount++
		}
		for _, p := range g.peers {
			if condition(p) {
				satisfiedCount++
				if 2*satisfiedCount > count {
					return true
				}
			}
		}
	}
	return false
}

func (g *Graft) applyWorker() {
	for e := range g.applyChan {
		switch e := e.(type) {
		case entryIndexRange:
			var lastAppliedEntry *pb.LogEntry
			for i := e.from; i <= e.to; i += defaultEntryBatchSize {
				entries, err := g.persistence.GetEntries(i, min(e.to, i+defaultEntryBatchSize))
				if err != nil {
					var indexOutOfRangeError IndexOutOfRangeError
					if errors.As(err, &indexOutOfRangeError) {
						// A concurrent truncation (due to snapshotting) could've occurred. We can proceed towards getting the next
						// snapshot.
						break
					}
					panic(err)
				}

				for _, entry := range entries {
					if entry.Type == pb.LogEntry_NOOP {
						continue
					}

					if err := g.callbacks.Apply(entry); err != nil {
						panic(err)
					}
					lastAppliedEntry = entry
				}
			}

			if lastAppliedEntry != nil {
				g.logger.Infof("Applied entries up to %d", lastAppliedEntry.Index)

				g.mut.Lock()
				g.lastApplied = lastAppliedEntry.Index
				g.mut.Unlock()

				if g.callbacks.ShouldSnapshot() {
					metadata := &pb.SnapshotMetadata{
						LastIncludedIndex: lastAppliedEntry.Index,
						LastIncludedTerm:  lastAppliedEntry.Term,
					}
					g.writeSnapshot(metadata)
					g.logger.Infof("Snapshotted: %v", metadata)
				}
			}
		case *pb.SnapshotMetadata:
			if g.restoreSnapshot(e) {
				g.logger.Infof("Restored snapshot: %v", e)

				g.mut.Lock()
				g.lastApplied = e.LastIncludedIndex
				g.mut.Unlock()
			}
		default:
			g.logger.Panicf("Unknown apply entry type %T", e)
		}
	}
}

func (g *Graft) restoreSnapshot(metadata *pb.SnapshotMetadata) bool {
	snapshot, err := g.persistence.OpenSnapshot(metadata)
	if err != nil {
		if errors.Is(err, ErrNoSuchSnapshot) {
			// A concurrent snapshotting could've removed this snapshot. We can proceed towards getting the next
			// snapshot.
			return false
		}
		panic(err)
	}

	defer snapshot.Close()

	if err := g.callbacks.Restore(snapshot); err != nil {
		panic(err)
	}
	return true
}

func (g *Graft) writeSnapshot(metadata *pb.SnapshotMetadata) {
	writer, err := g.persistence.CreateSnapshot(metadata)
	if err != nil {
		panic(err)
	}
	defer writer.Close()

	if err := g.callbacks.Snapshot(&sequentialSnapshotWriter{base: writer}); err != nil {
		panic(err)
	}
	if err = writer.Commit(); err != nil {
		panic(err)
	}

	g.logCompactionChan <- metadata.LastIncludedIndex
}

type sequentialSnapshotWriter struct {
	base   SnapshotWriter
	offset int64
}

func (w *sequentialSnapshotWriter) Write(data []byte) (int, error) {
	if n, err := w.base.WriteAt(data, w.offset); err != nil {
		return 0, err
	} else {
		w.offset += int64(n)
	}
	return len(data), nil
}

func (g *Graft) electionWorker() {
	for timeoutTerm := range g.electionTimer.C {
		g.runElection(timeoutTerm)
	}
}

func (g *Graft) runElection(timeoutTerm int64) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if timeoutTerm != g.currentTerm {
		return // Outdated election.
	}

	switch g.state {
	case stateLeader, stateDead:
		// We might've already won an election for this term but couldn't stop the timer on time.
		return
	case stateCandidate, stateFollower:
		// Continue election.
	default:
		panic("Running elections when not yet initialized")
	}

	g.unguardedTransitionToCandidate()

	voteGranted := make(map[string]bool)
	electionTerm := timeoutTerm + 1
	for _, p := range g.peers {
		go func() {
			if client, cerr := p.client(); cerr != nil {
				g.logger.Error("Error connecting to peer", zap.String("peer", p.id), zap.Error(cerr))
			} else {
				request := func() *pb.RequestVoteRequest {
					g.mut.Lock()
					defer g.mut.Unlock()

					if g.currentTerm != electionTerm || g.state != stateCandidate {
						return nil
					}

					lastLogIndex, err := g.persistence.LastEntryIndex()
					if err != nil {
						panic(err)
					}

					var lastLogTerm int64 = -1
					if lastLogIndex >= 0 {
						lastLogTerm, err = g.persistence.GetEntryTerm(lastLogIndex)
						if err != nil {
							panic(err)
						}
					} else if metadata, err := g.persistence.LastSnapshotMetadata(); err == nil && metadata != nil {
						lastLogIndex, lastLogTerm = metadata.LastIncludedIndex, metadata.LastIncludedTerm
					} else if err != nil {
						panic(err)
					}

					return &pb.RequestVoteRequest{
						Term:         electionTerm,
						CandidateId:  g.id,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
				}()

				if request == nil {
					return
				}

				ctx, cancel := context.WithTimeout(g.rpcCtx, g.rpcTimeouts.RequestVote)
				defer cancel()

				if res, rerr := client.RequestVote(ctx, request); rerr != nil {
					g.logger.Error("RequestVote->", zap.String("peer", p.id), zap.Any("request", request), zap.Error(rerr))
				} else {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.logger.Info("RequestVote->", zap.String("peer", p.id), zap.Any("request", request), zap.Int64("election", electionTerm), zap.Any("response", res))

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
					} else if res.VoteGranted && g.currentTerm == electionTerm && g.state == stateCandidate { // Check that the election isn't invalidated.
						voteGranted[p.id] = true
						if g.unguardedCountMajority(func(p *peer) bool {
							_, ok := voteGranted[p.id]
							return ok
						}) {
							g.unguardedTransitionToLeader()
						}
					}
				}
			}
		}()
	}
}

func (g *Graft) heartbeatWorker() {
	for range g.heartbeatTimer.C {
		g.runHeartbeat()
	}
}

func (g *Graft) runHeartbeat() {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == stateLeader {
		g.heartbeatTimer.reset()
		for _, c := range g.broadcastChans {
			c.notify()
		}
	}
}

func (g *Graft) logCompactionWorker() {
	for index := range g.logCompactionChan {
		func() {
			g.mut.Lock()
			defer g.mut.Unlock()

			err := g.persistence.TruncateEntriesTo(index)
			if err != nil {
				g.logger.Error("Error compacting log", zap.Error(err))
			}
		}()
	}
}

func (g *Graft) createAppendEntriesOrSnapshotRequest(p *peer) proto.Message {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != stateLeader {
		return nil
	}

	/*
		Generally, we have this layout:

			|--------------------------+--------------------------+
			|         Snapshot         |       Log entries        |
			|--------------------------+--------------------------+
			^                          ^                          ^
			|                          |                          |
			+- 0 to LastIncludedIndex -+- firstNext to lastIndex -+

		We want to make sure that peer's nextIndex lies within 0 to max(lastIndex, snapshotMetadata.LastIncludedIndex) + 1.
		We also want to take into account cases where there are no snapshot and/or log entries.
	*/

	if p.nextIndex < 0 {
		g.logger.Panicf("Invalid peer.nextIndex: (%s).nextIndex=%d", p.id, p.nextIndex)
	}

	firstIndex, err := g.persistence.FirstEntryIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := g.persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	var snapshotMetadata *pb.SnapshotMetadata
	if firstIndex < 0 || p.nextIndex < firstIndex {
		// We might have a snapshot that covers nextIndex.
		snapshotMetadata, err = g.persistence.LastSnapshotMetadata()
		if err != nil {
			panic(err)
		}
		if snapshotMetadata != nil && p.nextIndex > snapshotMetadata.LastIncludedIndex+1 {
			g.logger.Panicf(
				"Invalid nextIndex: (%s).nextIndex=%d, firstIndex=%d, lastIndex=%d, snapshotMetadata={%v}",
				p.id, p.nextIndex, firstIndex, lastIndex, snapshotMetadata)
		}
	}

	// We have a valid snapshot to send.
	if snapshotMetadata != nil && p.nextIndex <= snapshotMetadata.LastIncludedIndex {
		return &pb.SnapshotRequest{
			Term:     g.currentTerm,
			LeaderId: g.id,
			Metadata: snapshotMetadata,
			// Data, Offset & Done are filled later.
		}
	}

	if p.nextIndex > lastIndex+1 {
		g.logger.Panicf("Invalid nextIndex: (%s).nextIndex=%d, lastIndex=%d", p.id, p.nextIndex, lastIndex)
	}

	request := &pb.AppendEntriesRequest{
		Term:              g.currentTerm,
		LeaderId:          g.id,
		LeaderCommitIndex: g.commitIndex,
	}

	prevLogIndex := p.nextIndex - 1
	if prevLogIndex < 0 {
		request.PrevLogIndex, request.PrevLogTerm = -1, -1
	} else if firstIndex >= 0 && prevLogIndex >= firstIndex && prevLogIndex <= lastIndex {
		prevLogTerm, err := g.persistence.GetEntryTerm(prevLogIndex)
		if err != nil {
			panic(err)
		}
		request.PrevLogIndex, request.PrevLogTerm = prevLogIndex, prevLogTerm
	} else if metadata, err := g.persistence.LastSnapshotMetadata(); err == nil &&
		metadata != nil && prevLogIndex == metadata.LastIncludedIndex {
		request.PrevLogIndex, request.PrevLogTerm = metadata.LastIncludedIndex, metadata.LastIncludedIndex
	} else if err != nil {
		panic(err)
	} else {
		g.logger.Panicf("Invalid nextIndex: (%s).prevLogIndex=%d, firstIndex=%d, lastIndex=%d, snapshotMetadata=%v",
			p.id, prevLogIndex, firstIndex, lastIndex, metadata)
	}

	if p.nextIndex <= lastIndex {
		es, err := g.persistence.GetEntries(p.nextIndex, min(p.nextIndex+defaultEntryBatchSize-1, lastIndex))
		if err != nil {
			panic(err)
		}
		request.Entries = es
	}

	return request
}

func (g *Graft) broadcastWorker(p *peer, c *broadcastChannel) {
	for range c.c {
		req := g.createAppendEntriesOrSnapshotRequest(p)

		if req == nil {
			continue
		}

		if client, err := p.client(); err != nil {
			g.logger.Error("Error connecting to peer", zap.String("id", p.id), zap.Error(err), zap.Stringer("state", g))
		} else {
			timeBeforeSending := time.Now()
			res := func() proto.Message {
				switch req := req.(type) {
				case *pb.AppendEntriesRequest:
					ctx, cancel := context.WithTimeout(g.rpcCtx, g.rpcTimeouts.AppendEntries)
					defer cancel()

					if res, err := client.AppendEntries(ctx, req); err != nil {
						g.logger.Error("AppendEntries->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
						return nil
					} else {
						return res
					}

				case *pb.SnapshotRequest:
					snapshot, err := g.persistence.OpenSnapshot(req.Metadata)
					if errors.Is(err, ErrNoSuchSnapshot) {
						// Snapshot is lost. Might be due to a concurrent snapshot that superseded this one. We'll try again with
						// updated state the next time the broadcaster is poked.
						return nil
					} else if err != nil {
						panic(err)
					}

					ctx, cancel := context.WithTimeout(g.rpcCtx, g.rpcTimeouts.InstallSnapshot)
					defer cancel()

					if stream, err := client.InstallSnapshot(ctx); err != nil {
						g.logger.Error("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
						return nil
					} else if snapshot.Metadata().Size <= defaultSnapshotBufferSize {
						// Send without streaming.
						data, err := snapshot.ReadAll()
						if err != nil {
							panic(err)
						}

						req.Data = data
						req.Offset = 0
						req.Done = true
						err = stream.Send(req)
						if err == nil || errors.Is(err, io.EOF) {
							if res, err := stream.CloseAndRecv(); err != nil {
								g.logger.Error("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
								return nil
							} else {
								return res
							}
						} else {
							g.logger.Error("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
							return nil
						}
					} else {
						reader := snapshot.Reader()
						buffer := make([]byte, defaultSnapshotBufferSize)
						req.Offset = 0
						for {
							n, err := reader.Read(buffer)
							if errors.Is(err, io.EOF) {
								req.Done = true
							} else if err != nil {
								// Don't forget to close the stream.
								panic(errors.Join(err, stream.CloseSend()))
							}

							req.Data = buffer[:n]
							err = stream.Send(req)
							if (req.Done && err == nil) || errors.Is(err, io.EOF) {
								if res, err := stream.CloseAndRecv(); err != nil {
									g.logger.Error("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
									return nil
								} else {
									return res
								}
							} else if err != nil {
								g.logger.Error("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
								return nil
							}

							req.Offset += int64(n)
						}
					}

				default:
					g.logger.Panicf("Unknown request type: %T", req)
					return nil // Unreachable
				}
			}()

			var doRetry bool
			switch res := res.(type) {
			case *pb.AppendEntriesResponse:
				doRetry = func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.logger.Info("AppendEntries->", zap.String("peer", p.id), zap.Any("request", req), zap.Any("response", res), zap.Stringer("state", g))

					if g.state != stateLeader {
						return false
					}

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
						return false
					} else if res.Success {
						p.lastHeartbeatTime = timeBeforeSending

						req := req.(*pb.AppendEntriesRequest)
						if len(req.Entries) > 0 {
							p.nextIndex = req.Entries[len(req.Entries)-1].Index + 1
							p.matchIndex = p.nextIndex - 1
						} else if req.PrevLogIndex >= 0 {
							p.nextIndex = req.PrevLogIndex + 1
							p.matchIndex = req.PrevLogIndex
						}

						lastIndex, err := g.persistence.LastEntryIndex()
						if err != nil {
							panic(err)
						}

						if p.learner && p.matchIndex >= lastIndex {
							p.learner = false
						}

						g.unguardedFlushCommittedEntries()
						g.unguardedContinueConfigUpdate()

						return p.matchIndex < lastIndex
					} else {
						p.lastHeartbeatTime = timeBeforeSending
						p.nextIndex--
						return true
					}
				}()

			case *pb.SnapshotResponse:
				doRetry = func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.logger.Info("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Any("response", res), zap.Stringer("state", g))

					if g.state != stateLeader {
						return false
					}

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
						return false
					} else {
						p.lastHeartbeatTime = timeBeforeSending

						req := req.(*pb.SnapshotRequest)
						p.nextIndex = req.Metadata.LastIncludedIndex + 1
						p.matchIndex = req.Metadata.LastIncludedIndex
						return true // Continue emitting log.
					}
				}()
			}

			if doRetry {
				c.notify()
			}
		}
	}
}

func (g *Graft) unguardedTransitionToLeader() {
	g.logger.Info("Transitioning to Leader", zap.Stringer("state", g))

	g.state = stateLeader
	g.leaderId = g.id

	lastIndex, err := g.persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	for _, peer := range g.peers {
		peer.nextIndex = lastIndex + 1 // Assumes lastIndex = -1 if the log is empty.
		peer.matchIndex = -1
	}

	// Append NOOP entry if we have uncommitted entries from previous terms.
	if g.commitIndex < lastIndex {
		_, err := g.persistence.Append(g.unguardedCapturePersistedState(), []*pb.LogEntry{
			{
				Term: g.currentTerm,
				Type: pb.LogEntry_NOOP,
			},
		})
		if err != nil {
			panic(err)
		}
	}

	g.electionTimer.pause()
	g.heartbeatTimer.poke() // Establish authority.
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.logger.Info("Transitioning to Candidate", zap.Stringer("state", g))

	g.state = stateCandidate
	g.currentTerm++
	g.votedFor = g.id // Vote for self.
	g.leaderId = UnknownLeader
	g.electionTimer.reset()
	g.heartbeatTimer.pause()
}

func (g *Graft) unguardedTransitionToFollower(term int64) {
	g.logger.Info("Transitioning to Follower", zap.Stringer("state", g))

	g.state = stateFollower
	g.currentTerm = term
	g.votedFor = UnknownLeader
	g.electionTimer.reset()
	g.heartbeatTimer.pause() // If we were a leader.
}

func (g *Graft) unguardedFlushCommittedEntries() {
	lastIndex, err := g.persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	if lastIndex < 0 {
		return // Log is empty.
	}

	if g.commitIndex >= lastIndex {
		return // All entries in the log are committed.
	}

	newCommitIndex := g.commitIndex
	for i := g.commitIndex + 1; i <= lastIndex; i++ {
		// We must only commit entries from current term. See paper section 5.4.2.
		term, err := g.persistence.GetEntryTerm(i)
		if err != nil {
			panic(err)
		}

		if term == g.currentTerm {
			if g.unguardedCountMajority(func(p *peer) bool {
				return p.matchIndex >= i
			}) {
				newCommitIndex = i
			} else {
				break
			}
		}
	}

	if newCommitIndex > g.commitIndex {
		oldCommitIndex := g.commitIndex
		g.unguardedUpdateCommitIndex(newCommitIndex)
		g.applyChan <- entryIndexRange{oldCommitIndex + 1, newCommitIndex}
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
	}
}

func (g *Graft) unguardedUpdateCommitIndex(newCommitIndex int64) {
	if g.lastConfigUpdateIndex >= 0 && newCommitIndex >= g.lastConfigUpdateIndex {
		g.lastCommittedConfigUpdate = g.lastConfigUpdate
	}
	g.commitIndex = newCommitIndex
}

func (g *Graft) unguardedContinueConfigUpdate() {
	if g.lastConfigUpdateIndex < 0 || g.commitIndex < g.lastConfigUpdateIndex {
		return
	}

	switch g.lastConfigUpdate.Phase {
	case pb.ConfigUpdate_LEARNING:
		hasLearners := false
		for _, p := range g.peers {
			if p.learner {
				hasLearners = true
				break
			}
		}

		if !hasLearners {
			update := cloneMsg(g.lastConfigUpdate)
			update.Phase = pb.ConfigUpdate_JOINT
			if _, err := g.unguardedAppendConfigUpdate(update); err != nil {
				panic(err)
			}
		}
	case pb.ConfigUpdate_JOINT:
		update := cloneMsg(g.lastConfigUpdate)
		update.Phase = pb.ConfigUpdate_APPLIED
		if _, err := g.unguardedAppendConfigUpdate(update); err != nil {
			panic(err)
		}
	case pb.ConfigUpdate_APPLIED:
		if g.leaving {
			g.leaving = false
			g.unguardedTransitionToFollower(g.currentTerm) // Step down.
		}
	}
}

func (g *Graft) unguardedCapturePersistedState() *pb.PersistedState {
	return &pb.PersistedState{
		CurrentTerm: g.currentTerm,
		VotedFor:    g.votedFor,
		CommitIndex: g.commitIndex,
	}
}

func (g *Graft) unguardedProbablyHasLeader() bool {
	switch g.state {
	case stateFollower:
		return time.Now().Sub(g.lastHeartbeatTime) < g.minElectionTimeout
	case stateLeader:
		now := time.Now()
		return g.unguardedCountMajority(func(p *peer) bool {
			return now.Sub(p.lastHeartbeatTime) < g.minElectionTimeout
		})
	default:
		return false
	}
}

func (g *Graft) unguardedIsUnknownPeer(peerId string) bool {
	if g.lastCommittedConfigUpdate == nil {
		return false // We can't know otherwise.
	}

	switch g.lastCommittedConfigUpdate.Phase {
	case pb.ConfigUpdate_LEARNING:
		return !in(peerId, g.lastCommittedConfigUpdate.Old)
	case pb.ConfigUpdate_JOINT:
		return !in(peerId, g.lastCommittedConfigUpdate.Old) && !in(peerId, g.lastCommittedConfigUpdate.New)
	case pb.ConfigUpdate_APPLIED:
		return !in(peerId, g.lastCommittedConfigUpdate.New)
	default:
		g.logger.Panicf("Unknown phase: %v", g.lastCommittedConfigUpdate.Phase)
		return false // Unreachable.
	}
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	myLastLogIndex, err := g.persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	var myLastLogTerm int64 = -1
	if myLastLogIndex >= 0 {
		myLastLogTerm, err = g.persistence.GetEntryTerm(myLastLogIndex)
		if err != nil {
			panic(err)
		}
	} else if metadata, err := g.persistence.LastSnapshotMetadata(); err == nil && metadata != nil {
		myLastLogIndex, myLastLogTerm = metadata.LastIncludedIndex, metadata.LastIncludedTerm
	} else if err != nil {
		panic(err)
	}

	// Note that this comparison works with the -1 sentinel values.
	return myLastLogTerm < request.LastLogTerm ||
		(myLastLogTerm == request.LastLogTerm && myLastLogIndex <= request.LastLogIndex)
}

func (g *Graft) requestVote(request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.logger.Info("->RequestVote", zap.Any("request", request), zap.Stringer("state", g))

	if g.state == stateDead {
		return nil, ErrDead
	}

	if request.Term < g.currentTerm ||
		g.unguardedIsUnknownPeer(request.CandidateId) ||
		g.unguardedProbablyHasLeader() {
		return &pb.RequestVoteResponse{
			Term:        g.currentTerm,
			VoteGranted: false,
		}, nil
	}

	electionTimerIsReset := false
	if request.Term > g.currentTerm {
		g.unguardedTransitionToFollower(request.Term)
		electionTimerIsReset = true
	}

	grantVote := (g.votedFor == UnknownLeader || g.votedFor == request.CandidateId) && g.unguardedIsCandidateLogUpToDate(request)
	if grantVote {
		g.votedFor = request.CandidateId
		if !electionTimerIsReset {
			g.electionTimer.reset()
		}
	}

	if grantVote {
		g.lastHeartbeatTime = time.Now()
	}

	if err := g.persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
		panic(err)
	}
	return &pb.RequestVoteResponse{
		Term:        g.currentTerm,
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) appendEntries(request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.logger.Info("->AppendEntries", zap.Any("request", request), zap.Stringer("state", g))

	if g.state == stateDead {
		return nil, ErrDead
	}

	if request.Term < g.currentTerm || g.unguardedIsUnknownPeer(request.LeaderId) {
		return &pb.AppendEntriesResponse{
			Term:    g.currentTerm,
			Success: false,
		}, nil
	}

	g.lastHeartbeatTime = time.Now()

	if request.Term > g.currentTerm {
		g.unguardedTransitionToFollower(request.Term)
	} else {
		g.electionTimer.reset()
	}

	if g.leaderId != request.LeaderId {
		g.logger.Info("Changed leadership", zap.String("leaderId", request.LeaderId))
		g.leaderId = request.LeaderId
	}

	lastIndex, err := g.persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	if lastIndex >= 0 && request.PrevLogIndex > lastIndex {
		if err := g.persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
			panic(err)
		}
		return &pb.AppendEntriesResponse{
			Term:    request.Term,
			Success: false,
		}, nil
	}

	firstIndex, err := g.persistence.FirstEntryIndex()
	if err != nil {
		panic(err)
	}

	if firstIndex >= 0 && request.PrevLogIndex >= firstIndex {
		myPrevLogTerm, err := g.persistence.GetEntryTerm(request.PrevLogIndex)
		if err != nil {
			panic(err)
		}

		if request.PrevLogTerm != myPrevLogTerm {
			if err := g.persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
				panic(err)
			}
			return &pb.AppendEntriesResponse{
				Term:    request.Term,
				Success: false,
			}, nil
		}
	}

	if request.PrevLogIndex >= 0 && request.PrevLogIndex+1 <= lastIndex {
		if err := g.persistence.TruncateEntriesFrom(request.PrevLogIndex + 1); err != nil {
			panic(err)
		}
	}

	if len(request.Entries) > 0 {
		_, err = g.persistence.Append(g.unguardedCapturePersistedState(), request.Entries)
		if err != nil {
			panic(err)
		}
	}

	if request.LeaderCommitIndex > g.commitIndex {
		lastIndex, err := g.persistence.LastEntryIndex()
		if err != nil {
			panic(err)
		}

		newCommitIndex := min(request.LeaderCommitIndex, lastIndex)
		if newCommitIndex > g.commitIndex {
			oldCommitIndex := g.commitIndex
			g.unguardedUpdateCommitIndex(newCommitIndex)
			g.applyChan <- entryIndexRange{oldCommitIndex + 1, newCommitIndex}
		}
	}

	// Apply config entries if any. Each node in RAFT uses the last entry in its log, regardless of whether it was committed.
	for _, entry := range request.Entries {
		if entry.Type == pb.LogEntry_CONFIG {
			var update pb.ConfigUpdate
			protoUnmarshal(entry.Data, &update)
			g.unguardedInitConfigUpdate(&update, entry.Index)
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    request.Term,
		Success: true,
	}, nil
}

func (g *Graft) installSnapshot(stream grpc.ClientStreamingServer[pb.SnapshotRequest, pb.SnapshotResponse]) error {
	// Do not block for the entire streaming duration.
	var writer SnapshotWriter
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()

	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&pb.SnapshotResponse{Term: g.GetCurrentTerm()})
		} else if err != nil {
			return err
		}

		success, err := g.installSnapshotBlock(request, writer)
		if err != nil {
			return err
		} else if !success {
			return stream.SendAndClose(&pb.SnapshotResponse{Term: g.GetCurrentTerm()})
		}
	}
}

func (g *Graft) installSnapshotBlock(request *pb.SnapshotRequest, writer SnapshotWriter) (bool, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.logger.Info("->InstallSnapshot", zap.Any("request", request), zap.Stringer("state", g))

	if g.state == stateDead {
		return false, ErrDead
	}

	if request.Term < g.currentTerm || g.unguardedIsUnknownPeer(request.LeaderId) {
		return false, nil
	}

	g.lastHeartbeatTime = time.Now()

	if request.Term > g.currentTerm {
		g.unguardedTransitionToFollower(request.Term)
	} else {
		g.electionTimer.reset()
	}

	if g.leaderId != request.LeaderId {
		g.logger.Info("Changed leadership", zap.String("leaderId", request.LeaderId))
		g.leaderId = request.LeaderId
	}

	_, err := writer.WriteAt(request.Data, request.Offset)
	if err != nil {
		panic(err)
	}

	if request.Done {
		if err := writer.Commit(); err != nil {
			panic(err)
		}

		firstIndex, err := g.persistence.FirstEntryIndex()
		if err != nil {
			panic(err)
		}
		if firstIndex >= 0 && request.Metadata.LastIncludedIndex >= firstIndex {
			lastIndex, err := g.persistence.LastEntryIndex()
			if err != nil {
				panic(err)
			}
			g.logCompactionChan <- min(request.Metadata.LastIncludedIndex, lastIndex)
		} // Otherwise, snapshot is older than log or log is empty - no truncation needed.

		// Apply snapshot configuration.
		if request.Metadata.ConfigUpdate != nil {
			g.unguardedInitConfigUpdate(request.Metadata.ConfigUpdate, request.Metadata.LastIncludedIndex)
		}

		// Publish snapshot to state-machine.
		g.applyChan <- writer.Metadata()
	}
	return true, nil
}

type peerFactory struct {
	existingPeers        map[string]*peer
	lastLogIndex         int64
	defaultLastHeartbeat time.Time
}

func (f *peerFactory) get(config *pb.NodeConfig) *peer {
	p, ok := f.existingPeers[config.Id]
	if !ok {
		p = &peer{
			id:                config.Id,
			url:               config.Url,
			nextIndex:         f.lastLogIndex + 1,
			matchIndex:        -1,
			learner:           false,
			lastHeartbeatTime: f.defaultLastHeartbeat,
		}
		f.existingPeers[config.Id] = p
	}
	return p
}

func (g *Graft) unguardedAppendConfigUpdate(update *pb.ConfigUpdate) (int64, error) {
	nextIndex, err := g.persistence.Append(g.unguardedCapturePersistedState(), []*pb.LogEntry{{
		Term: g.currentTerm,
		Type: pb.LogEntry_CONFIG,
		Data: protoMarshal(update),
	}})
	if err != nil {
		return -1, err
	}
	g.unguardedInitConfigUpdate(update, nextIndex-1)
	g.heartbeatTimer.poke() // Broadcast update.
	return nextIndex, nil
}

func (g *Graft) unguardedInitConfigUpdate(update *pb.ConfigUpdate, updateIndex int64) {
	g.logger.Info("Applying ConfigUpdate", zap.Any("update", update), zap.Stringer("state", g))

	g.lastConfigUpdate = update
	g.lastConfigUpdateIndex = updateIndex

	// If the leader is not in the new configuration, make it step down when the configuration is committed.
	if g.state == stateLeader && update.Phase == pb.ConfigUpdate_APPLIED && !in(g.id, update.New) {
		g.leaving = true
	}

	lastIndex, err := g.persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	newPeers := make(map[string]*peer)
	factory := &peerFactory{
		existingPeers:        g.peers,
		lastLogIndex:         lastIndex,
		defaultLastHeartbeat: time.Now().Add(-g.minElectionTimeout),
	}

	switch update.Phase {
	case pb.ConfigUpdate_LEARNING:
		for _, config := range update.Old {
			newPeers[config.Id] = factory.get(config)
		}

		for _, config := range update.New {
			if !in(config.Id, update.Old) {
				p := factory.get(config)
				p.learner = true
				newPeers[config.Id] = p
			}
		}
	case pb.ConfigUpdate_JOINT:
		for _, config := range update.Old {
			newPeers[config.Id] = factory.get(config)
		}

		for _, config := range update.New {
			if !in(config.Id, update.Old) {
				newPeers[config.Id] = factory.get(config)
			}
		}
	case pb.ConfigUpdate_APPLIED:
		for _, config := range update.New {
			newPeers[config.Id] = factory.get(config)
		}
	}

	delete(newPeers, g.id) // Remove self.

	newBroadcastChans := make(map[string]*broadcastChannel)
	for _, p := range newPeers {
		if ch, ok := g.broadcastChans[p.id]; ok {
			newBroadcastChans[p.id] = ch
			delete(g.broadcastChans, p.id)
		} else {
			newBroadcastChans[p.id] = newBroadcastChannel()
			if g.state > stateStarting { // Only launch workers if already initialized.
				go g.broadcastWorker(p, newBroadcastChans[p.id])
			}
		}
	}

	// Close remaining unused broadcast channels.
	for _, ch := range g.broadcastChans {
		ch.close()
	}

	for _, p := range newPeers {
		delete(factory.existingPeers, p.id)
	}

	// Close remaining unused peers.
	for _, p := range factory.existingPeers {
		p.closeConn()
	}

	g.peers = newPeers
	g.broadcastChans = newBroadcastChans
}

func (g *Graft) Append(commands [][]byte) (int64, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == stateDead {
		return -1, ErrDead
	}

	if g.state != stateLeader {
		return -1, ErrNotLeader
	}

	if len(commands) == 0 {
		return -1, nil
	}

	entries := make([]*pb.LogEntry, len(commands))
	for i, cmd := range commands {
		entries[i] = &pb.LogEntry{
			Term: g.currentTerm,
			Data: cmd,
			Type: pb.LogEntry_COMMAND,
		}
	}

	nextIndex, err := g.persistence.Append(g.unguardedCapturePersistedState(), entries)
	if err != nil {
		return -1, err
	}
	g.heartbeatTimer.poke() // Broadcast new entries.
	return nextIndex - int64(len(entries)), nil
}

func (g *Graft) UpdateCluster(updateId string, addedNodes map[string]string, removedNodes []string) (int64, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == stateDead {
		return -1, ErrDead
	}

	if g.state != stateLeader {
		return -1, ErrNotLeader
	}

	existingNodes := make(map[string]string)
	for _, peer := range g.peers {
		existingNodes[peer.id] = peer.url
	}
	existingNodes[g.id] = g.url

	// Perform some clean-up: make sure existingNodes & addedNodes are disjoint, and removedNodes is a subset of
	// existingNodes and is disjoint with addedNodes.

	for id, url := range existingNodes {
		if addedUrl, ok := addedNodes[id]; ok {
			if url == addedUrl {
				delete(existingNodes, id)
			} else {
				return -1, ErrIdConflict
			}
		}
	}

	for _, id := range removedNodes {
		if _, ok := existingNodes[id]; !ok {
			return -1, ErrNoSuchId
		}
	}

	for _, id := range removedNodes {
		delete(addedNodes, id)
	}

	// Create config update.

	update := &pb.ConfigUpdate{
		Id:  updateId,
		Old: g.lastConfigUpdate.New,
	}

	for id, url := range existingNodes {
		if !slices.Contains(removedNodes, id) {
			update.New = append(update.New, &pb.NodeConfig{
				Id:  id,
				Url: url,
			})
		}
	}

	for id, url := range addedNodes {
		update.New = append(update.New, &pb.NodeConfig{
			Id:  id,
			Url: url,
		})
	}

	if len(addedNodes) > 0 {
		update.Phase = pb.ConfigUpdate_LEARNING
	} else {
		update.Phase = pb.ConfigUpdate_JOINT
	}
	return g.unguardedAppendConfigUpdate(update)
}

func in(id string, configs []*pb.NodeConfig) bool {
	for _, config := range configs {
		if config.Id == id {
			return true
		}
	}
	return false
}

func (g *Graft) LeaderId() string {
	g.mut.Lock()
	defer g.mut.Unlock()
	return g.leaderId
}

func (g *Graft) Config() map[string]string {
	g.mut.Lock()
	defer g.mut.Unlock()

	config := make(map[string]string)
	for _, nodeConfig := range g.lastConfigUpdate.New {
		config[nodeConfig.Id] = nodeConfig.Url
	}
	return config
}
