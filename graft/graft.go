package graft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// TODO recheck the overuse of panics.

const UnknownLeader = "UNKNOWN"

type state int

// TODO add stateUninitialized
const (
	stateFollower state = iota
	stateCandidate
	stateLeader
	stateDead
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrDead      = errors.New("dead")
)

func (s state) String() string {
	switch s {
	case stateFollower:
		return "Follower"
	case stateCandidate:
		return "Candidate"
	case stateLeader:
		return "Leader"
	case stateDead:
		return "Dead"
	default:
		log.Panicf("Unknown state: %d", int(s))
		return "" // Unreachable
	}
}

// TODO make this as default, allow users to override.
const defaultEntryBatchSize = 4096

const defaultSnapshotBufferSize = 1 * 1024 * 1024

// TODO limit broadcastWorker & make each worker send requests to any peer.

type raftState struct {
	state       state
	currentTerm int64
	votedFor    string
	commitIndex int64
	lastApplied int64
}

type Graft struct {
	_ uncopyable

	raftState
	address                   string
	leaderId                  string
	peers                     map[string]*peer
	electionTimer             *periodicTimer
	heartbeatTimer            *periodicTimer
	server                    server
	grpcServer                *grpc.Server
	mut                       sync.Mutex
	applyChan                 chan any
	electionChan              chan int64
	heartbeatChan             chan struct{}
	broadcastChans            map[string]*broadcastChannel
	initialized               bool
	latestConfigUpdate        *pb.ConfigUpdate // Currently active update entry. Might not have been committed yet.
	lastCommittedConfigUpdate *pb.ConfigUpdate // Last update committed to raft log.
	lastAppliedConfigUpdate   *pb.ConfigUpdate // Last update applied to the state machine.
	latestConfigUpdateIndex   int64            // -1 if no update has been applied (using static configuration).
	leaving                   bool             // Set to true when a leader is not part of the new configuration and should be leaving when the configuration is committed.
	// TODO we need some sort of a timeout for this.
	snapshotWriter     SnapshotWriter // Writer for the currently streaming snapshot.
	lastHeartbeatTime  time.Time
	minElectionTimeout time.Duration
	logger             *zap.SugaredLogger

	Id                string
	Persistence       Persistence
	Restore           func(snapshot Snapshot) error
	Apply             func(entries []*pb.LogEntry) (shouldSnapshot bool, err error)
	Snapshot          func(writer io.Writer) error
	ApplyConfigUpdate func(update *pb.ConfigUpdate) error
	Closed            func() error
}

func (g *Graft) String() string {
	return fmt.Sprintf(
		"{state: %v, currentTerm: %d, votedFor: %s, commitIndex: %d, lastApplied: %d, leaderId: %s, len(log)=%d}",
		g.state, g.currentTerm, g.votedFor, g.commitIndex, g.lastApplied, g.leaderId, g.Persistence.EntryCount())
}

type Config struct {
	Id                        string
	Addresses                 map[string]string
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
	HeartbeatMillis           int
	Persistence               Persistence
	Logger                    *zap.Logger
}

func (c *Config) LoggerOrNoop() *zap.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return zap.NewNop()
}

func New(config Config) (*Graft, error) {
	if config.Id == UnknownLeader {
		return nil, fmt.Errorf("sever ID cannot be %s", UnknownLeader)
	}

	g := &Graft{
		Id: config.Id,
		raftState: raftState{
			state:       stateFollower,
			currentTerm: 0,
			votedFor:    UnknownLeader,
			commitIndex: -1,
			lastApplied: -1,
		},
		leaderId:                UnknownLeader,
		address:                 config.Addresses[config.Id],
		latestConfigUpdateIndex: -1,
		minElectionTimeout:      time.Duration(config.ElectionTimeoutLowMillis) * time.Millisecond,
		electionTimer: newRandomizedTimer(
			time.Duration(config.ElectionTimeoutLowMillis)*time.Millisecond,
			time.Duration(config.ElectionTimeoutHighMillis)*time.Millisecond),
		heartbeatTimer:    newTimer(time.Duration(config.HeartbeatMillis) * time.Millisecond),
		server:            server{},
		applyChan:         make(chan any, 1024), // Buffer the channel so that a slow client doesn't immediately block workflow.
		electionChan:      make(chan int64),
		heartbeatChan:     make(chan struct{}),
		broadcastChans:    make(map[string]*broadcastChannel),
		peers:             make(map[string]*peer),
		lastHeartbeatTime: time.Now().Add(-time.Duration(config.ElectionTimeoutLowMillis) * time.Millisecond),
		logger:            config.LoggerOrNoop().With(zap.String("name", "Graft"), zap.String("id", config.Id)).Sugar(),

		Persistence: config.Persistence,
		Restore: func(snapshot Snapshot) error {
			return nil
		},
		Apply: func(entries []*pb.LogEntry) (shouldSnapshot bool, err error) {
			return false, nil
		},
		Snapshot: func(writer io.Writer) error {
			return nil
		},
		ApplyConfigUpdate: func(update *pb.ConfigUpdate) error {
			return nil
		},
	}

	initialConfigUpdate := &pb.ConfigUpdate{
		Phase: pb.ConfigUpdate_APPLIED,
	}
	for id, addr := range config.Addresses {
		initialConfigUpdate.New = append(initialConfigUpdate.New, &pb.NodeConfig{
			Id:      id,
			Address: addr,
		})
	}
	g.unguardedInitConfigUpdate(initialConfigUpdate, -1)

	return g, nil
}

func (g *Graft) GetCurrentTerm() int64 {
	g.mut.Lock()
	defer g.mut.Unlock()
	return g.currentTerm
}

func (g *Graft) ListenAndServe() error {
	if err := g.Initialize(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", g.address)
	if err != nil {
		return err
	}

	g.logger.Infof("Listening on %v", listener.Addr())

	g.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				return handler(context.WithValue(ctx, graftKey{}, g), req)
			}))
	pb.RegisterRaftServer(g.grpcServer, &g.server)
	return g.grpcServer.Serve(listener)
}

func (g *Graft) Initialize() error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.initialized {
		return nil
	}
	g.initialized = true

	go g.applyWorker()
	go g.electionWorker()
	go g.heartbeatWorker()
	for _, peer := range g.peers {
		go g.broadcastWorker(peer, g.broadcastChans[peer.id])
	}

	g.electionTimer.start(func() {
		g.electionChan <- g.GetCurrentTerm()
	})
	g.heartbeatTimer.start(func() {
		g.heartbeatChan <- struct{}{}
	})

	state, err := g.Persistence.RetrieveState()
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
		if err := g.Persistence.SaveState(state); err != nil {
			return err
		}
	}

	g.unguardedTransitionToFollower(state.CurrentTerm)
	g.votedFor = state.VotedFor
	g.commitIndex = state.CommitIndex

	// Publish snapshot to applyChan if there is one.
	metadata, err := g.Persistence.LastSnapshotMetadata()
	if err != nil {
		return err
	}
	if metadata != nil {
		g.applyChan <- metadata
	}

	firstIndex, err := g.Persistence.FirstEntryIndex()
	if err != nil {
		return err
	}

	// Apply committed entries to the state machine.
	// TODO we might want to do this in batches if the log is too big.
	//      or we can use an entry iterator.
	commitIndex := g.commitIndex
	if firstIndex >= 0 && commitIndex >= firstIndex {
		entries, err := g.Persistence.GetEntries(firstIndex, commitIndex)
		if err != nil {
			panic(err)
		}

		for _, e := range entries {
			if e.Type == pb.LogEntry_CONFIG {
				var update *pb.ConfigUpdate
				protoUnmarshal(e.Data, update)
				g.unguardedInitConfigUpdate(update, e.Index)
			}
		}

		g.applyChan <- entries
	}
	return nil
}

func (g *Graft) Close() {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == stateDead {
		return
	}
	g.state = stateDead

	g.heartbeatTimer.stop()
	g.electionTimer.stop()

	close(g.applyChan)
	close(g.electionChan)
	close(g.heartbeatChan)
	for _, ch := range g.broadcastChans {
		ch.close()
	}

	for _, peer := range g.peers {
		peer.closeConn()
	}

	if g.grpcServer != nil {
		g.grpcServer.Stop()
	}
}

func (g *Graft) unguardedCountMajority(condition func(*peer) bool) bool {
	switch g.latestConfigUpdate.Phase {
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

		count1 := len(g.latestConfigUpdate.Old)
		count2 := len(g.latestConfigUpdate.New)

		satisfiedCount1 := 1 // Count self.
		satisfiedCount2 := 0
		if in(g.Id, g.latestConfigUpdate.New) {
			satisfiedCount2++
		}

		for _, p := range g.peers {
			if condition(p) {
				if in(p.id, g.latestConfigUpdate.Old) {
					satisfiedCount1++
				}
				if in(p.id, g.latestConfigUpdate.New) && condition(p) {
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
		if in(g.Id, g.latestConfigUpdate.New) {
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
		case []*pb.LogEntry:
			// Split entries to: commands (applied in batches), configs (passed to callback individually) & NOOP (skipped),
			// and apply them in order.

			commandEntries := make([]*pb.LogEntry, 0, len(e))
			for _, entry := range e {
				switch entry.Type {
				case pb.LogEntry_COMMAND:
					commandEntries = append(commandEntries, entry)
				case pb.LogEntry_CONFIG:
					if len(commandEntries) > 0 {
						g.applyCommands(commandEntries)
						commandEntries = commandEntries[:0]
					}

					var update *pb.ConfigUpdate
					protoUnmarshal(entry.Data, update)
					if err := g.ApplyConfigUpdate(update); err != nil {
						panic(err)
					}

					func() {
						g.mut.Lock()
						defer g.mut.Unlock()

						g.lastAppliedConfigUpdate = update
					}()
				case pb.LogEntry_NOOP:
					// Skip.
				default:
					panic(fmt.Sprintf("Unexpected entry type %v", entry.Type))
				}
			}

			if len(commandEntries) > 0 {
				g.applyCommands(commandEntries)
			}
		case *pb.SnapshotMetadata:
			func() {
				snapshot, err := g.Persistence.OpenSnapshot(e)
				defer snapshot.Close()

				if err != nil {
					panic(err)
				}
				if err := g.Restore(snapshot); err != nil {
					panic(err)
				}

				func() {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.lastApplied = e.LastAppliedIndex
				}()
			}()
		default:
			g.logger.Panicf("Unknown apply entry type %T", e)
		}
	}
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

func (g *Graft) applyCommands(commandEntries []*pb.LogEntry) {
	shouldSnapshot, err := g.Apply(commandEntries)
	if err != nil {
		panic(err)
	}

	func() {
		g.mut.Lock()
		defer g.mut.Unlock()

		g.lastApplied = commandEntries[len(commandEntries)-1].Index

		if shouldSnapshot {
			metadata := &pb.SnapshotMetadata{
				LastAppliedIndex: g.lastApplied,
				LastAppliedTerm:  commandEntries[len(commandEntries)-1].Term,
			}

			writer, err := g.Persistence.CreateSnapshot(metadata)
			if err != nil {
				panic(err)
			}
			defer g.snapshotWriter.Close()

			if err := g.Snapshot(&sequentialSnapshotWriter{base: writer}); err != nil {
				panic(err)
			}
			if _, err = writer.Commit(); err != nil {
				panic(err)
			}
			if err := g.Persistence.TruncateEntriesTo(metadata.LastAppliedIndex); err != nil {
				panic(err)
			}
		}
	}()
}

func (g *Graft) electionWorker() {
	for timeoutTerm := range g.electionChan {
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

					lastLogIndex, err := g.Persistence.LastEntryIndex()
					if err != nil {
						panic(err)
					}

					var lastLogTerm int64 = -1
					if lastLogIndex >= 0 {
						lastLogTerm, err = g.Persistence.GetEntryTerm(lastLogIndex)
						if err != nil {
							panic(err)
						}
					} else if metadata, err := g.Persistence.LastSnapshotMetadata(); err == nil && metadata != nil {
						lastLogIndex, lastLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedTerm
					} else if err != nil {
						panic(err)
					}

					return &pb.RequestVoteRequest{
						Term:         electionTerm,
						CandidateId:  g.Id,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
				}()

				if request == nil {
					return
				}

				if res, rerr := client.RequestVote(context.Background(), request); rerr != nil {
					g.logger.Error("RequestVote->", zap.String("peer", p.id), zap.Any("request", request), zap.Error(cerr))
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
	for range g.heartbeatChan {
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

		We want to make sure that peer's nextIndex lies within 0 to lastIndex+1.
		We also want to take into account cases where there are no snapshot and/or log entries.
	*/

	if p.nextIndex < 0 {
		g.logger.Panicf("Invalid peer.nextIndex: {%s}.nextIndex=%d", p.id, p.nextIndex)
	}

	firstIndex, err := g.Persistence.FirstEntryIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := g.Persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	var metadata *pb.SnapshotMetadata
	if firstIndex < 0 || p.nextIndex < firstIndex {
		// We might have a snapshot that covers nextIndex.
		metadata, err = g.Persistence.LastSnapshotMetadata()
		if err != nil {
			panic(err)
		}
		if p.nextIndex > 0 && (metadata == nil || p.nextIndex <= metadata.LastAppliedIndex) {
			g.logger.Panicf(
				"Invalid nextIndex: {%s}.nextIndex=%d, firstIndex=%d, lastIndex=%d, metadata=%v",
				p.id, p.nextIndex, firstIndex, lastIndex, metadata)
		}
	}

	if metadata != nil {
		snapshot, err := g.Persistence.OpenSnapshot(metadata)
		if err != nil {
			panic(err)
		}
		return &pb.SnapshotRequest{
			Term:     g.currentTerm,
			LeaderId: g.Id,
			Metadata: snapshot.Metadata(),
			// Data, Offset & Done are filled later.
		}
	}

	if p.nextIndex > lastIndex+1 {
		g.logger.Panicf("Invalid nextIndex: {%s}.nextIndex=%d, lastIndex=%d", p.id, p.nextIndex, lastIndex)
	}

	request := &pb.AppendEntriesRequest{
		Term:              g.currentTerm,
		LeaderId:          g.Id,
		LeaderCommitIndex: g.commitIndex,
	}

	prevLogIndex := p.nextIndex - 1
	if prevLogIndex < 0 {
		request.PrevLogIndex, request.PrevLogTerm = -1, -1
	} else if firstIndex >= 0 && prevLogIndex >= firstIndex && prevLogIndex <= lastIndex {
		prevLogTerm, err := g.Persistence.GetEntryTerm(prevLogIndex)
		if err != nil {
			panic(err)
		}
		request.PrevLogIndex, request.PrevLogTerm = prevLogIndex, prevLogTerm
	} else if metadata, err := g.Persistence.LastSnapshotMetadata(); err == nil &&
		metadata != nil && prevLogIndex == metadata.LastAppliedIndex {
		request.PrevLogIndex, request.PrevLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedIndex
	} else if err != nil {
		panic(err)
	} else {
		g.logger.Panicf("Invalid prevLogIndex: {%s}.prevLogIndex=%d, firstIndex=%d, lastIndex=%d, metadata=%v",
			p.id, prevLogIndex, firstIndex, lastIndex, metadata)
	}

	if p.nextIndex <= lastIndex && p.nextIndex >= 0 {
		es, err := g.Persistence.GetEntries(p.nextIndex, min(p.nextIndex+defaultEntryBatchSize-1, lastIndex))
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
			res := func() proto.Message {
				switch req := req.(type) {
				case *pb.AppendEntriesRequest:
					if res, err := client.AppendEntries(context.Background(), req); err != nil {
						g.logger.Error("AppendEntries->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
						return nil
					} else {
						return res
					}

				case *pb.SnapshotRequest:
					if stream, err := client.InstallSnapshot(context.Background()); err != nil {
						g.logger.Error("InstallSnapshot->", zap.String("peer", p.id), zap.Any("request", req), zap.Error(err), zap.Stringer("state", g))
						return nil
					} else {
						snapshot, err := g.Persistence.OpenSnapshot(req.Metadata)
						if err != nil {
							panic(err)
						}

						defer snapshot.Close()

						if snapshot.Metadata().Size <= defaultSnapshotBufferSize {
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
								res, err := stream.CloseAndRecv()
								if err != nil {
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
									// TODO this will cause a leak. We need a way to discard sending the snapshot first.
									panic(err)
								}

								req.Data = buffer[:n]
								err = stream.Send(req)
								if errors.Is(err, io.EOF) {
									// The corresponding node closed the stream & returned early. Might be a change in leadership.
									res, err := stream.CloseAndRecv()
									if err != nil {
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
						p.lastHeartbeatTime = time.Now()

						req := req.(*pb.AppendEntriesRequest)
						if len(req.Entries) > 0 {
							p.nextIndex = req.Entries[len(req.Entries)-1].Index + 1
							p.matchIndex = p.nextIndex - 1
						} else if req.PrevLogIndex >= 0 {
							p.nextIndex = req.PrevLogIndex + 1
							p.matchIndex = req.PrevLogIndex
						}

						lastIndex, err := g.Persistence.LastEntryIndex()
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
						p.lastHeartbeatTime = time.Now()
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
						p.lastHeartbeatTime = time.Now()

						req := req.(*pb.SnapshotRequest)
						p.nextIndex = req.Metadata.LastAppliedIndex + 1
						p.matchIndex = req.Metadata.LastAppliedIndex
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
	g.leaderId = g.Id

	lastIndex, err := g.Persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	for _, peer := range g.peers {
		peer.nextIndex = lastIndex + 1 // Assumes lastIndex = -1 if the log is empty.
		peer.matchIndex = -1
	}

	// Append NOOP entry if we have uncommitted entries from previous terms.
	if g.commitIndex < lastIndex {
		_, err := g.Persistence.Append(g.unguardedCapturePersistedState(), []*pb.LogEntry{
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
	g.votedFor = g.Id // Vote for self.
	g.leaderId = UnknownLeader
	g.electionTimer.reset()
	g.heartbeatTimer.pause()
}

func (g *Graft) unguardedTransitionToFollower(term int64) {
	g.logger.Info("Transitioning to Follower", zap.Stringer("state", g))

	g.state = stateFollower
	g.currentTerm = term
	g.votedFor = ""
	g.electionTimer.reset()
	g.heartbeatTimer.pause() // If we were a leader.
}

func (g *Graft) unguardedFlushCommittedEntries() {
	newCommitIndex := g.commitIndex
	lastIndex, err := g.Persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	if lastIndex < 0 { // Log is empty.
		return
	}

	// Note that commitIndex is initialized to -1 initially.
	entries, err := g.Persistence.GetEntries(g.commitIndex+1, lastIndex)
	for _, e := range entries {
		// We must only commit entries from current term. See paper section 5.4.2.
		if e.Term == g.currentTerm && g.unguardedCountMajority(func(p *peer) bool {
			return p.matchIndex >= e.Index
		}) {
			newCommitIndex = e.Index
		}
	}

	if newCommitIndex > g.commitIndex {
		g.unguardedUpdateCommitIndex(newCommitIndex)
		g.applyChan <- entries
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
	}
}

func (g *Graft) unguardedUpdateCommitIndex(newCommitIndex int64) {
	if g.latestConfigUpdateIndex >= 0 && newCommitIndex >= g.latestConfigUpdateIndex {
		g.lastCommittedConfigUpdate = g.latestConfigUpdate
	}
	g.commitIndex = newCommitIndex
}

func (g *Graft) unguardedContinueConfigUpdate() {
	if g.latestConfigUpdateIndex < 0 || g.commitIndex < g.latestConfigUpdateIndex {
		return
	}

	switch g.latestConfigUpdate.Phase {
	case pb.ConfigUpdate_LEARNING:
		hasLearners := false
		for _, p := range g.peers {
			if p.learner {
				hasLearners = true
				break
			}
		}

		if !hasLearners {
			update := cloneMsg(g.latestConfigUpdate)
			update.Phase = pb.ConfigUpdate_JOINT
			if err := g.unguardedAppendConfigUpdate(update); err != nil {
				panic(err)
			}
		}
	case pb.ConfigUpdate_JOINT:
		update := cloneMsg(g.latestConfigUpdate)
		update.Phase = pb.ConfigUpdate_APPLIED
		if err := g.unguardedAppendConfigUpdate(update); err != nil {
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
	myLastLogIndex, err := g.Persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}

	var myLastLogTerm int64 = -1
	if myLastLogIndex >= 0 {
		myLastLogTerm, err = g.Persistence.GetEntryTerm(myLastLogIndex)
		if err != nil {
			panic(err)
		}
	} else if metadata, err := g.Persistence.LastSnapshotMetadata(); err == nil && metadata != nil {
		myLastLogIndex, myLastLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedTerm
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

	grantVote := (g.votedFor == "" || g.votedFor == request.CandidateId) && g.unguardedIsCandidateLogUpToDate(request)
	if grantVote {
		g.votedFor = request.CandidateId
		if !electionTimerIsReset {
			g.electionTimer.reset()
		}
	}

	if grantVote {
		g.lastHeartbeatTime = time.Now()
	}

	if err := g.Persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
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

	lastIndex, err := g.Persistence.LastEntryIndex()
	if err != nil {
		panic(err)
	}
	if request.PrevLogIndex > lastIndex {
		if err := g.Persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
			panic(err)
		}
		return &pb.AppendEntriesResponse{
			Term:    request.Term,
			Success: false,
		}, nil
	}

	firstIndex, err := g.Persistence.FirstEntryIndex()
	if err != nil {
		panic(err)
	}
	if request.PrevLogIndex >= firstIndex && firstIndex >= 0 {
		myPrevLogTerm, err := g.Persistence.GetEntryTerm(request.PrevLogIndex)
		if err != nil {
			panic(err)
		}
		if request.PrevLogTerm != myPrevLogTerm {
			if err := g.Persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
				panic(err)
			}
			return &pb.AppendEntriesResponse{
				Term:    request.Term,
				Success: false,
			}, nil
		}
	}

	if request.PrevLogIndex+1 <= lastIndex {
		if err := g.Persistence.TruncateEntriesFrom(request.PrevLogIndex + 1); err != nil {
			panic(err)
		}
	}

	_, err = g.Persistence.Append(g.unguardedCapturePersistedState(), request.Entries)
	if err != nil {
		panic(err)
	}

	if request.LeaderCommitIndex > g.commitIndex {
		lastIndex, err := g.Persistence.LastEntryIndex()
		if err != nil {
			panic(err)
		}
		newCommitIndex := min(request.LeaderCommitIndex, lastIndex)
		if newCommitIndex > g.commitIndex {
			g.unguardedUpdateCommitIndex(newCommitIndex)
		}
	}

	// Apply config entries if any.
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
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&pb.SnapshotResponse{Term: g.GetCurrentTerm()})
		} else if err != nil {
			return err
		}

		success, err := g.installSnapshotBlock(request)
		if err != nil {
			return err
		} else if !success {
			return stream.SendAndClose(&pb.SnapshotResponse{Term: g.GetCurrentTerm()})
		}
	}
}

func (g *Graft) installSnapshotBlock(request *pb.SnapshotRequest) (bool, error) {
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

	if request.Offset == 0 {
		// Discard potentially incomplete snapshot.
		if g.snapshotWriter != nil {
			g.snapshotWriter.Close()
			g.snapshotWriter = nil
		}

		// Begin a new snapshot.
		writer, err := g.Persistence.CreateSnapshot(request.Metadata)
		if err != nil {
			panic(err)
		}
		g.snapshotWriter = writer
	}

	_, err := g.snapshotWriter.WriteAt(request.Data, request.Offset)
	if err != nil {
		panic(err)
	}

	if request.Done {
		defer func() {
			g.snapshotWriter.Close()
			g.snapshotWriter = nil
		}()

		metadata, err := g.snapshotWriter.Commit()
		if err != nil {
			panic(err)
		}

		// Truncate entries invalidated by the snapshot.
		firstIndex, err := g.Persistence.FirstEntryIndex()
		if err != nil {
			panic(err)
		}
		if firstIndex >= 0 { // If the log has entries.
			if request.Metadata.LastAppliedIndex >= firstIndex {
				lastIndex, err := g.Persistence.LastEntryIndex()
				if err != nil {
					panic(err)
				}
				err = g.Persistence.TruncateEntriesTo(min(request.Metadata.LastAppliedIndex, lastIndex))
			} else if err := g.Persistence.TruncateEntriesFrom(firstIndex); err != nil { // Truncate entire log.
				panic(err)
			}
		}

		// Apply snapshot configuration.
		if request.Metadata.ConfigUpdate != nil {
			g.unguardedInitConfigUpdate(request.Metadata.ConfigUpdate, request.Metadata.LastAppliedIndex)
		}

		// Publish snapshot to state-machine.
		g.applyChan <- metadata
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
			address:           config.Address,
			nextIndex:         f.lastLogIndex + 1,
			matchIndex:        -1,
			learner:           false,
			lastHeartbeatTime: f.defaultLastHeartbeat,
		}
		f.existingPeers[config.Id] = p
	}
	return p
}

func (g *Graft) unguardedAppendConfigUpdate(update *pb.ConfigUpdate) error {
	nextIndex, err := g.Persistence.Append(g.unguardedCapturePersistedState(), []*pb.LogEntry{{
		Term: g.currentTerm,
		Type: pb.LogEntry_CONFIG,
		Data: protoMarshal(update),
	}})
	if err != nil {
		return err
	}
	g.unguardedInitConfigUpdate(update, nextIndex-1)
	g.heartbeatTimer.poke() // Broadcast update.
	return nil
}

func (g *Graft) unguardedInitConfigUpdate(update *pb.ConfigUpdate, updateIndex int64) {
	g.logger.Info("Applying ConfigUpdate", zap.Any("update", update), zap.Stringer("state", g))

	g.latestConfigUpdate = update
	g.latestConfigUpdateIndex = updateIndex

	// If the leader is not in the new configuration, make it step down when the configuration is committed.
	if g.state == stateLeader && update.Phase == pb.ConfigUpdate_APPLIED && !in(g.Id, update.New) {
		g.leaving = true
	}

	lastIndex, err := g.Persistence.LastEntryIndex()
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

	delete(newPeers, g.Id) // Remove self.

	newBroadcastChans := make(map[string]*broadcastChannel)
	for _, p := range newPeers {
		ch, ok := g.broadcastChans[p.id]
		if ok {
			newBroadcastChans[p.id] = ch
			delete(g.broadcastChans, p.id)
		} else {
			newBroadcastChans[p.id] = newBroadcastChannel()
			if g.initialized {
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

	nextIndex, err := g.Persistence.Append(g.unguardedCapturePersistedState(), entries)
	if err != nil {
		return -1, err
	}
	g.heartbeatTimer.poke() // Broadcast new entries.
	return nextIndex - int64(len(entries)), nil
}

func (g *Graft) ConfigUpdate(id string, addedNodes map[string]string, removedNodes []string) error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == stateDead {
		return ErrDead
	}

	if g.state != stateLeader {
		return ErrNotLeader
	}

	existingNodes := make(map[string]string)
	for _, peer := range g.peers {
		existingNodes[peer.id] = peer.address
	}
	existingNodes[g.Id] = g.address

	// Perform some clean-up: make sure existingNodes & addedNodes are disjoint, and removedNodes is a subset of
	// existingNodes and is disjoint with addedNodes.

	for id := range existingNodes {
		delete(addedNodes, id)
	}

	cleanedRemovedNodes := make([]string, 0)
	for _, id := range removedNodes {
		if _, ok := existingNodes[id]; ok {
			cleanedRemovedNodes = append(cleanedRemovedNodes, id)
		}
	}
	removedNodes = cleanedRemovedNodes

	for _, id := range removedNodes {
		delete(addedNodes, id)
	}

	// Create config update.

	update := &pb.ConfigUpdate{
		Id:  id,
		Old: g.latestConfigUpdate.New,
	}

	for id, address := range existingNodes {
		if !slices.Contains(removedNodes, id) {
			update.New = append(update.New, &pb.NodeConfig{
				Id:      id,
				Address: address,
			})
		}
	}

	for id, address := range addedNodes {
		update.New = append(update.New, &pb.NodeConfig{
			Id:      id,
			Address: address,
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
	for _, nodeConfig := range g.latestConfigUpdate.New {
		config[nodeConfig.Id] = nodeConfig.Address
	}
	return config
}
