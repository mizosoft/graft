package graft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const UnknownLeader = "UNKNOWN"

type state int

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

const entryBatchSize = 4096

type broadcastChannel struct {
	c      chan struct{}
	closed bool
	mut    sync.Mutex
}

func newBroadcastChannel() *broadcastChannel {
	return &broadcastChannel{
		c: make(chan struct{}, 1), // We only need an additional (keep_broadcasting) signal.
	}
}

func (c *broadcastChannel) notify() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.closed {
		return
	}

	select {
	case c.c <- struct{}{}:
	default:
		// Ignore: c has a capacity of 1 so a new AppendEntries is surely running in the future and that is all
		// what we need to know.
	}
}

func (c *broadcastChannel) close() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	close(c.c)
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

	raftState
	address                         string
	leaderId                        string
	peers                           map[string]*peer
	electionTimer                   *periodicTimer
	heartbeatTimer                  *periodicTimer
	server                          server
	grpcServer                      *grpc.Server
	persistence                     Persistence
	mut                             sync.Mutex
	applyChan                       chan any
	electionChan                    chan int64
	heartbeatChan                   chan struct{}
	broadcastChans                  map[string]*broadcastChannel
	initialized                     bool
	lastUpdate, lastCommittedUpdate *pb.ConfigUpdate
	lastUpdateIndex                 int64 // -1 if no update has been applied (using static configuration).
	leaving                         bool  // Set to true when a leader is not part of the new configuration.
	lastHeartbeatTime               time.Time
	minElectionTimeout              time.Duration

	Id               string
	Restore          func(snapshot Snapshot)
	Apply            func(entries []*pb.LogEntry) (snapshot []byte)
	ConfigUpdateDone func(update *pb.ConfigUpdate)
	Closed           func()
}

func (g *Graft) String() string {
	return fmt.Sprintf(
		"{state: %v, currentTerm: %d, votedFor: %s, commitIndex: %d, lastApplied: %d, leaderId: %s, len(log)=%d}",
		g.state, g.currentTerm, g.votedFor, g.commitIndex, g.lastApplied, g.leaderId, g.persistence.EntryCount())
}

func (g *Graft) log(format string, vals ...any) {
	log.Printf("%s: %v\n", g.Id, fmt.Sprintf(format, vals...))
}

func (g *Graft) fatal(err any) {
	log.Fatalf("Graft%v encountered a fatal error: %v\n%s", g, err, debug.Stack())
}

type Config struct {
	Id                        string
	Addresses                 map[string]string
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
	HeartbeatMillis           int
	Persistence               Persistence
}

func New(config Config) (*Graft, error) {
	if config.Id == UnknownLeader {
		return nil, fmt.Errorf("sever ID cannot be %s", UnknownLeader)
	}

	update := &pb.ConfigUpdate{
		Phase: pb.ConfigUpdate_APPLIED,
	}
	for id, addr := range config.Addresses {
		update.New = append(update.New, &pb.NodeConfig{
			Id:      id,
			Address: addr,
		})
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
		address:            config.Addresses[config.Id],
		lastUpdateIndex:    -1,
		minElectionTimeout: time.Duration(config.ElectionTimeoutLowMillis) * time.Millisecond,
		electionTimer: newRandomizedTimer(
			time.Duration(config.ElectionTimeoutLowMillis)*time.Millisecond,
			time.Duration(config.ElectionTimeoutHighMillis)*time.Millisecond),
		heartbeatTimer:    newTimer(time.Duration(config.HeartbeatMillis) * time.Millisecond),
		server:            server{},
		applyChan:         make(chan any, 1024), // Buffer the channel so that a slow client doesn't immediately block workflow.
		electionChan:      make(chan int64),
		heartbeatChan:     make(chan struct{}),
		broadcastChans:    make(map[string]*broadcastChannel),
		persistence:       config.Persistence,
		peers:             make(map[string]*peer),
		lastHeartbeatTime: time.Now().Add(-time.Duration(config.ElectionTimeoutLowMillis) * time.Millisecond),

		ConfigUpdateDone: func(update *pb.ConfigUpdate) {},
		Apply: func(entries []*pb.LogEntry) (snapshot []byte) {
			return nil
		},
		Restore: func(snapshot Snapshot) {},
	}
	g.unguardedApplyUpdate(update, -1)
	return g, nil
}

func (g *Graft) GetCurrentTerm() int64 {
	g.mut.Lock()
	defer g.mut.Unlock()
	return g.currentTerm
}

func (g *Graft) Serve() error {
	g.Initialize()

	listener, err := net.Listen("tcp", g.address)
	if err != nil {
		return err
	}

	g.log("listening on %v", listener.Addr())

	g.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				return handler(context.WithValue(ctx, graftKey{}, g), req)
			}))
	pb.RegisterRaftServer(g.grpcServer, &g.server)
	return g.grpcServer.Serve(listener)
}

func (g *Graft) Initialize() {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.initialized {
		return
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

	state := g.persistence.RetrieveState()
	if state == nil {
		state = &pb.PersistedState{
			CurrentTerm: 0,
			VotedFor:    UnknownLeader,
			CommitIndex: -1,
		}
		g.persistence.SaveState(state)
	}

	g.votedFor = state.VotedFor
	g.commitIndex = state.CommitIndex
	g.unguardedTransitionToFollower(state.CurrentTerm)

	// Restore the state-machine.
	snapshot := g.persistence.RetrieveSnapshot()
	if snapshot != nil {
		g.Restore(snapshot)
	}

	firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
	commitIndex := g.commitIndex // Only apply committed entries.
	if firstIndex >= 0 && commitIndex >= firstIndex {
		g.unguardedApply(firstIndex, commitIndex, true)
	}
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
		g.grpcServer.GracefulStop()
	}
}

func (g *Graft) unguardedCountMajority(condition func(*peer) bool) bool {
	switch g.lastUpdate.Phase {
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

		count1 := len(g.lastUpdate.Old)
		count2 := len(g.lastUpdate.New)

		satisfiedCount1 := 1 // Count self.
		satisfiedCount2 := 0
		if in(g.Id, g.lastUpdate.New) {
			satisfiedCount2++
		}

		for _, p := range g.peers {
			if condition(p) {
				if in(p.id, g.lastUpdate.Old) {
					satisfiedCount1++
				}
				if in(p.id, g.lastUpdate.New) && condition(p) {
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
		if in(g.Id, g.lastUpdate.New) {
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
			snapshotData := g.Apply(e)
			g.lastApplied = e[len(e)-1].Index
			if snapshotData != nil {
				func() {
					g.mut.Lock()
					defer g.mut.Unlock()

					metadata := &pb.SnapshotMetadata{
						LastAppliedIndex: g.lastApplied,
						LastAppliedTerm:  e[len(e)-1].Term,
					}
					if g.lastUpdateIndex >= 0 && g.lastUpdateIndex <= g.lastApplied {
						metadata.ConfigUpdate = g.lastUpdate
					}

					g.persistence.SaveSnapshot(NewSnapshot(metadata, snapshotData))
					g.persistence.TruncateEntriesTo(metadata.LastAppliedIndex)
				}()
			}
		case Snapshot:
			g.Restore(e)
			g.lastApplied = e.Metadata().LastAppliedIndex
		case []*pb.ConfigUpdate:
			for _, c := range e {
				g.ConfigUpdateDone(c)
			}
		default:
			log.Panicf("unknown type %T", e)
		}
	}
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
	case stateLeader:
		// We might've already won an election for this term but couldn't stop the timer on time.
		g.log("Election timed out too late, we are the leader now")
		return
	case stateCandidate, stateFollower:
		// Continue election.
	case stateDead:
		return
	}

	g.unguardedTransitionToCandidate()

	voteGranted := make(map[string]bool)
	electionTerm := timeoutTerm + 1
	for _, p := range g.peers {
		go func() {
			if client, cerr := p.client(); cerr != nil {
				g.log("Error connecting to peer %s: %v", p.id, cerr)
			} else {
				var lastLogIndex int64
				var lastLogTerm int64
				request := func() *pb.RequestVoteRequest {
					g.mut.Lock()
					defer g.mut.Unlock()

					if g.currentTerm != electionTerm || g.state != stateCandidate {
						return nil
					}

					lastLogIndex, lastLogTerm = g.persistence.LastLogIndexAndTerm()
					if lastLogIndex < 0 {
						if metadata := g.persistence.SnapshotMetadata(); metadata != nil {
							lastLogIndex, lastLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedTerm
						}
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
					g.log("RequestVote to %s failed: %v", p.id, rerr)
				} else {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.log("RequestVote(%v) to %s at election (%d) got {%v}, state=%v", request, p.id, electionTerm, res, g)

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

func (g *Graft) broadcastWorker(peer *peer, c *broadcastChannel) {
	for range c.c {
		req := func() proto.Message {
			g.mut.Lock()
			defer g.mut.Unlock()

			if g.state != stateLeader {
				return nil
			}

			// Check nextIndex validity.
			nextIndex := peer.nextIndex
			firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
			lastIndex, _ := g.persistence.LastLogIndexAndTerm()
			if nextIndex < firstIndex || nextIndex > lastIndex+1 { // Even if lastIndex == -1 we want nextIndex = 0.
				if metadata := g.persistence.SnapshotMetadata(); metadata == nil || nextIndex < 0 || nextIndex > metadata.LastAppliedIndex+1 {
					g.fatal(fmt.Errorf(
						"invalid nextIndex: nextIndex=%d, firstIndex=%d, lastIndex=%d, snapshotMetadata=%v",
						nextIndex, firstIndex, metadata.LastAppliedIndex, metadata))
				}
			}

			if firstIndex < 0 || nextIndex < firstIndex {
				if snapshot := g.persistence.RetrieveSnapshot(); snapshot != nil {
					return &pb.SnapshotRequest{
						Term:     g.currentTerm,
						LeaderId: g.Id,
						Offset:   0,
						Done:     true,
						Metadata: snapshot.Metadata(),
						Data:     snapshot.Data(),
					}
				}
			}

			request := &pb.AppendEntriesRequest{
				Term:              g.currentTerm,
				LeaderId:          g.Id,
				LeaderCommitIndex: g.commitIndex,
			}

			prevLogIndex := nextIndex - 1
			if prevLogIndex < 0 {
				request.PrevLogIndex, request.PrevLogTerm = -1, -1
			} else if firstIndex >= 0 && prevLogIndex >= firstIndex && prevLogIndex <= lastIndex {
				request.PrevLogIndex, request.PrevLogTerm = prevLogIndex, g.persistence.GetEntryTerm(prevLogIndex)
			} else if metadata := g.persistence.SnapshotMetadata(); metadata != nil && prevLogIndex == metadata.LastAppliedIndex {
				request.PrevLogIndex, request.PrevLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedIndex
			} else {
				g.fatal(fmt.Errorf("invalid prevLogIndex: %d", prevLogIndex))
			}

			if nextIndex <= lastIndex && nextIndex >= 0 {
				from := nextIndex
				to := nextIndex + entryBatchSize - 1
				if to > lastIndex {
					to = lastIndex
				}
				request.Entries = g.persistence.GetEntries(from, to)
			}

			return request
		}()

		if req == nil {
			continue
		}

		if client, err := peer.client(); err != nil {
			g.log("Error connecting to peer %s: %v", peer.id, err)
		} else {
			res := func() proto.Message {
				switch req := req.(type) {
				case *pb.AppendEntriesRequest:
					if res, err := client.AppendEntries(context.Background(), req); err != nil {
						g.log("AppendEntries(%v) to %s failed: %v", req, peer.id, err)
						return nil
					} else {
						return res
					}
				case *pb.SnapshotRequest:
					if res, err := client.InstallSnapshot(context.Background(), req); err != nil {
						g.log("InstallSnapshot(%v) to %s failed: %v", req, peer.id, err)
						return nil
					} else {
						return res
					}
				default:
					log.Panicf("Unknown request type: %T", req)
					return nil // Unreachable
				}
			}()

			var doRetry bool
			switch res := res.(type) {
			case *pb.AppendEntriesResponse:
				doRetry = func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.log("AppendEntries(%v) to %s got {%v}, state=%v", req, peer.id, res, g)

					if g.state != stateLeader {
						return false
					}

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
						return false
					} else if res.Success {
						peer.lastHeartbeatTime = time.Now()

						req := req.(*pb.AppendEntriesRequest)
						if len(req.Entries) > 0 {
							peer.nextIndex = req.Entries[len(req.Entries)-1].Index + 1
							peer.matchIndex = peer.nextIndex - 1
						} else if req.PrevLogIndex >= 0 {
							peer.nextIndex = req.PrevLogIndex + 1
							peer.matchIndex = req.PrevLogIndex
						}

						lastIndex, _ := g.persistence.LastLogIndexAndTerm()

						if peer.learner && peer.matchIndex >= lastIndex {
							peer.learner = false
						}

						g.unguardedFlushCommittedEntries()
						g.unguardedContinueConfigUpdate()

						return peer.matchIndex < lastIndex
					} else {
						peer.lastHeartbeatTime = time.Now()
						peer.nextIndex--
						return true
					}
				}()

			case *pb.SnapshotResponse:
				doRetry = func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.log("AppendEntries(%v) to %s got {%v}, state=%v", req, peer.id, res, g)

					if g.state != stateLeader {
						return false
					}

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
						return false
					} else {
						peer.lastHeartbeatTime = time.Now()

						req := req.(*pb.SnapshotRequest)
						peer.nextIndex = req.Metadata.LastAppliedIndex + 1
						peer.matchIndex = req.Metadata.LastAppliedIndex
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

func (g *Graft) unguardedFlushCommittedEntries() {
	newCommitIndex := g.commitIndex
	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
	for i := g.commitIndex + 1; i <= lastIndex; i++ { // Note that commitIndex is initialized to -1 initially.
		// We must only commit entries from current term. See paper section 5.4.2.
		if g.persistence.GetEntryTerm(i) == g.currentTerm && g.unguardedCountMajority(func(p *peer) bool {
			return p.matchIndex >= i
		}) {
			newCommitIndex = i
		}
	}

	if newCommitIndex > g.commitIndex {
		g.unguardedCommit(newCommitIndex)
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
	}
}

func (g *Graft) unguardedTransitionToLeader() {
	g.log("Transitioning to Leader, state=%v", g)
	g.state = stateLeader
	g.leaderId = g.Id

	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
	for _, peer := range g.peers {
		peer.nextIndex = lastIndex + 1
		peer.matchIndex = -1
	}

	// Append NOOP entry if we have uncommitted entries from previous terms.
	if g.commitIndex < lastIndex {
		g.persistence.Append(g.unguardedCapturePersistedState(), []*pb.LogEntry{
			{
				Term: g.currentTerm,
				Type: pb.LogEntry_NOOP,
			},
		})
	}

	g.electionTimer.pause()
	g.heartbeatTimer.poke() // Establish authority.
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.log("Transitioning to Candidate, state=%v", g)
	g.state = stateCandidate
	g.currentTerm++
	g.votedFor = g.Id // Vote for self.
	g.leaderId = UnknownLeader
	g.electionTimer.reset()
	g.heartbeatTimer.pause()
}

func (g *Graft) unguardedTransitionToFollower(term int64) {
	g.log("Transitioning to Follower at (%d), state=%v", term, g)
	g.state = stateFollower
	g.currentTerm = term
	g.votedFor = ""
	g.electionTimer.reset()
	g.heartbeatTimer.pause() // If we were a leader.
}

func (g *Graft) unguardedContinueConfigUpdate() {
	if g.lastUpdateIndex < 0 || g.commitIndex < g.lastUpdateIndex {
		return
	}

	switch g.lastUpdate.Phase {
	case pb.ConfigUpdate_LEARNING:
		hasLearners := false
		for _, p := range g.peers {
			if p.learner {
				hasLearners = true
				break
			}
		}

		if !hasLearners {
			update := cloneMsg(g.lastUpdate)
			update.Phase = pb.ConfigUpdate_JOINT
			g.unguardedAppendUpdate(update)
		}
	case pb.ConfigUpdate_JOINT:
		update := cloneMsg(g.lastUpdate)
		update.Phase = pb.ConfigUpdate_APPLIED
		g.unguardedAppendUpdate(update)
	case pb.ConfigUpdate_APPLIED:
		if g.leaving {
			g.leaving = false
			g.unguardedTransitionToFollower(g.currentTerm) // Step down.
		}
	}
}

func (g *Graft) unguardedCommit(newCommitIndex int64) {
	if g.lastUpdateIndex >= 0 && newCommitIndex >= g.lastUpdateIndex {
		g.lastCommittedUpdate = g.lastUpdate
	}

	prevCommitIndex := g.commitIndex
	g.commitIndex = newCommitIndex
	g.unguardedApply(prevCommitIndex+1, newCommitIndex, false)
}

func (g *Graft) unguardedApply(from, to int64, applyConfigUpdates bool) {
	commandEntries := make([]*pb.LogEntry, 0)
	for _, entry := range g.persistence.GetEntries(from, to) {
		switch entry.Type {
		case pb.LogEntry_COMMAND:
			commandEntries = append(commandEntries, entry)
		case pb.LogEntry_CONFIG:
			if len(commandEntries) > 0 {
				g.applyChan <- cloneMsgs(commandEntries)
				commandEntries = commandEntries[:0]
			}

			var update pb.ConfigUpdate
			protoUnmarshal(entry.Data, &update)
			if applyConfigUpdates {
				g.unguardedApplyUpdate(&update, entry.Index)
			}
			g.applyChan <- &update
		}
	}

	if len(commandEntries) > 0 {
		g.applyChan <- cloneMsgs(commandEntries)
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
	if g.lastCommittedUpdate == nil {
		return false // We can't know otherwise.
	}

	switch g.lastCommittedUpdate.Phase {
	case pb.ConfigUpdate_LEARNING:
		return !in(peerId, g.lastCommittedUpdate.Old)
	case pb.ConfigUpdate_JOINT:
		return !in(peerId, g.lastCommittedUpdate.Old) && !in(peerId, g.lastCommittedUpdate.New)
	case pb.ConfigUpdate_APPLIED:
		return !in(peerId, g.lastCommittedUpdate.New)
	default:
		log.Panicf("Unknown phase: %v", g.lastCommittedUpdate.Phase)
		return false // Unreachable.
	}
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	myLastLogIndex, myLastLogTerm := g.persistence.LastLogIndexAndTerm()
	if myLastLogIndex < 0 {
		if metadata := g.persistence.SnapshotMetadata(); metadata != nil {
			myLastLogIndex, myLastLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedTerm
		}
	}

	// Note that this comparison works with the -1 sentinel values.
	return myLastLogTerm < request.LastLogTerm ||
		(myLastLogTerm == request.LastLogTerm && myLastLogIndex <= request.LastLogIndex)
}

func (g *Graft) requestVote(request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received RequestVote(%v), state=%v", request, g)

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

	g.persistence.SaveState(g.unguardedCapturePersistedState())
	return &pb.RequestVoteResponse{
		Term:        g.currentTerm,
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) appendEntries(request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received AppendEntries(%v), state=%v", request, g)

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
		g.log("Changed leadership (%s -> %s), state=%v", g.leaderId, request.LeaderId, g)
		g.leaderId = request.LeaderId
	}

	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
	if request.PrevLogIndex > lastIndex {
		g.persistence.SaveState(g.unguardedCapturePersistedState())
		return &pb.AppendEntriesResponse{
			Term:    request.Term,
			Success: false,
		}, nil
	}

	firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
	if request.PrevLogIndex >= firstIndex && firstIndex >= 0 {
		myPrevLogTerm := g.persistence.GetEntryTerm(request.PrevLogIndex)
		if request.PrevLogTerm != myPrevLogTerm {
			g.persistence.SaveState(g.unguardedCapturePersistedState())
			return &pb.AppendEntriesResponse{
				Term:    request.Term,
				Success: false,
			}, nil
		}
	}

	if request.PrevLogIndex+1 <= lastIndex {
		g.persistence.TruncateEntriesFrom(request.PrevLogIndex + 1)
	}

	g.persistence.Append(g.unguardedCapturePersistedState(), request.Entries)

	if request.LeaderCommitIndex > g.commitIndex {
		lastIndex, _ := g.persistence.LastLogIndexAndTerm()
		newCommitIndex := min(request.LeaderCommitIndex, lastIndex)
		if newCommitIndex > g.commitIndex {
			g.unguardedCommit(newCommitIndex)
		}
	}

	for _, entry := range request.Entries {
		if entry.Type == pb.LogEntry_CONFIG {
			var update pb.ConfigUpdate
			protoUnmarshal(entry.Data, &update)
			g.unguardedApplyUpdate(&update, entry.Index)
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    request.Term,
		Success: true,
	}, nil
}

// FIXME may want to use streaming
func (g *Graft) installSnapshot(request *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received InstallSnapshot(%v), state=%v", request, g)

	if g.state == stateDead {
		return nil, ErrDead
	}

	if request.Term < g.currentTerm || g.unguardedIsUnknownPeer(request.LeaderId) {
		return &pb.SnapshotResponse{
			Term: g.currentTerm,
		}, nil
	}

	g.lastHeartbeatTime = time.Now()

	if request.Term > g.currentTerm {
		g.unguardedTransitionToFollower(request.Term)
	} else {
		g.electionTimer.reset()
	}

	if g.leaderId != request.LeaderId {
		g.log("Changed leadership (%s -> %s), state=%v", g.leaderId, request.LeaderId, g)
		g.leaderId = request.LeaderId
	}

	g.persistence.SaveState(g.unguardedCapturePersistedState())

	if !request.Done {
		return nil, fmt.Errorf("expected snapshot to be done in one call")
	}

	if request.Offset > 0 {
		return nil, fmt.Errorf("expected offset to be zero")
	}

	snapshot := NewSnapshot(request.Metadata, request.Data)
	g.persistence.SaveSnapshot(snapshot)

	firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
	if request.Metadata.LastAppliedIndex >= firstIndex {
		g.persistence.TruncateEntriesTo(request.Metadata.LastAppliedIndex)
	} else if firstIndex >= 0 {
		g.persistence.TruncateEntriesFrom(firstIndex) // Truncate entire log.
	}

	if request.Metadata.ConfigUpdate != nil {
		g.unguardedApplyUpdate(request.Metadata.ConfigUpdate, request.Metadata.LastAppliedIndex)
	}

	g.applyChan <- snapshot
	return &pb.SnapshotResponse{
		Term: g.currentTerm,
	}, nil
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

func (g *Graft) unguardedAppendUpdate(update *pb.ConfigUpdate) {
	updateIndex := g.persistence.Append(g.unguardedCapturePersistedState(), []*pb.LogEntry{{
		Term: g.currentTerm,
		Type: pb.LogEntry_CONFIG,
		Data: protoMarshal(update),
	}}) - 1
	g.unguardedApplyUpdate(update, updateIndex)
	g.heartbeatTimer.poke() // Broadcast update.
}

func (g *Graft) unguardedApplyUpdate(update *pb.ConfigUpdate, updateIndex int64) {
	g.log("Applying update: %v", update)

	g.lastUpdate = update
	g.lastUpdateIndex = updateIndex

	// Leaders not in the new configuration step down when the configuration is committed.
	if g.state == stateLeader && update.Phase == pb.ConfigUpdate_APPLIED && !in(g.Id, update.New) {
		g.leaving = true
	}

	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
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

	nextIndex := g.persistence.Append(g.unguardedCapturePersistedState(), entries)
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
		Old: g.lastUpdate.New,
	}

	for id, address := range existingNodes {
		if !contains(removedNodes, id) {
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
	g.unguardedAppendUpdate(update)
	return nil
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
