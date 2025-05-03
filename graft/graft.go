package graft

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/mizosoft/graft/graftpb"
	"github.com/mizosoft/graft/raftpb"
	"google.golang.org/grpc"
)

const UnknownLeader = "UNKNOWN"

type state int

const (
	Follower state = iota
	Candidate
	Leader
)

var (
	errNotLeader = errors.New("not leader")
)

func (s state) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		log.Fatalf("Unknown state: %d", int(s))
		return "" // Unreachable
	}
}

type raftState struct {
	state       state
	currentTerm int64
	votedFor    string
	commitIndex int64
	lastApplied int64

	// Leader-specific state.
	nextIndex  map[string]int64
	matchIndex map[string]int64
}

type Graft struct {
	_ uncopyable

	raftState
	address        string
	leaderId       string
	cluster        *cluster
	electionTimer  *periodicTimer
	heartbeatTimer *periodicTimer
	server         server
	persistence    Persistence
	mut            sync.Mutex
	applyChan      chan any
	electionChan   chan int64
	heartbeatChan  chan struct{}
	broadcastChans map[string]chan struct{}
	initialized    bool

	Id      string
	Restore func(snapshot Snapshot)
	Apply   func(entries []*raftpb.LogEntry) (snapshot []byte)
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
	addresses := config.Addresses
	myAddress := addresses[config.Id]
	delete(addresses, config.Id)

	if config.Id == UnknownLeader {
		return nil, fmt.Errorf("sever ID cannot be %s", UnknownLeader)
	}

	cluster := newCluster(addresses)

	broadcastChans := make(map[string]chan struct{})
	for peerId := range cluster.peers {
		broadcastChans[peerId] = make(chan struct{}, 1) // We only need an additional (keep_broadcasting) signal.
	}

	return &Graft{
		Id: config.Id,
		raftState: raftState{
			state:       Follower,
			currentTerm: 0,
			votedFor:    UnknownLeader,
			commitIndex: -1,
			nextIndex:   make(map[string]int64),
			matchIndex:  make(map[string]int64),
			lastApplied: -1,
		},
		address: myAddress,
		cluster: cluster,
		electionTimer: newRandomizedTimer(
			time.Duration(config.ElectionTimeoutLowMillis)*time.Millisecond,
			time.Duration(config.ElectionTimeoutHighMillis)*time.Millisecond),
		heartbeatTimer: newTimer(time.Duration(config.HeartbeatMillis) * time.Millisecond),
		server:         server{},
		applyChan:      make(chan any, 1024), // Buffer the channel so that a slow client doesn't immediately block workflow.
		electionChan:   make(chan int64),
		heartbeatChan:  make(chan struct{}),
		broadcastChans: broadcastChans,
		persistence:    config.Persistence,
	}, nil
}

type graftKey struct{}

// TODO add some running & closed booleans.
// TODO make read failures reacoverable, but write failures fatal.

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

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				return handler(context.WithValue(ctx, graftKey{}, g), req)
			}))
	raftpb.RegisterRaftServer(grpcServer, &g.server)
	return grpcServer.Serve(listener)
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
	for _, peer := range g.cluster.peers {
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
		state = &graftpb.PersistedState{
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
	commitIndex := g.commitIndex // Only apply upto committed entries.
	if firstIndex >= 0 && commitIndex >= firstIndex {
		commandEntries := toCommandEntries(g.persistence.GetEntries(firstIndex, commitIndex))
		if len(commandEntries) > 0 {
			g.Apply(commandEntries)
		}
	}
}

func (g *Graft) Close() {
	// TODO how to close.
}

func (g *Graft) applyWorker() {
	for e := range g.applyChan {
		switch e := e.(type) {
		case []*raftpb.LogEntry:
			snapshotData := g.Apply(e)
			g.lastApplied = e[len(e)-1].Index
			if snapshotData != nil {
				// Run in background.
				go func() {
					g.mut.Lock()
					defer g.mut.Unlock()

					metadata := &graftpb.SnapshotMetadata{
						LastAppliedIndex: g.lastApplied,
						LastAppliedTerm:  e[len(e)-1].Term,
					}
					g.persistence.SaveSnapshot(NewSnapshot(metadata, snapshotData))
					g.persistence.TruncateEntriesTo(metadata.LastAppliedIndex)
				}()
			}
		case Snapshot:
			g.Restore(e)
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

	if g.state == Leader {
		// We might've already won an election for this term but couldn't stop the timer on time.
		g.log("Election timed out too late, we are the leader now")
		return
	}

	if g.state == Candidate {
		g.log("Candidate election timed out")
	}

	g.unguardedTransitionToCandidate()

	// Request votes from peers.
	voteCount := 1 // Vote for self.
	electionTerm := timeoutTerm + 1
	for _, peer := range g.cluster.peers {
		go func() {
			if client, cerr := peer.client(); cerr != nil {
				g.log("Error connecting to peer %s: %v", peer.id, cerr)
			} else {
				var lastLogIndex int64
				var lastLogTerm int64
				requestVote := func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					if g.currentTerm != electionTerm {
						return false
					}
					lastLogIndex, lastLogTerm = g.persistence.LastLogIndexAndTerm()
					if lastLogIndex < 0 {
						if metadata := g.persistence.SnapshotMetadata(); metadata != nil {
							lastLogIndex, lastLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedTerm
						}
					}
					return true
				}()

				if !requestVote {
					return
				}

				request := &raftpb.RequestVoteRequest{
					Term:         electionTerm,
					CandidateId:  g.Id,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				if res, rerr := client.RequestVote(context.Background(), request); rerr != nil {
					g.log("RequestVote to %s failed: %v", peer.id, rerr)
				} else {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.log("RequestVote(%v) to %s at election (%d) got {%v}, state=%v", request, peer.id, electionTerm, res, g)

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
					} else if res.VoteGranted && g.currentTerm == electionTerm && g.state == Candidate { // Check that the election isn't invalidated.
						voteCount++
						if voteCount >= g.cluster.majorityCount() {
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

	if g.state == Leader {
		g.heartbeatTimer.reset()
		for _, c := range g.broadcastChans {
			select {
			case c <- struct{}{}:
			default:
				// Ignore: c has a capacity of 1 so a new AppendEntries is surely running in the future and that is all
				// what we need to know.
			}
		}
	}
}

// TODO will it be of practical benefit to send log entries in batches if there are many?
func (g *Graft) broadcastWorker(peer *peer, broadcastChan chan struct{}) {
	for range broadcastChan {
	retry:
		req := func() proto.Message {
			g.mut.Lock()
			defer g.mut.Unlock()

			if g.state != Leader {
				return nil
			}

			nextIndex := g.nextIndex[peer.id]
			firstIndex, _ := g.persistence.FirstLogIndexAndTerm()

			if nextIndex >= firstIndex {
				request := &raftpb.AppendEntriesRequest{
					Term:              g.currentTerm,
					LeaderId:          g.Id,
					LeaderCommitIndex: g.commitIndex,
				}

				prevLogIndex := nextIndex - 1
				if prevLogIndex >= firstIndex && firstIndex >= 0 {
					request.PrevLogIndex, request.PrevLogTerm = prevLogIndex, g.persistence.GetEntryTerm(prevLogIndex)
				} else if metadata := g.persistence.SnapshotMetadata(); metadata != nil && prevLogIndex >= 0 {
					request.PrevLogIndex, request.PrevLogTerm = metadata.LastAppliedIndex, metadata.LastAppliedIndex
				} else if prevLogIndex >= 0 {
					log.Panicf("Valid prevLogIndex but there is not snapshot: %d", prevLogIndex)
				} else {
					request.PrevLogIndex, request.PrevLogTerm = -1, -1
				}

				lastIndex, _ := g.persistence.LastLogIndexAndTerm()
				if nextIndex <= lastIndex && nextIndex >= 0 {
					request.Entries = g.persistence.GetEntriesFrom(nextIndex)
				}
				return request
			} else if snapshot := g.persistence.RetrieveSnapshot(); snapshot != nil {
				return &raftpb.SnapshotRequest{
					Term:              g.currentTerm,
					LeaderId:          g.Id,
					LastIncludedTerm:  snapshot.Metadata().LastAppliedTerm,
					LastIncludedIndex: snapshot.Metadata().LastAppliedIndex,
					Offset:            0,
					Done:              true,
					Data:              snapshot.Data(),
				}
			} else {
				log.Panicf("Next index of %s is too far back (%d) and there is not snapshot", peer.id, nextIndex)
				return nil // Unreachable.
			}
		}()

		if req == nil {
			continue
		}

		if client, err := peer.client(); err != nil {
			g.log("Error connecting to peer %s: %v", peer.id, err)
		} else {
			res := func() proto.Message {
				switch req := req.(type) {
				case *raftpb.AppendEntriesRequest:
					if res, err := client.AppendEntries(context.Background(), req); err != nil {
						g.log("AppendEntries to %s failed: %v", peer.id, err)
						return nil
					} else {
						return res
					}
				case *raftpb.SnapshotRequest:
					if res, err := client.InstallSnapshot(context.Background(), req); err != nil {
						g.log("InstallSnapshot to %s failed: %v", peer.id, err)
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
			case *raftpb.AppendEntriesResponse:
				doRetry = func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.log("AppendEntries(%v) to %s got {%v}, state=%v", req, peer.id, res, g)

					if g.state != Leader {
						return false
					}

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
						return false
					} else if res.Success {
						req := req.(*raftpb.AppendEntriesRequest)
						if len(req.Entries) > 0 {
							g.nextIndex[peer.id] = req.Entries[len(req.Entries)-1].Index + 1
							g.matchIndex[peer.id] = g.nextIndex[peer.id] - 1
							g.unguardedFlushCommittedEntries()
						}
						return false
					} else {
						g.nextIndex[peer.id]--
						return true
					}
				}()

			case *raftpb.SnapshotResponse:
				doRetry = func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.log("AppendEntries(%v) to %s got {%v}, state=%v", req, peer.id, res, g)

					if g.state != Leader {
						return false
					}

					if res.Term > g.currentTerm {
						g.unguardedTransitionToFollower(res.Term)
						return false
					} else {
						req := req.(*raftpb.SnapshotRequest)
						g.nextIndex[peer.id] = req.LastIncludedIndex + 1
						return true // Continue emitting log.
					}
				}()
			}

			if doRetry {
				goto retry
			}
		}
	}
}

func (g *Graft) appendEntriesRequestIfLeader(peerId string) *raftpb.AppendEntriesRequest {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return nil
	}

	request := &raftpb.AppendEntriesRequest{
		Term:              g.currentTerm,
		LeaderId:          g.Id,
		LeaderCommitIndex: g.commitIndex,
	}

	nextIndex := g.nextIndex[peerId]
	prevLogIndex := nextIndex - 1

	firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
	if prevLogIndex >= firstIndex && firstIndex >= 0 {
		request.PrevLogIndex, request.PrevLogTerm = prevLogIndex, g.persistence.GetEntryTerm(prevLogIndex)
	} else {
		request.PrevLogIndex, request.PrevLogTerm = -1, -1
	}

	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
	if nextIndex <= lastIndex && nextIndex >= 0 {
		request.Entries = g.persistence.GetEntriesFrom(nextIndex)
	}
	return request
}

func (g *Graft) unguardedTransitionToLeader() {
	g.log("Transitioning to Leader, state=%v", g)
	g.state = Leader

	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
	for id := range g.cluster.peers {
		g.nextIndex[id] = lastIndex + 1
		g.matchIndex[id] = 0
	}

	g.leaderId = g.Id
	g.electionTimer.stop()

	// Append NOOP entry if we have uncommitted entries from previous terms.
	if g.commitIndex < lastIndex {
		g.persistence.Append(g.unguardedCapturePersistedState(), []*raftpb.LogEntry{
			{
				Term: g.currentTerm,
				Type: raftpb.LogEntry_NOOP,
			},
		})
	}

	g.heartbeatTimer.poke() // Establish authority.
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.log("Transitioning to Candidate, state=%v", g)
	g.state = Candidate
	g.currentTerm++
	g.votedFor = g.Id // Vote for self.
	g.leaderId = UnknownLeader
	g.electionTimer.reset()
	g.heartbeatTimer.stop()
}

func (g *Graft) unguardedTransitionToFollower(term int64) {
	g.log("Transitioning to Follower at (%d), state=%v", term, g)
	g.state = Follower
	g.currentTerm = term
	g.votedFor = ""
	g.electionTimer.reset()
	g.heartbeatTimer.stop() // If we were a leader.
}

func (g *Graft) unguardedFlushCommittedEntries() {
	newCommitIndex := g.commitIndex
	lastIndex, _ := g.persistence.LastLogIndexAndTerm()
	for i := g.commitIndex + 1; i <= lastIndex; i++ { // Note that commitIndex is initialized to -1 initially.
		// We must only commit entries from current term. See paper section 5.4.2.
		if g.persistence.GetEntryTerm(i) == g.currentTerm {
			matchCount := 1 // Count self.
			for peerId := range g.cluster.peers {
				if g.matchIndex[peerId] >= i {
					matchCount++
				}

				if matchCount >= g.cluster.majorityCount() {
					newCommitIndex = i
					break
				}
			}
		}
	}

	if g.commitIndex != newCommitIndex {
		prevCommitIndex := g.commitIndex
		g.commitIndex = newCommitIndex
		g.unguardedApply(prevCommitIndex+1, newCommitIndex)
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
	}
}

func (g *Graft) unguardedApply(from int64, to int64) {
	if from < 0 {
		from, _ = g.persistence.FirstLogIndexAndTerm()
	}

	if from > to {
		return
	}

	commandEntries := toCommandEntries(g.persistence.GetEntries(from, to))
	if len(commandEntries) > 0 {
		g.applyChan <- cloneMsgs(commandEntries)
	}
}

func toCommandEntries(entries []*raftpb.LogEntry) []*raftpb.LogEntry {
	commandEntries := make([]*raftpb.LogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Type == raftpb.LogEntry_COMMAND {
			commandEntries = append(commandEntries, entry)
		}
	}
	return commandEntries
}

func (g *Graft) unguardedCapturePersistedState() *graftpb.PersistedState {
	return &graftpb.PersistedState{
		CurrentTerm: g.currentTerm,
		VotedFor:    g.votedFor,
		CommitIndex: g.commitIndex,
	}
}

func (g *Graft) requestVote(request *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received RequestVote(%v), state=%v", request, g)

	if request.Term < g.currentTerm {
		return &raftpb.RequestVoteResponse{
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

	g.persistence.SaveState(g.unguardedCapturePersistedState())
	return &raftpb.RequestVoteResponse{
		Term:        g.currentTerm,
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *raftpb.RequestVoteRequest) bool {
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

func (g *Graft) appendEntries(request *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received AppendEntries(%v), state=%v", request, g)

	if request.Term < g.currentTerm {
		return &raftpb.AppendEntriesResponse{
			Term:    g.currentTerm,
			Success: false,
		}, nil
	}

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
		return &raftpb.AppendEntriesResponse{
			Term:    request.Term,
			Success: false,
		}, nil
	}

	firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
	if request.PrevLogIndex >= firstIndex && firstIndex >= 0 {
		myPrevLogTerm := g.persistence.GetEntryTerm(request.PrevLogIndex)
		if request.PrevLogTerm != myPrevLogTerm {
			g.persistence.SaveState(g.unguardedCapturePersistedState())
			return &raftpb.AppendEntriesResponse{
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
		if g.commitIndex != newCommitIndex {
			prevCommitIndex := g.commitIndex
			g.commitIndex = newCommitIndex
			g.unguardedApply(prevCommitIndex+1, newCommitIndex)
		}
	}

	return &raftpb.AppendEntriesResponse{
		Term:    request.Term,
		Success: true,
	}, nil
}

func (g *Graft) installSnapshot(request *raftpb.SnapshotRequest) (*raftpb.SnapshotResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received InstallSnapshot(%v), state=%v", request, g)

	if request.Term < g.currentTerm {
		return &raftpb.SnapshotResponse{
			Term: g.currentTerm,
		}, nil
	}

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

	snapshot := NewSnapshot(&graftpb.SnapshotMetadata{
		LastAppliedIndex: request.LastIncludedIndex,
		LastAppliedTerm:  request.LastIncludedTerm,
	}, request.Data)
	g.persistence.SaveSnapshot(snapshot)

	firstIndex, _ := g.persistence.FirstLogIndexAndTerm()
	if request.LastIncludedIndex >= firstIndex {
		g.persistence.TruncateEntriesTo(request.LastIncludedIndex)
	} else if firstIndex >= 0 {
		g.persistence.TruncateEntriesFrom(firstIndex) // Truncate entire log.
	}

	g.applyChan <- snapshot
	return &raftpb.SnapshotResponse{
		Term: g.currentTerm,
	}, nil
}

func (g *Graft) Append(commands [][]byte) (int64, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return -1, errNotLeader
	}

	if len(commands) == 0 {
		return -1, nil
	}

	entries := make([]*raftpb.LogEntry, len(commands))
	for i, cmd := range commands {
		entries[i] = &raftpb.LogEntry{
			Term:    g.currentTerm,
			Command: cmd,
			Type:    raftpb.LogEntry_COMMAND,
		}
	}

	nextIndex := g.persistence.Append(g.unguardedCapturePersistedState(), entries)
	g.heartbeatTimer.poke() // Broadcast new entries.
	return nextIndex - int64(len(entries)), nil
}
