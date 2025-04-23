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
	id             string
	leaderId       string
	cluster        *cluster
	electionTimer  *periodicTimer
	heartbeatTimer *periodicTimer
	server         server
	mut            sync.Mutex
	commitChan     chan Commit
	appliedChan    chan int64
	electionChan   chan int64
	heartbeatChan  chan int64
	broadcastChans map[string]chan struct{}

	Persistence Persistence
	Commit      func(Commit)
}

func (g *Graft) String() string {
	return fmt.Sprintf(
		"{state: %v, currentTerm: %d, votedFor: %s, commitIndex: %d, lastApplied: %d, leaderId: %s, len(log)=%d}",
		g.state, g.currentTerm, g.votedFor, g.commitIndex, g.lastApplied, g.leaderId, g.Persistence.EntryCount())
}

func (g *Graft) log(format string, vals ...any) {
	log.Printf("%s: %v\n", g.id, fmt.Sprintf(format, vals...))
}

func (g *Graft) fatal(err any) {
	log.Fatalf("Graft%v encountered a fatal error: %v\n%s", g, err, debug.Stack())
}

type Commit interface {
	Entries() []*pb.LogEntry

	Applied(snapshotData []byte) error
}

type commit struct {
	g       *Graft
	entries []*pb.LogEntry
}

func (c *commit) Entries() []*pb.LogEntry {
	return c.entries
}

func (c *commit) Applied(snapshotData []byte) error {
	lastEntry := c.entries[len(c.entries)-1]

	var snapshot Snapshot
	if snapshotData != nil {
		snapshot = NewSnapshot(&pb.SnapshotMetadata{
			LastAppliedIndex: lastEntry.Index,
			LastAppliedTerm:  lastEntry.Term,
		}, snapshotData)
	}
	return c.g.applied(lastEntry.Index, snapshot)
}

type Config struct {
	Id                        string
	Addresses                 map[string]string
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
	HeartbeatMillis           int
	Persistence               Persistence
	Commit                    func(Commit)
}

func New(config Config) (*Graft, error) {
	addresses := config.Addresses
	myAddress := addresses[config.Id]
	delete(addresses, config.Id)

	state := config.Persistence.RetrieveState()
	if state == nil {
		state = &pb.PersistedState{CurrentTerm: 0, VotedFor: "", CommitIndex: -1}
	}
	if config.Id == UnknownLeader {
		return nil, fmt.Errorf("sever ID cannot be %s", UnknownLeader)
	}

	cluster := newCluster(addresses)

	broadcastChans := make(map[string]chan struct{})
	for peerId := range cluster.peers {
		broadcastChans[peerId] = make(chan struct{}, 1) // We only need an additional (keep_broadcasting) signal.
	}

	return &Graft{
		raftState: raftState{
			state:       Follower,
			currentTerm: state.CurrentTerm,
			votedFor:    state.VotedFor,
			commitIndex: state.CommitIndex,
			nextIndex:   make(map[string]int64),
			matchIndex:  make(map[string]int64),
			lastApplied: -1,
		},
		id:      config.Id,
		address: myAddress,
		cluster: cluster,
		electionTimer: newRandomizedTimer(
			time.Duration(config.ElectionTimeoutLowMillis)*time.Millisecond,
			time.Duration(config.ElectionTimeoutHighMillis)*time.Millisecond),
		heartbeatTimer: newTimer(time.Duration(config.HeartbeatMillis) * time.Millisecond),
		server:         server{},
		commitChan:     make(chan Commit, 1024), // Buffer the channel so that a slow client doesn't immediately block workflow.
		appliedChan:    make(chan int64),
		electionChan:   make(chan int64),
		heartbeatChan:  make(chan int64),
		broadcastChans: broadcastChans,
		Persistence:    config.Persistence,
		Commit:         config.Commit,
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

func (g *Graft) GetLastEntryIndex() int64 {
	g.mut.Lock()
	defer g.mut.Unlock()

	lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
	return lastIndex
}

func (g *Graft) Start() error {
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
	pb.RegisterRaftServer(grpcServer, &g.server)

	go g.commitWorker()
	go g.electionWorker()
	go g.heartbeatWorker()
	for _, peer := range g.cluster.peers {
		go g.broadcastWorker(peer, g.broadcastChans[peer.id])
	}

	g.electionTimer.start(func() {
		g.electionChan <- g.GetCurrentTerm()
	})
	g.heartbeatTimer.start(func() {
		g.heartbeatChan <- g.GetLastEntryIndex()
	})

	// Begin as follower.
	func() {
		g.mut.Lock()
		defer g.mut.Unlock()

		g.unguardedTransitionToFollower(g.currentTerm)
	}()

	errChan := make(chan error)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errChan <- err
		}
		close(errChan)
	}()
	return <-errChan
}

func (g *Graft) Close() {
	// TODO how to close.
}

func (g *Graft) commitWorker() {
	for commit := range g.commitChan {
		g.Commit(commit)
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
					lastLogIndex, lastLogTerm = g.Persistence.LastLogIndexAndTerm()
					return true
				}()

				if !requestVote {
					return
				}

				request := &pb.RequestVoteRequest{
					Term:         electionTerm,
					CandidateId:  g.id,
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
		if req := g.appendEntriesRequestIfLeader(peer.id); req != nil {
			if client, err := peer.client(); err != nil {
				g.log("Error connecting to peer %s: %v", peer.id, err)
			} else if res, err := client.AppendEntries(context.Background(), req); err != nil {
				g.log("AppendEntries to %s failed: %v", peer.id, err)
			} else {
				doRetry := func() bool {
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

				if doRetry {
					goto retry
				}
			}
		}
	}
}

func (g *Graft) appendEntriesRequestIfLeader(peerId string) *pb.AppendEntriesRequest {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return nil
	}

	request := &pb.AppendEntriesRequest{
		Term:              g.currentTerm,
		LeaderId:          g.id,
		LeaderCommitIndex: g.commitIndex,
	}

	nextIndex := g.nextIndex[peerId]
	prevLogIndex := nextIndex - 1
	firstIndex, _ := g.Persistence.FirstLogIndexAndTerm()
	if prevLogIndex >= firstIndex && firstIndex >= 0 {
		term, err := g.Persistence.GetEntryTerm(prevLogIndex)
		if err != nil {
			g.fatal(err)
		}
		request.PrevLogIndex, request.PrevLogTerm = prevLogIndex, term
	} else {
		request.PrevLogIndex, request.PrevLogTerm = -1, -1
	}

	lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
	if nextIndex <= lastIndex && nextIndex >= 0 {
		entries, err := g.Persistence.GetEntriesFrom(nextIndex)
		if err != nil {
			g.fatal(err)
		}
		request.Entries = entries
	}
	return request
}

func (g *Graft) unguardedTransitionToLeader() {
	g.log("Transitioning to Leader, state=%v", g)
	g.state = Leader
	for id := range g.cluster.peers {
		lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
		g.nextIndex[id] = lastIndex + 1
		g.matchIndex[id] = 0
	}
	g.leaderId = g.id
	g.electionTimer.stop()
	g.heartbeatTimer.poke() // Establish authority.
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.log("Transitioning to Candidate, state=%v", g)
	g.state = Candidate
	g.currentTerm++
	g.votedFor = g.id // Vote for self.
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
	lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
	for i := g.commitIndex + 1; i <= lastIndex; i++ { // Note that commitIndex is initialized to -1 initially.
		term, err := g.Persistence.GetEntryTerm(i)
		if err != nil {
			g.fatal(err)
		}

		// We must only commit entries from current term. See paper section 5.4.2.
		if term == g.currentTerm {
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
		entries, err := g.Persistence.GetEntries(g.commitIndex, newCommitIndex)
		if err != nil {
			g.fatal(err)
		}
		g.commitIndex = newCommitIndex
		g.commitChan <- &commit{
			g:       g,
			entries: cloneMsgs(entries),
		}
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
	}
}

func (g *Graft) unguardedCapturePersistedState() *pb.PersistedState {
	return &pb.PersistedState{
		CurrentTerm: g.currentTerm,
		VotedFor:    g.votedFor,
		CommitIndex: g.commitIndex,
	}
}

func (g *Graft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received RequestVote(%v), state=%v", request, g)

	if request.Term < g.currentTerm {
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

	if err := g.Persistence.SaveState(g.unguardedCapturePersistedState()); err != nil {
		return nil, err
	}
	return &pb.RequestVoteResponse{
		Term:        g.currentTerm,
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	myLastLogIndex, myLastLogTerm := g.Persistence.LastLogIndexAndTerm()

	// Note that this comparison works with the -1 sentinel values.
	return myLastLogTerm < request.LastLogTerm ||
		(myLastLogTerm == request.LastLogTerm && myLastLogIndex <= request.LastLogIndex)
}

func (g *Graft) appendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received AppendEntries(%v), state=%v", request, g)

	if request.Term < g.currentTerm {
		return &pb.AppendEntriesResponse{
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

	lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
	if request.PrevLogIndex > lastIndex {
		return &pb.AppendEntriesResponse{
			Term:    request.Term,
			Success: false,
		}, nil
	}

	firstIndex, _ := g.Persistence.FirstLogIndexAndTerm()
	if request.PrevLogIndex >= firstIndex && firstIndex >= 0 {
		myPrevLogTerm, err := g.Persistence.GetEntryTerm(request.PrevLogIndex)
		if err != nil {
			return nil, err
		}
		if request.PrevLogTerm != myPrevLogTerm {
			return &pb.AppendEntriesResponse{
				Term:    request.Term,
				Success: false,
			}, nil
		}
	}

	if request.PrevLogIndex+1 <= lastIndex {
		if err := g.Persistence.TruncateEntriesFrom(request.PrevLogIndex + 1); err != nil {
			return nil, err
		}
	}

	nextIndex, err := g.Persistence.Append(g.unguardedCapturePersistedState(), request.Entries)
	if err != nil {
		g.fatal(err)
	}
	if request.LeaderCommitIndex > g.commitIndex {
		newCommitIndex := min(request.LeaderCommitIndex, nextIndex-1)
		if g.commitIndex != newCommitIndex {
			entries, err := g.Persistence.GetEntries(g.commitIndex, newCommitIndex)
			if err != nil {
				g.log("Failed to get entries for committing: %v", err)
			} else {
				g.commitIndex = newCommitIndex
				g.commitChan <- &commit{
					g:       g,
					entries: cloneMsgs(entries),
				}
			}
		}
	}
	return &pb.AppendEntriesResponse{
		Term:    request.Term,
		Success: true,
	}, nil
}

func (g *Graft) Append(commands [][]byte) (int64, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return -1, errNotLeader
	}

	if len(commands) == 0 {
		lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
		return lastIndex, nil
	}

	entries, err := g.Persistence.AppendCommands(g.unguardedCapturePersistedState(), commands)
	if err != nil {
		return -1, err
	}
	g.heartbeatTimer.poke() // Broadcast new entries.
	return entries[len(entries)-1].Index, err
}

func (g *Graft) applied(lastIndex int64, snapshot Snapshot) error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if lastIndex > g.lastApplied {
		g.log("Applied %d with snapshot %v", lastIndex, snapshot)

		g.lastApplied = lastIndex
		if snapshot != nil {
			return g.Persistence.SaveSnapshot(snapshot)
		}
	}
	return nil
}
