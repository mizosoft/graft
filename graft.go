package graft

import (
	"context"
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
	raftState
	address        string
	id             string
	leaderId       string
	cluster        *cluster
	electionTimer  *periodicTimer
	heartbeatTimer *periodicTimer
	server         server
	mut            sync.Mutex
	committedChan  chan []CommittedEntry

	Persistence Persistence
	Committed   func([]CommittedEntry)
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

type CommittedEntry struct {
	Index   int64
	Term    int64
	Command []byte
}

type Config struct {
	Id                        string
	Addresses                 map[string]string
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
	HeartbeatMillis           int
	Persistence               Persistence
	Committed                 func([]CommittedEntry)
}

func New(config Config) (*Graft, error) {
	addresses := config.Addresses
	myAddress := addresses[config.Id]
	delete(addresses, config.Id)
	state := config.Persistence.GetState()
	if state == nil {
		state = &pb.PersistedState{CurrentTerm: 0, VotedFor: "", CommitIndex: -1}
	}
	if config.Id == UnknownLeader {
		return nil, fmt.Errorf("sever ID cannot be %s", UnknownLeader)
	}

	return &Graft{
		raftState: raftState{
			state:       Follower,
			currentTerm: state.CurrentTerm,
			votedFor:    state.VotedFor,
			commitIndex: state.CommitIndex,
			lastApplied: -1,
		},
		id:      config.Id,
		address: myAddress,
		cluster: newCluster(addresses),
		electionTimer: newRandomizedTimer(
			time.Duration(config.ElectionTimeoutLowMillis)*time.Millisecond,
			time.Duration(config.ElectionTimeoutHighMillis)*time.Millisecond),
		heartbeatTimer: newTimer(time.Duration(config.HeartbeatMillis) * time.Millisecond),
		server:         server{},
		committedChan:  make(chan []CommittedEntry, 1024), // Buffer the channel so that a slow client doesn't immediately block workflow.
		Committed:      config.Committed,
		Persistence:    config.Persistence,
	}, nil
}

type graftKey struct{}

// TODO add some running & closed booleans.
// TODO make read failures reacoverable, but write failures fatal.

func (g *Graft) Serve() error {
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

	g.electionTimer.start(g.triggerElectionTimeout)
	g.heartbeatTimer.start(g.triggerHeartbeat)
	g.electionTimer.reset()

	go func() {
		for {
			committed, ok := <-g.committedChan
			if !ok {
				return
			}
			g.Committed(committed)
		}
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

func (g *Graft) triggerElectionTimeout() {
	g.mut.Lock()
	defer g.mut.Unlock()

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
	electionTerm := g.currentTerm
	for _, p := range g.cluster.peers {
		go func(peer *peer) {
			if client, cerr := peer.client(); cerr != nil {
				g.log("Error connecting to peer %s: %v", peer.id, cerr)
			} else {
				var lastLogIndex int64
				var lastLogTerm int64
				requestVote := func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					if electionTerm != g.currentTerm {
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
		}(p)
	}
}

func (g *Graft) triggerHeartbeat() {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state == Leader {
		g.heartbeatTimer.reset()
		g.unguardedBroadcast()
	}
}

func (g *Graft) unguardedTransitionToLeader() {
	g.log("Transitioning to Leader, state=%v", g)
	g.state = Leader
	g.nextIndex = make(map[string]int64)
	g.matchIndex = make(map[string]int64)
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

// TODO will it be of practical benefit to send log entries in batches if there are many?
func (g *Graft) unguardedBroadcast() {
	me := g.id
	witnessTerm := g.currentTerm
	commitIndex := g.commitIndex
	for _, p := range g.cluster.peers {
		go func(peer *peer) {
			if client, cerr := peer.client(); cerr != nil {
				g.log("Error connecting to peer %s: %v", peer.id, cerr)
			} else {
				for {
					var prevLogIndex int64
					var prevLogTerm int64
					var entries []*pb.LogEntry
					var newNextIndex int64
					broadcast := func() bool {
						g.mut.Lock()
						defer g.mut.Unlock()

						if witnessTerm != g.currentTerm {
							return false
						}

						nextIndex := g.nextIndex[peer.id]
						prevLogIndex = nextIndex - 1
						if prevLogIndex >= 0 {
							if term, err := g.Persistence.GetEntryTerm(prevLogIndex); err != nil {
								g.fatal(err)
							} else {
								prevLogTerm = term
							}
						} else {
							prevLogTerm = -1
						}

						lastIndex, _ := g.Persistence.LastLogIndexAndTerm()
						fmt.Printf("here: %d - %d\n", nextIndex, lastIndex)
						if nextIndex > 0 && nextIndex <= lastIndex {
							if es, err := g.Persistence.GetEntriesFrom(nextIndex); err != nil {
								g.fatal(err)
							} else {
								entries = es
								newNextIndex = nextIndex + int64(len(entries))
							}
						}
						return true
					}()

					if !broadcast {
						return
					}

					retry := func() bool {
						request := &pb.AppendEntriesRequest{
							Term:              witnessTerm,
							LeaderId:          me,
							PrevLogIndex:      prevLogIndex,
							PrevLogTerm:       prevLogTerm,
							Entries:           entries,
							LeaderCommitIndex: commitIndex,
						}
						if res, rerr := client.AppendEntries(context.Background(), request); rerr != nil {
							g.log("AppendEntries to %s failed: %v", peer.id, rerr)
							return false
						} else {
							g.mut.Lock()
							defer g.mut.Unlock()

							g.log("AppendEntries(%v) to %s got {%v}, state=%v", request, peer.id, res, g)

							if res.Term > g.currentTerm {
								g.unguardedTransitionToFollower(res.Term)
								return false
							} else if witnessTerm != g.currentTerm {
								return false
							} else if res.Success {
								g.nextIndex[peer.id] = newNextIndex
								g.matchIndex[peer.id] = newNextIndex - 1
								g.unguardedFlushCommittedEntries()
								return false
							} else {
								g.nextIndex[peer.id]--
								return true
							}
						}
					}()

					if !retry {
						break
					}
				}
			}
		}(p)
	}
}

func (g *Graft) unguardedFlushCommittedEntries() {
	newCommitIndex := g.commitIndex
	for i := max(0, g.commitIndex); i < g.Persistence.EntryCount(); i++ {
		// We must only commit entries from current term. See paper section 5.4.2.
		term, err := g.Persistence.GetEntryTerm(i)
		if err != nil {
			g.fatal(err)
		}
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
		var committedEntries []CommittedEntry
		for i := g.commitIndex + 1; i <= newCommitIndex; i++ {
			entry, err := g.Persistence.GetEntry(i)
			if err != nil {
				g.fatal(err)
			}
			committedEntries = append(committedEntries, CommittedEntry{
				Index:   i,
				Term:    entry.Term,
				Command: entry.Command,
			})
		}
		g.commitIndex = newCommitIndex
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
		g.committedChan <- committedEntries
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

	if err := g.Persistence.SetState(g.unguardedCapturePersistedState()); err != nil {
		return nil, err
	}
	return &pb.RequestVoteResponse{
		Term:        g.currentTerm,
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	myLastLogIndex, myLastLogTerm := g.Persistence.LastLogIndexAndTerm()
	return myLastLogTerm < request.LastLogTerm ||
		(myLastLogTerm == request.LastLogTerm && myLastLogIndex <= request.LastLogIndex)
}

func (g *Graft) appendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.log("Received AppendEntries(%v), state=%v", request, g)

	term := request.Term
	if term < g.currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    g.currentTerm,
			Success: false,
		}, nil
	}

	if term > g.currentTerm {
		g.unguardedTransitionToFollower(term)
	} else {
		g.electionTimer.reset()
	}

	if g.leaderId != request.LeaderId {
		g.log("Changed leadership (%s -> %s), state=%v", g.leaderId, request.LeaderId, g)
		g.leaderId = request.LeaderId
	}

	if request.PrevLogIndex >= g.Persistence.EntryCount() {
		return &pb.AppendEntriesResponse{
			Term:    term,
			Success: false,
		}, nil
	}

	if request.PrevLogTerm >= 0 {
		myPrevLogTerm, err := g.Persistence.GetEntryTerm(request.PrevLogIndex)
		if err != nil {
			return nil, err
		}
		if request.PrevLogTerm != myPrevLogTerm {
			return &pb.AppendEntriesResponse{
				Term:    term,
				Success: false,
			}, nil
		}
	}

	if request.PrevLogIndex >= 0 && request.PrevLogIndex < g.Persistence.EntryCount()-1 {
		if err := g.Persistence.TruncateEntriesFrom(request.PrevLogIndex + 1); err != nil {
			return nil, err
		}
	}

	nextIndex, err := g.Persistence.Append(g.unguardedCapturePersistedState(), request.Entries)
	if err != nil {
		g.fatal(err)
	}
	lastIndex := nextIndex - 1
	if request.LeaderCommitIndex > g.commitIndex {
		g.commitIndex = min(request.LeaderCommitIndex, lastIndex)
	}
	return &pb.AppendEntriesResponse{
		Term:    term,
		Success: true,
	}, nil
}

type NotLeaderError struct {
	Id       string
	LeaderId string
}

func (e NotLeaderError) Error() string {
	return fmt.Sprintf("we (%s) are not the leader, ask (%s)", e.Id, e.LeaderId)
}

func (g *Graft) Append(cmds [][]byte) ([]*pb.LogEntry, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return nil, NotLeaderError{Id: g.id, LeaderId: g.leaderId}
	}

	if len(cmds) == 0 {
		return []*pb.LogEntry{}, nil
	}

	entries, err := g.Persistence.AppendCommands(g.unguardedCapturePersistedState(), cmds)
	if err != nil {
		return nil, err
	}
	g.heartbeatTimer.poke() // Broadcast new entries.
	return entries, err
}
