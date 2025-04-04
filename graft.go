package graft

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/grpc"
)

const unknownLeader = "UNKNOWN"

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
	currentTerm int
	votedFor    string
	commitIndex int
	lastApplied int

	// Leader-specific state.
	nextIndex  map[string]int
	matchIndex map[string]int
}

type Graft struct {
	raftState
	address        string
	id             string
	leaderId       string
	cluster        *cluster
	electionTimer  *periodicTimer ``
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

func (g *Graft) info(format string, vals ...any) {
	log.Printf("%s: %v\n", g.id, fmt.Sprintf(format, vals...))
}

func (g *Graft) fatal(err any) {
	log.Fatalf("Graft%v encountered a fatal error: %v\n", g, err)
}

type CommittedEntry struct {
	Index int
	Term  int
	Cmd   []byte
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
	if err := config.Persistence.SetState(state); err != nil {
		return nil, err
	}
	return &Graft{
		raftState: raftState{
			state:       Follower,
			currentTerm: int(state.CurrentTerm),
			votedFor:    state.VotedFor,
			commitIndex: int(state.CommitIndex),
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

func (g *Graft) Run() error {
	listener, err := net.Listen("tcp", g.address)
	if err != nil {
		return err
	}

	g.info("Listening on %v", listener.Addr())

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				return handler(context.WithValue(ctx, graftKey{}, g), req)
			}))
	pb.RegisterRaftServer(grpcServer, &g.server)
	pb.RegisterGraftServer(grpcServer, &gserver{})

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
		g.info("Election timed out too late, we are the leader now")
		return
	}

	if g.state == Candidate {
		g.info("Candidate election timed out")
	}

	g.unguardedTransitionToCandidate()

	// Request votes from peers.
	voteCount := 1 // Vote for self.
	electionTerm := g.currentTerm
	for _, p := range g.cluster.peers {
		go func(peer *peer) {
			if client, cerr := peer.client(); cerr != nil {
				g.info("Error connecting to peer %s: %v", peer.id, cerr)
			} else {
				var lastLogIndex int
				var lastLogTerm int
				requestVote := func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					if electionTerm != g.currentTerm {
						return false
					}

					entry, index, err := g.Persistence.TailEntry()
					if err != nil {
						g.fatal(err)
					}
					lastLogIndex = index
					if lastLogIndex >= 0 {
						lastLogTerm = int(entry.Term)
					} else {
						lastLogTerm = -1
					}
					return true
				}()

				if !requestVote {
					return
				}

				request := &pb.RequestVoteRequest{
					Term:         int32(electionTerm),
					CandidateId:  g.id,
					LastLogIndex: int32(lastLogIndex),
					LastLogTerm:  int32(lastLogTerm),
				}
				if res, rerr := client.RequestVote(context.Background(), request); rerr != nil {
					g.info("RequestVote to %s failed: %v", peer.id, rerr)
				} else {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.info("RequestVote(%v) to %s at election (%d) got {%v}, state=%v", request, peer.id, electionTerm, res, g)

					if res.Term > int32(g.currentTerm) {
						g.unguardedTransitionToFollower(int(res.Term))
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
	g.info("Transitioning to Leader, state=%v", g)
	g.state = Leader
	g.nextIndex = make(map[string]int)
	g.matchIndex = make(map[string]int)
	for id := range g.cluster.peers {
		g.nextIndex[id] = g.Persistence.EntryCount()
		g.matchIndex[id] = 0
	}
	g.leaderId = g.id
	g.electionTimer.stop()
	g.heartbeatTimer.poke() // Establish aurhotiry.
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.info("Transitioning to Candidate, state=%v", g)
	g.state = Candidate
	g.currentTerm++
	g.votedFor = g.id // Vote for self.
	g.leaderId = unknownLeader
	g.electionTimer.reset()
	g.heartbeatTimer.stop()
}

func (g *Graft) unguardedTransitionToFollower(term int) {
	g.info("Transitioning to Follower at (%d), state=%v", term, g)
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
				g.info("Error connecting to peer %s: %v", peer.id, cerr)
			} else {
				for {
					var prevLogIndex int
					var prevLogTerm int
					var entries []*pb.LogEntry
					var newNextIndex int
					broadcast := func() bool {
						g.mut.Lock()
						defer g.mut.Unlock()

						if witnessTerm != g.currentTerm {
							return false
						}

						nextIndex := g.nextIndex[peer.id]
						prevLogIndex = nextIndex - 1
						if prevLogIndex >= 0 {
							term, err := g.Persistence.GetEntryTerm(prevLogIndex)
							if err != nil {
								g.fatal(err)
							}
							prevLogTerm = term
						} else {
							prevLogTerm = -1
						}

						es, err := g.Persistence.GetEntriesFrom(nextIndex)
						if err != nil {
							g.fatal(err)
						}
						entries = es
						newNextIndex = nextIndex + len(entries)
						return true
					}()

					if !broadcast {
						return
					}

					retry := func() bool {
						request := &pb.AppendEntriesRequest{
							Term:              int32(witnessTerm),
							LeaderId:          me,
							PrevLogIndex:      int32(prevLogIndex),
							PrevLogTerm:       int32(prevLogTerm),
							Entries:           entries,
							LeaderCommitIndex: int32(commitIndex),
						}
						if res, rerr := client.AppendEntries(context.Background(), request); rerr != nil {
							g.info("AppendEntries to %s failed: %v", peer.id, rerr)
							return false
						} else {
							g.mut.Lock()
							defer g.mut.Unlock()

							g.info("AppendEntries(%v) to %s got {%v}, state=%v", request, peer.id, res, g)

							if res.Term > int32(g.currentTerm) {
								g.unguardedTransitionToFollower(int(res.Term))
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
				Index: i,
				Term:  int(entry.Term),
				Cmd:   entry.Command,
			})
		}
		g.commitIndex = newCommitIndex
		g.heartbeatTimer.poke() // Broadcast new commitIndex.
		g.committedChan <- committedEntries
	}
}

func (g *Graft) unguardedCapturePersistedState() *pb.PersistedState {
	return &pb.PersistedState{
		CurrentTerm: int32(g.currentTerm),
		VotedFor:    g.votedFor,
		CommitIndex: int32(g.commitIndex)}
}

func (g *Graft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.info("Received RequestVote(%v), state=%v", request, g)

	if request.Term < int32(g.currentTerm) {
		return &pb.RequestVoteResponse{
			Term:        int32(g.currentTerm),
			VoteGranted: false,
		}, nil
	}

	electionTimerIsReset := false
	if request.Term > int32(g.currentTerm) {
		g.unguardedTransitionToFollower(int(request.Term))
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
		Term:        int32(g.currentTerm),
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	var myLastLogTerm int
	entry, index, err := g.Persistence.TailEntry()
	if err != nil {
		g.fatal(err)
	}
	myLastLogIndex := index
	if myLastLogIndex >= 0 {
		myLastLogTerm = int(entry.Term)
	} else {
		myLastLogTerm = -1
	}
	return myLastLogTerm < int(request.LastLogTerm) ||
		(myLastLogTerm == int(request.LastLogTerm) && myLastLogIndex <= int(request.LastLogIndex))
}

func (g *Graft) appendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.info("Received AppendEntries(%v), state=%v", request, g)

	term := int(request.Term)
	if term < g.currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    int32(g.currentTerm),
			Success: false,
		}, nil
	}

	g.electionTimer.reset()
	if term > g.currentTerm {
		g.unguardedTransitionToFollower(term)
	}

	if g.leaderId != request.LeaderId {
		g.info("Changed leadership (%s -> %s), state=%v", g.leaderId, request.LeaderId, g)
		g.leaderId = request.LeaderId
	}

	if int(request.PrevLogIndex) >= g.Persistence.EntryCount() {
		return &pb.AppendEntriesResponse{
			Term:    int32(term),
			Success: false,
		}, nil
	}

	if request.PrevLogTerm >= 0 {
		myPrevLogTerm, err := g.Persistence.GetEntryTerm(int(request.PrevLogIndex))
		if err != nil {
			return nil, err // TODO make errChan
		}
		if request.PrevLogTerm != int32(myPrevLogTerm) {
			return &pb.AppendEntriesResponse{
				Term:    int32(term),
				Success: false,
			}, nil
		}
	}

	if request.PrevLogIndex >= 0 && int(request.PrevLogIndex) < g.Persistence.EntryCount()-1 {
		if err := g.Persistence.TruncatEntriesFrom(int(request.PrevLogIndex) + 1); err != nil {
			return nil, err // TODO make errChan
		}
	}

	nextIndex, err := g.Persistence.Append(g.unguardedCapturePersistedState(), request.Entries)
	if err != nil {
		return nil, err // TODO make errChan
	}
	lastIndex := nextIndex - 1
	if int(request.LeaderCommitIndex) > g.commitIndex {
		g.commitIndex = min(int(request.LeaderCommitIndex), lastIndex)
	}
	return &pb.AppendEntriesResponse{
		Term:    int32(term),
		Success: true,
	}, nil
}

func (g *Graft) Append(cmds [][]byte) (index int, term int, err error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return -1, g.currentTerm, nil
	}

	var entries []*pb.LogEntry
	for _, cmd := range cmds {
		entries = append(entries, &pb.LogEntry{Term: int32(g.currentTerm), Command: cmd})
	}
	lastIndex, err := g.Persistence.Append(g.unguardedCapturePersistedState(), entries)
	if err != nil {
		return g.Persistence.EntryCount(), g.currentTerm, err // TODO make errorChan
	}
	g.heartbeatTimer.poke() // Broadcast new entries.
	return lastIndex, g.currentTerm, err
}

type gserver struct {
	pb.UnimplementedGraftServer
}

func (s *gserver) Append(ctx context.Context, request *pb.AppendRequest) (*pb.AppendResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		g.fatal("unable to find Graft instance in context")
	}

	index, term, err := g.Append(request.Cmds)
	if err != nil {
		return nil, err
	}
	return &pb.AppendResponse{Index: int32(index), Term: int32(term)}, nil
}
