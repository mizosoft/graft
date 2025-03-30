package graft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/mizosoft/graft/pb"
	grpc "google.golang.org/grpc"
)

type state int

const (
	Follower state = iota
	Candidate
	Leader
)

type raftState struct {
	state       state
	currentTerm int
	votedFor    string
	log         []*pb.LogEntry
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
	electionTimer  *periodicTimer
	heartbeatTimer *periodicTimer
	server         server
	mut            sync.Mutex

	Committed func([]CommittedEntry)
}

func (g *Graft) String() string {
	return fmt.Sprintf(
		"{state: %v, currentTerm: %d, votedFor: %s, commitIndex: %d, lastApplied: %d, leaderId: %s, len(log)=%d}",
		g.state, g.currentTerm, g.votedFor, g.commitIndex, g.lastApplied, g.leaderId, len(g.log))
}

func (s state) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic(fmt.Sprintf("Unknown state: %d", int(s)))
	}
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
}

func New(config Config) *Graft {
	addresses := config.Addresses
	myAddress := addresses[config.Id]
	delete(addresses, config.Id)
	g := &Graft{
		raftState: raftState{
			state:       Follower,
			currentTerm: 0,
			votedFor:    nilString,
			log:         []*pb.LogEntry{},
			commitIndex: -1,
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
		Committed:      func([]CommittedEntry) {},
	}
	return g
}

type graftKey struct{}

func (g *Graft) printf(format string, vals ...any) {
	fmt.Printf("%s: %v\n", g.id, fmt.Sprintf(format, vals...))
}

// TODO add some running & closed booleans.
func (g *Graft) Run() error {
	listener, err := net.Listen("tcp", g.address)
	if err != nil {
		return err
	}

	g.printf("Listening on %v", listener.Addr())

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
		g.printf("Election timed out too late, we are the leader now")
		return
	}

	if g.state == Candidate {
		g.printf("Candidate election timed out")
	}

	g.unguardedTransitionToCandidate()

	// Request votes from peers.
	voteCount := 1 // Vote for self.
	electionTerm := g.currentTerm
	for _, p := range g.cluster.peers {
		go func(peer *peer) {
			if client, cerr := peer.client(); cerr != nil {
				g.printf("Error connecting to peer %s: %v", peer.id, cerr)
			} else {
				var lastLogIndex int
				var lastLogTerm int
				requestVote := func() bool {
					g.mut.Lock()
					defer g.mut.Unlock()

					if electionTerm != g.currentTerm {
						return false
					}

					lastLogIndex = len(g.log) - 1
					if lastLogIndex >= 0 {
						lastLogTerm = int(g.log[lastLogIndex].Term)
					} else {
						lastLogTerm = -1
					}
					return true
				}()

				if !requestVote {
					return
				}

				if res, rerr := client.RequestVote(
					context.Background(),
					&pb.RequestVoteRequest{
						Term:         int32(electionTerm),
						CandidateId:  g.id,
						LastLogIndex: int32(lastLogIndex),
						LastLogTerm:  int32(lastLogTerm),
					}); rerr != nil {
					g.printf("RequestVote to %s failed: %v", peer.id, rerr)
				} else {
					g.mut.Lock()
					defer g.mut.Unlock()

					g.printf("RequestVote to %s at election (%d) got {%v}, state=%v", peer.id, electionTerm, res, g)

					if res.Term > int32(g.currentTerm) {
						g.unguardedTransitionToFollower(int(res.Term), unknown, false)
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
	g.printf("Transitioning to Leader, state=%v", g)
	g.state = Leader
	g.nextIndex = make(map[string]int)
	g.matchIndex = make(map[string]int)
	for id := range g.cluster.peers {
		g.nextIndex[id] = len(g.log)
		g.matchIndex[id] = 0
	}
	g.leaderId = g.id
	g.electionTimer.stop()
	g.heartbeatTimer.poke()
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.printf("Transitioning to Candidate, state=%v", g)
	g.state = Candidate
	g.currentTerm++
	g.votedFor = g.id // Vote for self.
	g.leaderId = unknown
	g.electionTimer.reset()
	g.heartbeatTimer.stop()
}

func (g *Graft) unguardedTransitionToFollower(term int, leaderId string, forceResetElectionTimer bool) {
	g.printf("Transitioning to Follower of (%v -> %v) at (%d), state=%v", g.leaderId, leaderId, term, g)
	prevState := g.state
	g.state = Follower
	g.currentTerm = term
	g.leaderId = leaderId
	g.votedFor = nilString
	if prevState != Follower || forceResetElectionTimer {
		g.electionTimer.reset()
	}
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
				g.printf("Error connecting to peer %s: %v", peer.id, cerr)
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
							prevLogTerm = int(g.log[prevLogIndex].Term)
						} else {
							prevLogTerm = -1
						}
						entries = g.log[nextIndex:]
						newNextIndex = len(g.log)
						return true
					}()

					if !broadcast {
						return
					}

					retry := func() bool {
						if res, rerr := client.AppendEntries(
							context.Background(),
							&pb.AppendEntriesRequest{
								Term:              int32(witnessTerm),
								LeaderId:          me,
								PrevLogIndex:      int32(prevLogIndex),
								PrevLogTerm:       int32(prevLogTerm),
								Entries:           entries,
								LeaderCommitIndex: int32(commitIndex),
							}); rerr != nil {
							g.printf("AppendEntries to %s failed: %v", peer.id, rerr)
							return false
						} else {
							g.mut.Lock()
							defer g.mut.Unlock()

							g.printf("AppendEntries to %s got {%v}, state=%v", peer.id, res, g)

							if res.Term > int32(g.currentTerm) {
								g.unguardedTransitionToFollower(int(res.Term), unknown, false)
								return false
							} else if witnessTerm != g.currentTerm {
								return false
							} else if res.Success {
								g.nextIndex[peer.id] = newNextIndex
								g.matchIndex[peer.id] = newNextIndex - 1

								newCommitIndex := g.commitIndex
								for i := max(0, g.commitIndex); i < len(g.log); i++ {
									// We must only commit entries from current term. See paper section 5.4.2.
									if int(g.log[i].Term) == g.currentTerm {
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
									committedEntries := []CommittedEntry{}
									for i := g.commitIndex + 1; i <= newCommitIndex; i++ {
										committedEntries = append(committedEntries, CommittedEntry{
											Index: i,
											Term:  int(g.log[i].Term),
											Cmd:   g.log[i].Command,
										})
									}
									g.commitIndex = newCommitIndex
									g.Committed(committedEntries)
								}
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

func (g *Graft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.printf("RequestVote: {%v}, state=%v", request, g)

	if request.Term < int32(g.currentTerm) {
		return &pb.RequestVoteResponse{
			Term:        int32(g.currentTerm),
			VoteGranted: false,
		}, nil
	}

	grantVote := false
	if request.Term > int32(g.currentTerm) {
		grantVote = g.unguardedIsCandidateLogUpToDate(request)
		potentialCandidate := unknown
		if grantVote {
			potentialCandidate = "?" + request.CandidateId
		}
		g.unguardedTransitionToFollower(int(request.Term), potentialCandidate, grantVote) // Only reset election timer if a vote is granted.
	} else if (g.votedFor == nilString || g.votedFor == request.CandidateId) && g.unguardedIsCandidateLogUpToDate(request) {
		g.unguardedTransitionToFollower(int(request.Term), "?"+request.CandidateId, true)
	}

	if grantVote {
		g.votedFor = request.CandidateId
	}

	return &pb.RequestVoteResponse{
		Term:        int32(g.currentTerm),
		VoteGranted: grantVote,
	}, nil
}

func (g *Graft) unguardedIsCandidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	myLastLogTerm := -1
	myLastLogIndex := len(g.log) - 1
	if myLastLogIndex >= 0 {
		myLastLogTerm = int(g.log[myLastLogIndex].Term)
	}
	return myLastLogTerm < int(request.LastLogTerm) ||
		(myLastLogTerm == int(request.LastLogTerm) && myLastLogIndex <= int(request.LastLogIndex))
}

func (g *Graft) appendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.printf("AppendEntries: {%v}, state=%v", request, g)

	term := int(request.Term)
	if term < g.currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    int32(g.currentTerm),
			Success: false,
		}, nil
	}

	// Make sure we're followers to this leader.
	g.unguardedTransitionToFollower(term, request.LeaderId, true)

	if int(request.PrevLogIndex) >= len(g.log) ||
		(request.PrevLogTerm >= 0 && request.PrevLogTerm != g.log[request.PrevLogIndex].Term) {
		return &pb.AppendEntriesResponse{
			Term:    int32(term),
			Success: false,
		}, nil
	}

	if request.PrevLogIndex >= 0 && int(request.PrevLogIndex) < len(g.log)-1 {
		mismatchCount := len(g.log) - int(request.PrevLogIndex) - 1
		g.log = g.log[:len(g.log)-mismatchCount+1]
	}

	lastIndex := g.unguardedAppendToLog(request.Entries)
	if int(request.LeaderCommitIndex) > g.commitIndex {
		g.commitIndex = min(int(request.LeaderCommitIndex), lastIndex)
	}

	return &pb.AppendEntriesResponse{
		Term:    int32(term),
		Success: true,
	}, nil
}

func (g *Graft) unguardedAppendToLog(entries []*pb.LogEntry) int {
	g.log = append(g.log, entries...)
	return len(g.log) - 1
}

func (g *Graft) Append(cmds [][]byte) (index int, term int) {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.state != Leader {
		return -1, g.currentTerm
	}

	var entries []*pb.LogEntry
	for _, cmd := range cmds {
		entries = append(entries, &pb.LogEntry{Term: int32(g.currentTerm), Command: cmd})
	}
	return g.unguardedAppendToLog(entries) - len(entries) + 1, g.currentTerm
}

type gserver struct {
	pb.UnimplementedGraftServer
}

func (s *gserver) Append(ctx context.Context, request *pb.AppendRequest) (*pb.AppendResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("Unable to find Graft instance in context")
	}

	index, term := g.Append(request.Cmds)
	return &pb.AppendResponse{Index: int32(index), Term: int32(term)}, nil
}
