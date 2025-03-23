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

type role int

const (
	Follower role = iota
	Candidate
	Leader
)

type state struct {
	currentTerm      int
	votedFor         string
	log              []pb.LogEntry
	commitIndex      int
	lastAppliedIndex int

	// Leader-specific state.
	nextIndex  map[string]int
	matchIndex map[string]int
}

type Graft struct {
	address                    string
	id                         string
	role                       role
	state                      state
	cluster                    *cluster
	electionTimeoutWitnessTerm int
	electionTimer              *periodicTimer
	heartbeatTimer             *periodicTimer
	server                     server
	mut                        sync.Mutex
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
		id:      config.Id,
		address: myAddress,
		role:    Follower,
		state: state{
			currentTerm:      0,
			votedFor:         nilString,
			log:              []pb.LogEntry{},
			commitIndex:      -1,
			lastAppliedIndex: -1,
		},
		cluster:                    newCluster(addresses),
		electionTimeoutWitnessTerm: -1,
		electionTimer: newRandomizedTimer(
			time.Duration(config.ElectionTimeoutLowMillis)*time.Millisecond,
			time.Duration(config.ElectionTimeoutHighMillis)*time.Millisecond),
		heartbeatTimer: newTimer(time.Duration(config.HeartbeatMillis) * time.Millisecond),
		server:         server{},
	}
	g.electionTimer.trigger = g.triggerElectionTimeout
	g.heartbeatTimer.trigger = g.triggerHeartbeat
	return g
}

type graftKey struct{}

func (g *Graft) Log(format string, vals ...any) {
	fmt.Printf("%v - %s: %v\n", time.Now().Local().UnixMilli(), g.id, fmt.Sprintf(format, vals...))
}

// TODO add some running & closed booleans.
func (g *Graft) Run() error {
	listener, err := net.Listen("tcp", g.address)
	if err != nil {
		return err
	}

	g.Log("listening on %v", listener.Addr())

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				return handler(context.WithValue(ctx, graftKey{}, g), req)
			}))
	pb.RegisterRaftServer(grpcServer, &g.server)

	func() {
		g.mut.Lock()
		defer g.mut.Unlock()

		g.unguardedResetElectionTimer(g.state.currentTerm)
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

func (g *Graft) unguardedResetElectionTimer(witnessTerm int) {
	g.electionTimeoutWitnessTerm = witnessTerm
	g.electionTimer.reset()
}

func (g *Graft) triggerElectionTimeout() {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.electionTimeoutWitnessTerm != g.state.currentTerm {
		g.Log("Election timeout on outdated term (%d), skpping", g.electionTimeoutWitnessTerm)
		return
	}

	if g.role == Leader {
		// We might've already won an election for this term but couldn't stop the timer on time.
		g.Log("Election timed out too late, we are the leader now")
		return
	}

	if g.role == Candidate {
		g.Log("Candidate election timed out")
	}

	g.Log("Running for leader at (%d)", g.state.currentTerm)

	g.unguardedTransitionToCandidate()

	// Request votes from peers.
	voteCount := 1 // Vote for self.
	electionTerm := g.state.currentTerm
	for _, p := range g.cluster.peers {
		go func(peer *peer) {
			if client, cerr := peer.client(); cerr != nil {
				g.Log("Error connecting to peer %s: %v", peer.id, cerr)
			} else if res, rerr := client.RequestVote(
				context.Background(),
				&pb.RequestVoteRequest{
					Term:         int32(electionTerm),
					CandidateId:  g.id,
					LastLogIndex: -1,
					LastLogTerm:  -1,
				}); rerr != nil {
				g.Log("RequestVote to %s failed: %v", peer.id, rerr)
			} else {
				g.mut.Lock()
				defer g.mut.Unlock()

				if res.Term > int32(g.state.currentTerm) {
					g.Log("RequestVote to %s detected higher term", peer.id)
					g.unguardedTransitionToFollower(int(res.Term))
				} else if res.VoteGranted && electionTerm == g.state.currentTerm && g.role == Candidate { // Check that the election has't timed out.
					voteCount++
					g.Log("RequestVote to %s success, voteCount=%d", peer.id, voteCount)
					if voteCount >= g.cluster.majorityCount() {
						g.Log("I am the leader now")
						g.unguardedTransitionToLeader()
					}
				}
			}
		}(p)
	}
}

func (g *Graft) triggerHeartbeat() {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.role == Leader {
		g.unguardedBroadcast()
	}
}

func (g *Graft) unguardedTransitionToLeader() {
	g.Log("Transitioning to Leader (%d)", g.state.currentTerm)
	g.role = Leader
	g.state.nextIndex = make(map[string]int)
	g.state.matchIndex = make(map[string]int)
	for id := range g.cluster.peers {
		g.state.nextIndex[id] = len(g.state.log)
		g.state.matchIndex[id] = 0
	}
	g.electionTimeoutWitnessTerm = -1
	g.electionTimer.stop()
	g.unguardedBroadcast() // Establish authority rn.
}

func (g *Graft) unguardedTransitionToCandidate() {
	g.Log("Transitioning to Candidate (%d -> %d)", g.state.currentTerm, g.state.currentTerm+1)
	g.role = Candidate
	g.state.currentTerm++
	g.state.votedFor = g.id // Vote for self.
	g.unguardedResetElectionTimer(g.state.currentTerm)
}

func (g *Graft) unguardedTransitionToFollower(term int) {
	g.Log("Transitioning to Follower at (%d -> %d)", g.state.currentTerm, term)
	g.role = Follower
	g.state.currentTerm = term
	g.state.votedFor = nilString
	g.unguardedResetElectionTimer(term)
	g.heartbeatTimer.stop() // If we were a leader.
}

func (g *Graft) unguardedBroadcast() chan struct{} {
	g.heartbeatTimer.reset()

	me := g.id
	witnessTerm := g.state.currentTerm
	commitIndex := g.state.commitIndex
	done := make(chan struct{})
	for _, p := range g.cluster.peers {
		go func(peer *peer) {
			defer func() {
				done <- struct{}{}
			}()
			if client, cerr := peer.client(); cerr != nil {
				g.Log("Error connecting to peer %s: %v", peer.id, cerr)
			} else if res, rerr := client.AppendEntries(
				context.Background(),
				&pb.AppendEntriesRequest{
					Term:              int32(witnessTerm),
					LeaderId:          string(me),
					PrevLogIndex:      -1,
					PrevLogTerm:       -1,
					Entries:           []*pb.LogEntry{},
					LeaderCommitIndex: int32(commitIndex),
				}); rerr != nil {
				g.Log("RequestVote to %s failed: %v", peer.id, rerr)
			} else {
				g.mut.Lock()
				defer g.mut.Unlock()

				if res.Term > int32(g.state.currentTerm) {
					g.Log("AppendEntries to %s detected higher term", peer.id)
					g.unguardedTransitionToFollower(int(res.Term))
				} else if res.Success && witnessTerm == g.state.currentTerm {
					g.Log("AppendEntries to %s success", peer.id)
					// TODO update leader state.
				}
			}
		}(p)
	}
	return done
}

func (g *Graft) requestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.Log("RequestVote from %v at (%d)", request.CandidateId, request.Term)

	if request.Term < int32(g.state.currentTerm) {
		return &pb.RequestVoteResponse{
			Term:        int32(g.state.currentTerm),
			VoteGranted: false,
		}, nil
	}

	if request.Term > int32(g.state.currentTerm) {
		g.unguardedTransitionToFollower(int(request.Term))
	}

	if g.state.votedFor == nilString || g.state.votedFor == request.CandidateId {
		g.state.votedFor = request.CandidateId
		return &pb.RequestVoteResponse{
			Term:        request.Term,
			VoteGranted: true,
		}, nil
	}

	return &pb.RequestVoteResponse{
		Term:        int32(g.state.currentTerm),
		VoteGranted: false,
	}, nil
}

func (g *Graft) appendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g.mut.Lock()
	defer g.mut.Unlock()

	g.Log("AppendEntries from %v at (%d)", request.LeaderId, request.Term)

	term := int(request.Term)
	if term < g.state.currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    int32(g.state.currentTerm),
			Success: false,
		}, nil
	}

	g.unguardedTransitionToFollower(term) // Make sure we're followers.

	return &pb.AppendEntriesResponse{
		Term:    request.Term,
		Success: true,
	}, nil
}
