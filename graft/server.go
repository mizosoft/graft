package graft

import (
	"context"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedRaftServer
	g *Graft
}

func (s *server) RequestVote(_ context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.g.requestVote(request)
}

func (s *server) AppendEntries(_ context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.g.appendEntries(request)
}

func (s *server) InstallSnapshot(stream grpc.ClientStreamingServer[pb.SnapshotRequest, pb.SnapshotResponse]) error {
	return s.g.installSnapshot(stream)
}
