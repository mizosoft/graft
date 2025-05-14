package graft

import (
	"context"
	"github.com/mizosoft/graft/pb"
)

type server struct {
	pb.UnimplementedRaftServer
}

type graftKey struct{}

func (s *server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.requestVote(request)
}

func (s *server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.appendEntries(request)
}

func (s *server) InstallSnapshot(ctx context.Context, request *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.installSnapshot(request)
}
