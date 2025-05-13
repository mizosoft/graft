package graft

import (
	"context"

	"github.com/mizosoft/graft/raftpb"
)

type server struct {
	raftpb.UnimplementedRaftServer
}

type graftKey struct{}

func (s *server) RequestVote(ctx context.Context, request *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.requestVote(request)
}

func (s *server) AppendEntries(ctx context.Context, request *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.appendEntries(request)
}

func (s *server) InstallSnapshot(ctx context.Context, request *raftpb.SnapshotRequest) (*raftpb.SnapshotResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.installSnapshot(request)
}
