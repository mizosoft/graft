package graft

import (
	"context"

	"github.com/mizosoft/graft/raftpb"
)

type server struct {
	raftpb.UnimplementedRaftServer
}

func (s *server) RequestVote(ctx context.Context, request *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.requestVote(ctx, request)
}

func (s *server) AppendEntries(ctx context.Context, request *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	g, ok := ctx.Value(graftKey{}).(*Graft)
	if !ok {
		panic("unable to find Graft instance in context")
	}
	return g.appendEntries(ctx, request)
}
