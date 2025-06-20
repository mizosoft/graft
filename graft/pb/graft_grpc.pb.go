// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: graft.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	Raft_RequestVote_FullMethodName     = "/graft.Raft/RequestVote"
	Raft_AppendEntries_FullMethodName   = "/graft.Raft/AppendEntries"
	Raft_InstallSnapshot_FullMethodName = "/graft.Raft/InstallSnapshot"
)

// RaftClient is the client API for Raft service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RaftClient interface {
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, in *SnapshotRequest, opts ...grpc.CallOption) (*SnapshotResponse, error)
}

type raftClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftClient(cc grpc.ClientConnInterface) RaftClient {
	return &raftClient{cc}
}

func (c *raftClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RequestVoteResponse)
	err := c.cc.Invoke(ctx, Raft_RequestVote_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, Raft_AppendEntries_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) InstallSnapshot(ctx context.Context, in *SnapshotRequest, opts ...grpc.CallOption) (*SnapshotResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SnapshotResponse)
	err := c.cc.Invoke(ctx, Raft_InstallSnapshot_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServer is the server API for Raft service.
// All implementations must embed UnimplementedRaftServer
// for forward compatibility.
type RaftServer interface {
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(context.Context, *SnapshotRequest) (*SnapshotResponse, error)
	mustEmbedUnimplementedRaftServer()
}

// UnimplementedRaftServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRaftServer struct{}

func (UnimplementedRaftServer) RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedRaftServer) AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (UnimplementedRaftServer) InstallSnapshot(context.Context, *SnapshotRequest) (*SnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InstallSnapshot not implemented")
}
func (UnimplementedRaftServer) mustEmbedUnimplementedRaftServer() {}
func (UnimplementedRaftServer) testEmbeddedByValue()              {}

// UnsafeRaftServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftServer will
// result in compilation errors.
type UnsafeRaftServer interface {
	mustEmbedUnimplementedRaftServer()
}

func RegisterRaftServer(s grpc.ServiceRegistrar, srv RaftServer) {
	// If the following call pancis, it indicates UnimplementedRaftServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Raft_ServiceDesc, srv)
}

func _Raft_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Raft_RequestVote_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Raft_AppendEntries_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_InstallSnapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).InstallSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Raft_InstallSnapshot_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).InstallSnapshot(ctx, req.(*SnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Raft_ServiceDesc is the grpc.ServiceDesc for Raft service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Raft_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "graft.Raft",
	HandlerType: (*RaftServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    _Raft_RequestVote_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _Raft_AppendEntries_Handler,
		},
		{
			MethodName: "InstallSnapshot",
			Handler:    _Raft_InstallSnapshot_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "graft.proto",
}
