package graft

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type peer struct {
	id                string
	address           string
	learner           bool
	nextIndex         int64
	matchIndex        int64
	lastHeartbeatTime time.Time // Last heartbeat sent to this peer.
	logger            zap.SugaredLogger

	// Lazily initialized, protected by mut.
	lazyConn   *grpc.ClientConn
	lazyClient pb.RaftClient
	mut        sync.Mutex
}

func (p *peer) client() (pb.RaftClient, error) {
	p.mut.Lock()
	defer p.mut.Unlock()

	// TODO may want to add connection monitor.
	client := p.lazyClient
	if client == nil {
		conn, err := grpc.NewClient(
			p.address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  50 * time.Millisecond,
					Multiplier: 1.2,
					Jitter:     0.2,
					MaxDelay:   500 * time.Millisecond,
				},
				MinConnectTimeout: 1 * time.Second,
			}),
		)
		if err != nil {
			return nil, err
		}
		client = pb.NewRaftClient(conn)
		p.lazyConn = conn
		p.lazyClient = client
	}
	return client, nil
}

func (p *peer) closeConn() {
	p.mut.Lock()
	defer p.mut.Unlock()

	if p.lazyConn != nil {
		err := p.lazyConn.Close()
		if err != nil {
			p.logger.Error("Error closing connection", zap.Error(err))
		}
	}
}
