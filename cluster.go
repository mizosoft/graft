package graft

import (
	"sync"
	"time"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type peer struct {
	id         string
	address    string
	lazyClient pb.RaftClient // Lazily initialized, protected by mut.
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
		p.lazyClient = client
	}
	return client, nil
}

type cluster struct {
	peers map[string]*peer
}

func (c *cluster) majorityCount() int {
	return 1 + (c.followerCount()+1)/2
}

func (c *cluster) followerCount() int {
	return len(c.peers)
}

func newCluster(peerAddresses map[string]string) *cluster {
	c := &cluster{peers: make(map[string]*peer)}
	for id, addr := range peerAddresses {
		c.peers[id] = &peer{
			id:      id,
			address: addr,
		}
	}
	return c
}
