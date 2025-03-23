package graft

import (
	"fmt"
	"sync"

	pb "github.com/mizosoft/graft/pb"
	grpc "google.golang.org/grpc"
	insecure "google.golang.org/grpc/credentials/insecure"
)

type peer struct {
	id         string
	address    string
	lazyClient pb.RaftClient // Lazily initialized, protected by mut.
	mut        sync.Mutex
}

func (n *peer) client() (pb.RaftClient, error) {
	n.mut.Lock()
	defer n.mut.Unlock()

	// TODO add retry logic.
	client := n.lazyClient
	if client == nil {
		fmt.Println("Connecting to " + n.address)
		conn, err := grpc.NewClient(n.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		client = pb.NewRaftClient(conn)
		n.lazyClient = client
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
