package benchmarks

import (
	"testing"
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	infratesting "github.com/mizosoft/graft/infra/testing"
	"github.com/mizosoft/graft/kvstore/client"
	"github.com/mizosoft/graft/kvstore/service"
	"go.uber.org/zap"
)

func newClusterClient(b *testing.B, nodeCount int, batchInterval time.Duration) (*infratesting.Cluster, *client.KvClient) {
	cluster, err := infratesting.StartLocalCluster(
		infratesting.ClusterConfig{
			Dir:                   b.TempDir(),
			NodeCount:             nodeCount,
			HeartbeatMillis:       50,
			ElectionTimeoutMillis: graft.IntRange{Low: 150, High: 300},
			ServerFactory: func(address string, config graft.Config) (*server.Server, error) {
				return service.NewKvServer(address, batchInterval, config)
			},
			Logger: zap.NewNop(),
		},
	)

	if err != nil {
		b.Fatalf("Couldn't start cluster: %v", err)
	}

	kvClient := client.NewKvClient("client-"+b.Name(), cluster.ServiceConfig())

	err = kvClient.CheckHealthy()
	if err != nil {
		b.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, kvClient
}

func newClusterClientWithWalPersistence(b *testing.B, nodeCount int, batchInterval time.Duration) (*infratesting.Cluster, *client.KvClient) {
	cluster, err := infratesting.StartLocalCluster(
		infratesting.ClusterConfig{
			Dir:                   b.TempDir(),
			NodeCount:             nodeCount,
			HeartbeatMillis:       50,
			ElectionTimeoutMillis: graft.IntRange{Low: 150, High: 300},
			ServerFactory: func(address string, config graft.Config) (*server.Server, error) {
				return service.NewKvServer(address, batchInterval, config)
			},
			PersistenceFactory: func(dir string) (graft.Persistence, error) {
				return graft.OpenWal(graft.WalOptions{
					Dir:             dir,
					SegmentSize:     64 * 1024 * 1024,
					SuffixCacheSize: 1 * 1024 * 1024,
				})
			},
			Logger: zap.NewNop(),
		},
	)

	if err != nil {
		b.Fatalf("Couldn't start cluster: %v", err)
	}

	kvClient := client.NewKvClient("client-"+b.Name(), cluster.ServiceConfig())

	err = kvClient.CheckHealthy()
	if err != nil {
		b.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, kvClient
}
