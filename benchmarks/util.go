package benchmarks

import (
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore2/client"
	"github.com/mizosoft/graft/kvstore2/service"
	"go.uber.org/zap"
	"testing"
	"time"
)

func newClusterClient(b *testing.B, nodeCount int, batchInterval time.Duration) (*server.Cluster[*service.KvService], *client.KvClient) {
	cluster, err := server.StartLocalCluster[*service.KvService](
		server.ClusterConfig[*service.KvService]{
			Dir:                       b.TempDir(),
			NodeCount:                 nodeCount,
			HeartbeatMillis:           50,
			ElectionTimeoutLowMillis:  150,
			ElectionTimeoutHighMillis: 300,
			ServiceFactory: func(address string, config graft.Config) (*service.KvService, error) {
				return service.NewKvService(address, batchInterval, config)
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

func newClusterClientWithWalPersistence(b *testing.B, nodeCount int, batchInterval time.Duration) (*server.Cluster[*service.KvService], *client.KvClient) {
	cluster, err := server.StartLocalCluster[*service.KvService](
		server.ClusterConfig[*service.KvService]{
			Dir:                       b.TempDir(),
			NodeCount:                 nodeCount,
			HeartbeatMillis:           50,
			ElectionTimeoutLowMillis:  150,
			ElectionTimeoutHighMillis: 300,
			ServiceFactory: func(address string, config graft.Config) (*service.KvService, error) {
				return service.NewKvService(address, batchInterval, config)
			},
			PersistenceFactory: func(dir string) (graft.Persistence, error) {
				return graft.OpenWal(dir, 64*1024*1024, 1*1024*1024, zap.NewNop())
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
