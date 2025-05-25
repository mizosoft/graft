package benchmarks

import (
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/kvstore2/client"
	"github.com/mizosoft/graft/kvstore2/service"
	"github.com/mizosoft/graft/testutil"
	"go.uber.org/zap"
	"testing"
	"time"
)

func BenchmarkKvStorePut(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 0)
	defer cluster.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := store.Put("k", "v")
		if err != nil {
			b.Fatalf("put error: %v", err)
		}
	}
}

func BenchmarkKvStorePutBatched(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 1*time.Millisecond)
	defer cluster.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := store.Put("k", "v")
		if err != nil {
			b.Fatalf("put error: %v", err)
		}
	}
}

func BenchmarkKvStorePutMultithreaded(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 0)
	defer cluster.Shutdown()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Put("k", "v")
		}
	})
}

func BenchmarkKvStorePutMultithreadedWithBatching(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 5*time.Millisecond)
	defer cluster.Shutdown()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Put("k", "v")
		}
	})
}

func newClusterClient(b *testing.B, nodeCount int, batchInterval time.Duration) (*testutil.Cluster[*service.KvService], *client.KvClient) {
	cluster, err := testutil.StartLocalCluster[*service.KvService](
		testutil.ClusterConfig[*service.KvService]{
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
