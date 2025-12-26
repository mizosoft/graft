package benchmarks

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore2/client"
	"github.com/mizosoft/graft/kvstore2/service"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newClusterClient(b *testing.B, nodeCount int, batchInterval time.Duration) (*server.Cluster, *client.KvClient) {
	// Open a file for writing logs
	file, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a zapcore.WriteSyncer from the file
	writeSyncer := zapcore.AddSync(file)

	// Create an encoder configuration
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Create a core that writes to the file
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		zapcore.InfoLevel,
	)

	// Create the logger
	logger := zap.New(core)

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutting down gracefully...")
		logger.Sync() // Ensure logs are flushed
		os.Exit(0)
	}()

	cluster, err := server.StartLocalCluster(
		server.ClusterConfig{
			Dir:                   b.TempDir(),
			NodeCount:             nodeCount,
			HeartbeatMillis:       50,
			ElectionTimeoutMillis: graft.IntRange{Low: 150, High: 300},
			ServerFactory: func(address string, config graft.Config) (*server.Server, error) {
				return service.NewKvServer(address, batchInterval, config)
			},
			Logger: logger,
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

func newClusterClientWithWalPersistence(b *testing.B, nodeCount int, batchInterval time.Duration) (*server.Cluster, *client.KvClient) {
	cluster, err := server.StartLocalCluster[*service.KvService](
		server.ClusterConfig{
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
