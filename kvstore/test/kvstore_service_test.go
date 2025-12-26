package test

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore2/client"
	"github.com/mizosoft/graft/kvstore2/service"
	"github.com/mizosoft/graft/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gotest.tools/v3/assert"
)

func TestKvServicePut(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	s, exists, err := client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, !exists)
	testutil.AssertNil(t, s)

	s, exists, err = client.Put("k1", "v1")
	assert.NilError(t, err)
	assert.Assert(t, !exists)
	testutil.AssertNil(t, s)

	s, exists, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "v1")

	s, exists, err = client.Put("k1", "v2")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "v1")

	s, exists, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "v2")
}

func TestKvServiceParallelPut(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	readersCount := 16
	writersCount := 8
	runCount := 256

	var wg sync.WaitGroup
	for range writersCount {
		wg.Go(func() {
			for range runCount {
				v, e, err := client.Put("k", "v")
				assert.NilError(t, err)
				if e {
					assert.Equal(t, v, "v")
				}
			}
		})
	}

	for range readersCount {
		wg.Go(func() {
			for range runCount {
				v, e, err := client.Get("k")
				assert.NilError(t, err)
				if e {
					assert.Equal(t, v, "v")
				}
			}
		})
	}

	wg.Wait()
}

func TestKvServiceDelete(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	s, exists, err := client.Delete("k1")
	assert.NilError(t, err)
	assert.Assert(t, !exists)
	testutil.AssertNil(t, s)

	s, exists, err = client.Put("k1", "v1")
	assert.NilError(t, err)
	assert.Assert(t, !exists)
	testutil.AssertNil(t, s)

	s, exists, err = client.Delete("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "v1")
}

func TestKvServicePutIfAbsent(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	b, err := client.PutIfAbsent("k1", "v1")
	assert.NilError(t, err)
	assert.Assert(t, b)

	s, exists, err := client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "v1")

	b, err = client.PutIfAbsent("k1", "v2")
	assert.NilError(t, err)
	assert.Assert(t, !b)

	s, exists, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "v1")
}

func TestKvServiceAppend(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	l, err := client.Append("k1", "aa")
	assert.NilError(t, err)
	assert.Equal(t, l, 2)

	s, exists, err := client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "aa")

	l, err = client.Append("k1", "bbb")
	assert.NilError(t, err)
	assert.Equal(t, l, 5)

	s, exists, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "aabbb")

	s, exists, err = client.Put("k1", "abab")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "aabbb")

	s, exists, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, exists)
	assert.Equal(t, s, "abab")
}

func TestKvServiceCas(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	ok, s, err := client.Cas("k1", "a", "aa")
	assert.NilError(t, err)
	assert.Assert(t, !ok)
	testutil.AssertNil(t, s)

	s, exists, err := client.Get("k1")
	assert.NilError(t, err)
	assert.Assert(t, !exists)
	testutil.AssertNil(t, s)

	ok, s, err = client.Cas("k1", "", "a")
	assert.NilError(t, err)
	assert.Equal(t, s, "a")

	ok, s, err = client.Cas("k1", "aa", "bb")
	assert.NilError(t, err)
	assert.Assert(t, !ok)
	assert.Equal(t, s, "a")

	ok, s, err = client.Cas("k1", "a", "bb")
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Equal(t, s, "bb")
}

func TestKvServiceFailOver(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	values := make(map[string]string)
	for i := range 10 {
		values[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}

	for k, v := range values {
		_, _, err := client.Put(k, v)
		assert.NilError(t, err)
	}

	for i := range 50 {
		if i%5 == 0 {
			err := cluster.Restart(client.LeaderId())
			assert.NilError(t, err)
		}

		newK, newV := fmt.Sprintf("k%d", 10*(i+1)), fmt.Sprintf("v%d", 10*(i+1))
		values[newK] = newV

		_, _, err := client.Put(newK, newV)
		assert.NilError(t, err)

		for k, v := range values {
			av, exists, err := client.Get(k)
			assert.NilError(t, err)
			assert.Assert(t, exists, k, av)
			assert.Equal(t, v, av)
		}
	}
}

func NewClusterClient(t *testing.T, nodeCount int) (*server.Cluster, *client.KvClient) {
	// Open a file for writing logs
	file, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

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
		file.Close()
		logger.Sync() // Ensure logs are flushed
	}()

	cluster, err := server.StartLocalCluster(
		server.ClusterConfig{
			Dir:                   t.TempDir(),
			NodeCount:             nodeCount,
			HeartbeatMillis:       50,
			ElectionTimeoutMillis: graft.IntRange{Low: 150, High: 300},
			ServerFactory: func(address string, config graft.Config) (*server.Server, error) {
				return service.NewKvServer(address, 0, config)
			},
			Logger: logger,
		},
	)

	if err != nil {
		t.Fatalf("Couldn't start cluster: %v", err)
	}

	kvClient := client.NewKvClient("client-"+t.Name(), cluster.ServiceConfig())

	err = kvClient.CheckHealthy()
	if err != nil {
		t.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, kvClient
}
