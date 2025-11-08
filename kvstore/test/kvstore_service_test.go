package test

import (
	"fmt"
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore2/client"
	"github.com/mizosoft/graft/kvstore2/service"
	"github.com/mizosoft/graft/testutil"
	"go.uber.org/zap"
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

func NewClusterClient(t *testing.T, nodeCount int) (*server.Cluster[*service.KvService], *client.KvClient) {
	cluster, err := server.StartLocalCluster[*service.KvService](
		server.ClusterConfig[*service.KvService]{
			Dir:                   t.TempDir(),
			NodeCount:             nodeCount,
			HeartbeatMillis:       50,
			ElectionTimeoutMillis: graft.IntRange{Low: 150, High: 300},
			ServiceFactory: func(address string, config graft.Config) (*service.KvService, error) {
				return service.NewKvService(address, 0, config)
			},
			Logger: zap.NewExample(),
		},
	)

	if err != nil {
		t.Fatalf("Couldn't start cluster: %v", err)
	}

	client := client.NewKvClient("client-"+t.Name(), cluster.ServiceConfig())

	err = client.CheckHealthy()
	if err != nil {
		t.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, client
}
