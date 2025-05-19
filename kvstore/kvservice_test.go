package kvstore

import (
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/testutil"
	"gotest.tools/v3/assert"
	"testing"
)

func TestKvServicePut(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	s, err := client.Get("k1")
	assert.NilError(t, err)
	testutil.AssertNil(t, s)

	s, err = client.Put("k1", "v1")
	assert.NilError(t, err)
	testutil.AssertNil(t, s)

	s, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Equal(t, *s, "v1")

	s, err = client.Put("k1", "v2")
	assert.NilError(t, err)
	assert.Equal(t, *s, "v1")

	s, err = client.Get("k1")
	assert.NilError(t, err)
	assert.Equal(t, *s, "v2")
}

func TestKvServiceDelete(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	s, err := client.Delete("k1")
	assert.NilError(t, err)
	testutil.AssertNil(t, s)

	s, err = client.Put("k1", "v1")
	assert.NilError(t, err)
	testutil.AssertNil(t, s)

	s, err = client.Delete("k1")
	assert.NilError(t, err)
	assert.Equal(t, *s, "v1")
}

func NewClusterClient(t *testing.T, nodeCount int) (*testutil.Cluster[*KvService], *KvClient) {
	cluster, err := testutil.StartLocalCluster[*KvService](
		testutil.ClusterConfig{
			Dir:                       t.TempDir(),
			NodeCount:                 nodeCount,
			HeartbeatMillis:           50,
			ElectionTimeoutLowMillis:  150,
			ElectionTimeoutHighMillis: 300,
		},
		func(address string, config graft.Config) (*KvService, error) {
			return NewKvService(address, config)
		},
	)

	if err != nil {
		t.Fatalf("Couldn't start cluster: %v", err)
	}

	client := NewKvClient("client-"+t.Name(), cluster.ServiceConfig())

	err = client.CheckHealthy()
	if err != nil {
		t.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, client
}
