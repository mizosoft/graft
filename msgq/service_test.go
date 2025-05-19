package msgq

import (
	"context"
	"errors"
	"fmt"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/testutil"
	"go.uber.org/zap"
	"gotest.tools/v3/assert"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestMsgqServiceEnqueueDequeAutoAck(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	msg, ok, err := client.Dequeue("a")
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	id, err := client.Enqueue("a", "hello")
	assert.NilError(t, err)

	msg, ok, err = client.Dequeue("a")
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Equal(t, id, msg.Id)
	assert.Equal(t, msg.Data, "hello")
}

func TestMsgqServiceEnqueueMultiple(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	ids := make([]string, 0)
	for i := range 10 {
		id, err := client.Enqueue("a", "hello"+strconv.Itoa(i))
		assert.NilError(t, err)
		ids = append(ids, id)
	}

	for i := range 10 {
		msg, ok, err := client.Dequeue("a")
		assert.NilError(t, err)
		assert.Assert(t, ok)
		assert.Equal(t, ids[i], msg.Id)
		assert.Equal(t, msg.Data, "hello"+strconv.Itoa(i))
	}
}

func TestKvServiceFailOver(t *testing.T) {
	cluster, client := NewClusterClient(t, 3)
	defer cluster.Shutdown()

	var q []string
	for i := range 10 {
		q = append(q, fmt.Sprintf("hello%d", i))
	}

	for _, v := range q {
		_, err := client.Enqueue("a", v)
		assert.NilError(t, err)
	}

	for i := range 50 {
		if i%5 == 0 {
			err := cluster.Restart(client.LeaderId())
			assert.NilError(t, err)
		}

		newV := fmt.Sprintf("hello%d", 10*(i+1))
		q = append(q, newV)

		_, err := client.Enqueue("a", newV)
		assert.NilError(t, err)

		for _, v := range q {
			msg, exists, err := client.Dequeue("a")
			assert.NilError(t, err)
			assert.Assert(t, exists)
			assert.Equal(t, v, msg.Data)
		}
		q = q[:0]
	}
}

func NewClusterClient(t *testing.T, nodeCount int) (*testutil.Cluster[*MsgqService], *MsgqClient) {
	cluster, err := testutil.StartLocalCluster[*MsgqService](
		testutil.ClusterConfig[*MsgqService]{
			Dir:                       t.TempDir(),
			NodeCount:                 nodeCount,
			HeartbeatMillis:           50,
			ElectionTimeoutLowMillis:  150,
			ElectionTimeoutHighMillis: 300,
			ServiceFactory: func(address string, config graft.Config) (*MsgqService, error) {
				return NewMsgqService(address, config)
			},
			Logger: zap.NewNop(),
		},
	)

	if err != nil {
		t.Fatalf("Couldn't start cluster: %v", err)
	}

	client := NewMsgqClient("client-"+t.Name(), cluster.ServiceConfig())

	err = client.CheckHealthy()
	if err != nil {
		t.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, client
}

func (c *MsgqClient) CheckHealthy() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/config", nil)
	if err != nil {
		return err
	}

retry:
	res, err := c.http.Do(request)
	if err != nil {
		select {
		case <-ctx.Done():
			return err
		case <-time.After(50 * time.Millisecond): // backoff
			goto retry
		}
	}

	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}
	return nil
}
