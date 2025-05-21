package dlock

import (
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/testutil"
	"go.uber.org/zap"
	"gotest.tools/v3/assert"
	"testing"
	"time"
)

func TestDlockServiceLockWithExpiry(t *testing.T) {
	clock := mockClock()
	cluster, client1 := NewClusterClient(t, 3, clock)
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	t1, ok, err := client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	_, ok, err = client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	clock.Advance(500 * time.Millisecond)

	_, ok, err = client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	clock.Advance(500 * time.Millisecond)

	t2, ok, err := client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t1 < t2)

	_, ok, err = client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
}

func TestDlockServiceRLockWithExpiry(t *testing.T) {
	clock := mockClock()
	cluster, client1 := NewClusterClient(t, 3, clock)
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	t1, ok, err := client1.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	_, ok, err = client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	t2, ok, err := client2.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t1 == t2)

	clock.Advance(500 * time.Millisecond)

	client3 := NewDlockClient("client3-"+t.Name(), cluster.ServiceConfig())

	_, ok, err = client3.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	clock.Advance(500 * time.Millisecond)

	t3, ok, err := client3.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t2 < t3)

	_, ok, err = client1.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	_, ok, err = client2.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
}

func TestDlockServiceLockUnlock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, SystemClock())
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	t1, ok, err := client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	_, ok, err = client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	ok, err = client1.Unlock("r1", t1)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	ok, err = client1.Unlock("r1", t1)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	t2, ok, err := client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t1 < t2)

	_, ok, err = client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	ok, err = client2.Unlock("r1", t1) // T1 is not current token.
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	ok, err = client2.Unlock("r1", t2)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	t3, ok, err := client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t2 < t3)
}

func TestDlockServiceRLockUnlock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, SystemClock())
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())
	client3 := NewDlockClient("client3-"+t.Name(), cluster.ServiceConfig())

	t1, ok, err := client1.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	t2, ok, err := client2.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t1 == t2)

	_, ok, err = client3.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	ok, err = client1.RUnlock("r1", t1)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	_, ok, err = client3.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	ok, err = client2.RUnlock("r1", t2)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	t3, ok, err := client3.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t2 < t3)

	ok, err = client3.Unlock("r1", t3)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	t4, ok, err := client2.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t3 == t4)
}

func TestDlockServiceRefreshLock(t *testing.T) {
	clock := mockClock()

	cluster, client1 := NewClusterClient(t, 3, clock)
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	t1, ok, err := client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	clock.Advance(900 * time.Millisecond)

	ok, err = client1.RefreshLock("r1", t1, 600*time.Millisecond)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	clock.Advance(100 * time.Millisecond)

	_, ok, err = client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	clock.Advance(500 * time.Millisecond)

	t2, ok, err := client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t1 < t2)

	clock.Advance(1 * time.Second)

	ok, err = client2.RefreshLock("r1", t1, 500*time.Millisecond)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
}

func TestDlockServiceRefreshRLock(t *testing.T) {
	clock := mockClock()

	cluster, client1 := NewClusterClient(t, 3, clock)
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	t1, ok, err := client1.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	clock.Advance(900 * time.Millisecond)

	ok, err = client1.RefreshRLock("r1", t1, 600*time.Millisecond)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	clock.Advance(100 * time.Millisecond)

	_, ok, err = client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	clock.Advance(500 * time.Millisecond)

	t2, ok, err := client2.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, t1 < t2)

	ok, err = client2.Unlock("r1", t2)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	_, ok, err = client2.TryRLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	clock.Advance(1 * time.Second)

	ok, err = client2.RefreshRLock("r1", t1, 500*time.Millisecond)
	assert.NilError(t, err)
	assert.Assert(t, !ok)
}

func TestDlockServiceBlockingLockWithExpiredTtl(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, SystemClock())
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	now := time.Now()
	t1, ok, err := client1.TryLock("r1", 500*time.Millisecond)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	type result struct {
		t   uint64
		err error
	}

	done := make(chan result)
	go func() {
		t, err := client2.Lock("r1", 500*time.Millisecond, 1*time.Second)
		done <- result{t: t, err: err}
	}()

	r := <-done
	timeToAcquire := time.Now().Sub(now) // Should be more than ttl and less than timeout.

	assert.NilError(t, r.err)
	assert.Assert(t, timeToAcquire >= 500*time.Millisecond)
	assert.Assert(t, timeToAcquire <= 1*time.Second)
	assert.Assert(t, t1 < r.t)
}

func TestDlockServiceBlockingLockWithUnlock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, SystemClock())
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	now := time.Now()
	t1, ok, err := client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	type result struct {
		t   uint64
		err error
	}

	done := make(chan result)
	go func() {
		t, err := client2.Lock("r1", 1*time.Second, 1*time.Second)
		done <- result{t: t, err: err}
	}()

	time.AfterFunc(500*time.Millisecond, func() {
		ok, err := client1.Unlock("r1", t1)
		assert.NilError(t, err)
		assert.Assert(t, ok)
	})

	r := <-done
	timeToAcquire := time.Now().Sub(now) // Should be more than ttl and less than timeout.

	assert.NilError(t, r.err)
	assert.Assert(t, timeToAcquire >= 500*time.Millisecond)
	assert.Assert(t, timeToAcquire <= 1*time.Second)
	assert.Assert(t, t1 < r.t)
}

func TestDlockServiceBlockingRLockWithExpiredTtl(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, SystemClock())
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	now := time.Now()
	t1, ok, err := client1.TryLock("r1", 500*time.Millisecond)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	type result struct {
		t   uint64
		err error
	}

	done := make(chan result)
	go func() {
		t, err := client2.RLock("r1", 500*time.Millisecond, 1*time.Second)
		done <- result{t: t, err: err}
	}()

	r := <-done
	timeToAcquire := time.Now().Sub(now) // Should be more than ttl and less than timeout.

	assert.NilError(t, r.err)
	assert.Assert(t, timeToAcquire >= 500*time.Millisecond)
	assert.Assert(t, timeToAcquire <= 1*time.Second)
	assert.Assert(t, t1 == r.t)
}

func TestDlockServiceBlockingRLockWithUnlock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, SystemClock())
	defer cluster.Shutdown()

	client2 := NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

	now := time.Now()
	t1, ok, err := client1.TryLock("r1", 1*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	type result struct {
		t   uint64
		err error
	}

	done := make(chan result)
	go func() {
		t, err := client2.RLock("r1", 1*time.Second, 1*time.Second)
		done <- result{t: t, err: err}
	}()

	time.AfterFunc(500*time.Millisecond, func() {
		ok, err := client1.Unlock("r1", t1)
		assert.NilError(t, err)
		assert.Assert(t, ok)
	})

	r := <-done
	timeToAcquire := time.Now().Sub(now) // Should be more than ttl and less than timeout.

	assert.NilError(t, r.err)
	assert.Assert(t, timeToAcquire >= 500*time.Millisecond)
	assert.Assert(t, timeToAcquire <= 1*time.Second)
	assert.Assert(t, t1 == r.t)
}

type MockClock struct {
	now time.Time
}

func (c *MockClock) Now() time.Time {
	return c.now
}

func (c *MockClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func mockClock() *MockClock {
	return &MockClock{
		now: time.Now(),
	}
}

func NewClusterClient(t *testing.T, nodeCount int, clock Clock) (*testutil.Cluster[*DlockService], *DlockClient) {
	cluster, err := testutil.StartLocalCluster[*DlockService](
		testutil.ClusterConfig[*DlockService]{
			Dir:                       t.TempDir(),
			NodeCount:                 nodeCount,
			HeartbeatMillis:           50,
			ElectionTimeoutLowMillis:  150,
			ElectionTimeoutHighMillis: 300,
			ServiceFactory: func(address string, config graft.Config) (*DlockService, error) {
				return NewDlockService(address, clock, config)
			},
			Logger: zap.NewNop(),
		},
	)

	if err != nil {
		t.Fatalf("Couldn't start cluster: %v", err)
	}

	client := NewDlockClient("client-"+t.Name(), cluster.ServiceConfig())

	err = client.CheckHealthy()
	if err != nil {
		t.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, client
}
