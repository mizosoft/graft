package dlock

import (
	"fmt"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/dlock/client"
	"github.com/mizosoft/graft/dlock/service"
	"github.com/mizosoft/graft/testutil"
	"go.uber.org/zap"
	"gotest.tools/v3/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestDlockServiceLockWithExpiry(t *testing.T) {
	clock := mockClock()
	cluster, client1 := NewClusterClient(t, 3, clock)
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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

	client3 := client.NewDlockClient("client3-"+t.Name(), cluster.ServiceConfig())

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
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())
	client3 := client.NewDlockClient("client3-"+t.Name(), cluster.ServiceConfig())

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

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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
	assert.Assert(t, t1 < r.t)
}

func TestDlockServiceBlockingLockWithUnlock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	client2 := client.NewDlockClient("client2-"+t.Name(), cluster.ServiceConfig())

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

func TestDlockServiceContention(t *testing.T) {
	cluster, _ := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	clients := make([]*client.DlockClient, 0)
	for i := range 50 {
		clients = append(clients, client.NewDlockClient(fmt.Sprintf("client%d-%s", i, t.Name()), cluster.ServiceConfig()))
	}

	var mut sync.RWMutex
	tokens := make([]uint64, 0)
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()

			writer := rand.Intn(5) == 1 // Make 20% writers.
			if writer {
				tok, err := c.Lock("r1", 1*time.Second, 5*time.Second)
				assert.NilError(t, err)

				assert.Assert(t, mut.TryLock())

				if len(tokens) > 0 {
					assert.Assert(t, tokens[len(tokens)-1] < tok)
				}
				tokens = append(tokens, tok)

				mut.Unlock()

				ok, err := c.Unlock("r1", tok)
				assert.NilError(t, err)
				assert.Assert(t, ok)
			} else {
				tok, err := c.RLock("r1", 1*time.Second, 5*time.Second)
				assert.NilError(t, err)

				assert.Assert(t, mut.TryRLock())

				if len(tokens) > 0 {
					assert.Assert(t, tokens[len(tokens)-1] == tok)
				}

				mut.RUnlock()

				ok, err := c.RUnlock("r1", tok)
				assert.NilError(t, err)
				assert.Assert(t, ok)
			}
		}()
	}

	wg.Wait()
}

func TestDlockServiceFairLock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	tok, ok, err := client1.TryLock("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	clients := make([]*client.DlockClient, 0)
	for i := range 20 {
		clients = append(clients, client.NewDlockClient(fmt.Sprintf("client%d-%s", i, t.Name()), cluster.ServiceConfig()))
	}

	for _, c := range clients {
		_, ok, err = c.TryLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, !ok)
	}

	ok, err = client1.Unlock("r1", tok)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	for i := range clients {
		// Lock is only given to earliest requesting client.

		if i > 0 {
			ok, err := clients[i-1].Unlock("r1", tok)
			assert.NilError(t, err)
			assert.Assert(t, ok)
		}

		for j := i + 1; j < len(clients); j++ {
			_, ok, err = clients[j].TryLockFair("r1", 1*time.Minute)
			assert.NilError(t, err, j)
			assert.Assert(t, !ok, j)
		}

		tok2, ok, err := clients[i].TryLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, ok)
		assert.Assert(t, tok < tok2)
		tok = tok2
	}
}

// If you're reading this, brace yourself! This is a long-ass test.
func TestDlockServiceFairLockWithRLock(t *testing.T) {
	cluster, client1 := NewClusterClient(t, 3, service.SystemClock())
	defer cluster.Shutdown()

	readers1 := make([]*client.DlockClient, 0)
	for i := range 10 {
		readers1 = append(readers1, client.NewDlockClient(fmt.Sprintf("client-r1-%d-%s", i, t.Name()), cluster.ServiceConfig()))
	}

	writer1 := client.NewDlockClient(fmt.Sprintf("client-w1-%s", t.Name()), cluster.ServiceConfig())
	writer2 := client.NewDlockClient(fmt.Sprintf("client-w2-%s", t.Name()), cluster.ServiceConfig())

	readers2 := make([]*client.DlockClient, 0)
	for i := range 10 {
		readers2 = append(readers2, client.NewDlockClient(fmt.Sprintf("client-r2-%d-%s", i, t.Name()), cluster.ServiceConfig()))
	}

	tok, ok, err := client1.TryLock("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	// Establish queue order: readers1, writer1, writer2, readers2.

	for _, c := range readers1 {
		_, ok, err = c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, !ok)
	}

	_, ok, err = writer1.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	_, ok, err = writer2.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	for _, c := range readers2 {
		_, ok, err = c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, !ok)
	}

	// Make all readers1 acquire the lock.

	ok, err = client1.Unlock("r1", tok)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	for _, c := range readers2 {
		_, ok, err = c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, !ok)
	}

	_, ok, err = writer2.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	_, ok, err = writer1.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	for _, c := range readers1 {
		tok2, ok, err := c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, ok)
		assert.Assert(t, tok == tok2)
	}

	// Make writer1 acquire the lock.

	for _, c := range readers1 {
		ok, err := c.RUnlock("r1", tok)
		assert.NilError(t, err)
		assert.Assert(t, ok)
	}

	for _, c := range readers2 {
		_, ok, err := c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, !ok)
	}

	_, ok, err = writer2.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, !ok)

	tok2, ok, err := writer1.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, tok < tok2)

	// Make writer2 acquire the lock.

	ok, err = writer1.Unlock("r1", tok2)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	for _, c := range readers2 {
		_, ok, err = c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, !ok)
	}

	tok3, ok, err := writer2.TryLockFair("r1", 1*time.Minute)
	assert.NilError(t, err)
	assert.Assert(t, ok)
	assert.Assert(t, tok2 < tok3)

	// Make readers2 acquire the lock

	ok, err = writer2.Unlock("r1", tok3)
	assert.NilError(t, err)
	assert.Assert(t, ok)

	for _, c := range readers2 {
		tok, ok, err = c.TryRLockFair("r1", 1*time.Minute)
		assert.NilError(t, err)
		assert.Assert(t, ok)
		assert.Assert(t, tok3 == tok)
	}
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

func NewClusterClient(t *testing.T, nodeCount int, clock service.Clock) (*testutil.Cluster[*service.DlockService], *client.DlockClient) {
	_, err := zap.NewDevelopment()
	assert.NilError(t, err)
	cluster, err := testutil.StartLocalCluster[*service.DlockService](
		testutil.ClusterConfig[*service.DlockService]{
			Dir:                       t.TempDir(),
			NodeCount:                 nodeCount,
			HeartbeatMillis:           50,
			ElectionTimeoutLowMillis:  150,
			ElectionTimeoutHighMillis: 300,
			ServiceFactory: func(address string, config graft.Config) (*service.DlockService, error) {
				return service.NewDlockService(address, clock, config)
			},
			Logger: zap.NewNop(),
		},
	)

	if err != nil {
		t.Fatalf("Couldn't start cluster: %v", err)
	}

	client := client.NewDlockClient("client-"+t.Name(), cluster.ServiceConfig())

	err = client.CheckHealthy()
	if err != nil {
		t.Fatalf("Couldn't connect to cluster: %v", err)
	}

	return cluster, client
}
