package client

import (
	"errors"
	"github.com/mizosoft/graft/dlock/api"
	"github.com/mizosoft/graft/infra/client"
	"math"
	"math/rand"
	"time"
)

type DlockClient struct {
	client *client.Client
}

func (c *DlockClient) CheckHealthy() error {
	return c.client.CheckHealthy()
}

func (c *DlockClient) LeaderId() string {
	return c.client.LeaderId()
}

func (c *DlockClient) TryLock(resource string, ttl time.Duration) (uint64, bool, error) {
	res, err := client.Post[api.LockResponse](c.client, "lock", api.LockRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return 0, false, err
	}
	return res.Token, res.Success, nil
}

func (c *DlockClient) TryRLock(resource string, ttl time.Duration) (uint64, bool, error) {
	res, err := client.Post[api.LockResponse](c.client, "rlock", api.LockRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return 0, false, err
	}
	return res.Token, res.Success, nil
}

func (c *DlockClient) TryLockFair(resource string, ttl time.Duration) (uint64, bool, error) {
	res, err := client.Post[api.LockResponse](c.client, "lock", api.LockRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
		Fair:      true,
	})
	if err != nil {
		return 0, false, err
	}
	return res.Token, res.Success, nil
}

func (c *DlockClient) TryRLockFair(resource string, ttl time.Duration) (uint64, bool, error) {
	res, err := client.Post[api.LockResponse](c.client, "rlock", api.LockRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
		Fair:      true,
	})
	if err != nil {
		return 0, false, err
	}
	return res.Token, res.Success, nil
}

func (c *DlockClient) Lock(resource string, ttl time.Duration, timeout time.Duration) (uint64, error) {
	return c.blockingLock(resource, ttl, timeout, "lock")
}

func (c *DlockClient) RLock(resource string, ttl time.Duration, timeout time.Duration) (uint64, error) {
	return c.blockingLock(resource, ttl, timeout, "rlock")
}

func (c *DlockClient) blockingLock(resource string, ttl time.Duration, timeout time.Duration, path string) (uint64, error) {
	retryCount := 0
	baseDelay := 10 * time.Millisecond
	const maxDelay = 5 * time.Second
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	} else {
		timeoutChan = make(<-chan time.Time) // Will never complete.
	}

retry:
	res, err := client.Post[api.LockResponse](c.client, path, api.LockRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return 0, err
	}

	if res.Success {
		return res.Token, nil
	}

	select {
	case <-time.After(backoff(baseDelay, maxDelay, retryCount)):
		retryCount++
		goto retry
	case <-timeoutChan:
		return 0, errors.New("timeout")
	}
}

func backoff(base time.Duration, mx time.Duration, retry int) time.Duration {
	sleep := min(float64(mx), float64(base)*math.Pow(1.5, float64(retry)))
	jitter := 0.2 * rand.Float64() * sleep // 20% jitter
	return time.Duration(sleep + jitter)
}

func (c *DlockClient) Unlock(resource string, token uint64) (bool, error) {
	res, err := client.Post[api.UnlockResponse](c.client, "unlock", api.UnlockRequest{
		ClientId: c.client.Id(),
		Resource: resource,
		Token:    token,
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *DlockClient) RUnlock(resource string, token uint64) (bool, error) {
	res, err := client.Post[api.UnlockResponse](c.client, "runlock", api.UnlockRequest{
		ClientId: c.client.Id(),
		Resource: resource,
		Token:    token,
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *DlockClient) RefreshLock(resource string, token uint64, ttl time.Duration) (bool, error) {
	res, err := client.Post[api.RefreshResponse](c.client, "refreshLock", api.RefreshRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		Token:     token,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *DlockClient) RefreshRLock(resource string, token uint64, ttl time.Duration) (bool, error) {
	res, err := client.Post[api.RefreshResponse](c.client, "refreshRLock", api.RefreshRequest{
		ClientId:  c.client.Id(),
		Resource:  resource,
		Token:     token,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func NewDlockClient(id string, serviceConfig map[string]string) *DlockClient {
	return &DlockClient{
		client: client.New(id, serviceConfig),
	}
}
