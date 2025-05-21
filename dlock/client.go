package dlock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/server"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type DlockClient struct {
	id            string
	leaderId      string
	url           string
	http          *http.Client
	serviceConfig map[string]string
	mut           sync.Mutex
}

func (c *DlockClient) TryLock(resource string, ttl time.Duration) (uint64, bool, error) {
	res, err := Post[LockResponse](c, "lock", LockRequest{
		ClientId:  c.id,
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return 0, false, err
	}
	return res.Token, res.Success, nil
}

func (c *DlockClient) TryRLock(resource string, ttl time.Duration) (uint64, bool, error) {
	res, err := Post[LockResponse](c, "rlock", LockRequest{
		ClientId:  c.id,
		Resource:  resource,
		TtlMillis: int64(ttl / time.Millisecond),
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
	base := 10 * time.Millisecond
	const mx = 5 * time.Second
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	} else {
		timeoutChan = make(<-chan time.Time)
	}
retry:
	res, err := Post[LockResponse](c, path, LockRequest{
		ClientId:  c.id,
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
	case <-time.After(backoff(base, mx, retryCount)):
		retryCount++
		goto retry
	case <-timeoutChan:
		return 0, errors.New("timeout")
	}
}

func backoff(base time.Duration, mx time.Duration, retry int) time.Duration {
	sleep := min(float64(mx), float64(base)*math.Pow(2, float64(retry)))
	jitter := 0.2 * rand.Float64() * sleep // 20% jitter
	return time.Duration(sleep + jitter)
}

func (c *DlockClient) Unlock(resource string, token uint64) (bool, error) {
	res, err := Post[UnlockResponse](c, "unlock", UnlockRequest{
		ClientId: c.id,
		Resource: resource,
		Token:    token,
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *DlockClient) RUnlock(resource string, token uint64) (bool, error) {
	res, err := Post[UnlockResponse](c, "runlock", UnlockRequest{
		ClientId: c.id,
		Resource: resource,
		Token:    token,
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *DlockClient) RefreshLock(resource string, token uint64, ttl time.Duration) (bool, error) {
	res, err := Post[RefreshResponse](c, "refreshLock", RefreshRequest{
		ClientId:  c.id,
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
	res, err := Post[RefreshResponse](c, "refreshRLock", RefreshRequest{
		ClientId:  c.id,
		Resource:  resource,
		Token:     token,
		TtlMillis: int64(ttl / time.Millisecond),
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *DlockClient) CheckHealthy() error {
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

func (c *DlockClient) LeaderId() string {
	return c.leaderId
}

func Post[T any](d *DlockClient, path string, body interface{}) (T, error) {
	bodyJson, err := json.Marshal(body)
	if err != nil {
		return *new(T), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

retry:
	u, err := url.JoinPath(d.url, path)
	if err != nil {
		return *new(T), err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(bodyJson))
	if err != nil {
		return *new(T), err
	}
	request.Header.Set("Content-Type", "application/json")

	res, err := d.http.Do(request)
	if err != nil {
		select {
		case <-ctx.Done():
			return *new(T), err
		case <-time.After(50 * time.Millisecond): // backoff
			goto retry
		}
	}

	if res.StatusCode != http.StatusOK {
		if res.StatusCode == http.StatusForbidden && res.Header.Get("Content-Type") == "application/json" {
			// Rediscover leader.
			config, err := decodeJson[server.NotLeaderResponse](res)
			if err != nil {
				return *new(T), err
			}

			if config.LeaderId != graft.UnknownLeader && len(config.LeaderId) > 0 {
				d.leaderId = config.LeaderId
				d.url = "http://" + d.serviceConfig[config.LeaderId] + "/"
			}

			goto retry // Retry with new leader
		}

		return *new(T), fmt.Errorf("invalid response from server: %v", res.StatusCode)
	}

	return decodeJson[T](res)
}

func decodeJson[T any](response *http.Response) (T, error) {
	defer response.Body.Close()

	var result T
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return *new(T), err
	}
	return result, nil
}

func NewDlockClient(id string, serviceConfig map[string]string) *DlockClient {
	var anyId string
	var anyAddress string
	for id, address := range serviceConfig {
		anyAddress = address
		anyId = id
		break
	}
	return &DlockClient{
		id:            id,
		serviceConfig: serviceConfig,
		url:           "http://" + anyAddress + "/",
		leaderId:      anyId,
		http:          &http.Client{},
	}
}
