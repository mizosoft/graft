package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/api"
)

const (
	maxRetriesPerUrl = 3
	retryBackoff     = 50 * time.Millisecond
)

type Client struct {
	id            string
	leaderId      string
	url           string
	http          *http.Client
	serviceConfig map[string]string
	mut           sync.Mutex
}

func (c *Client) Id() string {
	return c.id
}

func (c *Client) LeaderId() string {
	return c.leaderId
}

func (c *Client) CheckHealthy() error {
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

	if res.StatusCode == 200 {
		return nil
	}
	return errors.New(res.Status)
}

func Post[T any](c *Client, path string, payload any) (T, error) {
	bodyJson, err := json.Marshal(payload)
	if err != nil {
		return *new(T), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retries := 0
	urlSwitches := 0
	maxUrlSwitches := len(c.serviceConfig)

retry:
	c.mut.Lock()
	u, err := url.JoinPath(c.url, path)
	c.mut.Unlock()

	if err != nil {
		return *new(T), err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(bodyJson))
	if err != nil {
		return *new(T), err
	}
	request.Header.Set("Content-Type", "application/json")

	res, err := c.http.Do(request)
	if err != nil {
		retries++
		if retries >= maxRetriesPerUrl {
			// Too many retries on this URL, try another
			if urlSwitches < maxUrlSwitches && c.switchToNextUrl() {
				retries = 0
				urlSwitches++
			}
		}

		select {
		case <-ctx.Done():
			return *new(T), err
		case <-time.After(retryBackoff):
			goto retry
		}
	}

	if res.StatusCode == 200 {
		return decodeJson[T](res)
	}

	if res.StatusCode == http.StatusForbidden && res.Header.Get("Content-Type") == "application/json" {
		// Rediscover leader.
		config, err := decodeJson[api.NotLeaderResponse](res)
		if err != nil {
			return *new(T), err
		}

		if config.LeaderId != graft.UnknownLeader && len(config.LeaderId) > 0 {
			c.mut.Lock()
			c.leaderId = config.LeaderId
			if addr, ok := c.serviceConfig[config.LeaderId]; ok {
				c.url = "http://" + addr + "/"
			}
			c.mut.Unlock()
			retries = 0 // Reset retries when redirected to leader
		} else {
			// No leader known, try switching to another URL
			retries++
			if retries >= maxRetriesPerUrl {
				if urlSwitches < maxUrlSwitches && c.switchToNextUrl() {
					retries = 0
					urlSwitches++
				}
			}
		}

		select {
		case <-ctx.Done():
			return *new(T), errors.New("timeout while looking up leader")
		case <-time.After(retryBackoff):
			goto retry
		}
	}

	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)

	var body string
	if err != nil {
		body = err.Error()
	} else {
		body = string(data)
	}

	return *new(T), fmt.Errorf("invalid response from server: %v, %s", res.StatusCode, body)
}

func decodeJson[T any](response *http.Response) (T, error) {
	defer response.Body.Close()

	var result T
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return *new(T), err
	}
	return result, nil
}

// switchToNextUrl switches to a different URL from the service config.
// Returns true if switched, false if no other URLs available.
func (c *Client) switchToNextUrl() bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	currentUrl := c.url
	for id, address := range c.serviceConfig {
		candidateUrl := "http://" + address + "/"
		if candidateUrl != currentUrl {
			c.url = candidateUrl
			c.leaderId = id
			return true
		}
	}
	return false
}

func New(id string, serviceConfig map[string]string) *Client {
	var anyId string
	var anyAddress string
	for id, address := range serviceConfig {
		anyAddress = address
		anyId = id
		break
	}
	return &Client{
		id:            id,
		serviceConfig: serviceConfig,
		url:           "http://" + anyAddress + "/",
		leaderId:      anyId,
		http:          &http.Client{},
	}
}
