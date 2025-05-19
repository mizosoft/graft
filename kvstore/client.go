package kvstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/server"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type KvClient struct {
	id            string
	leaderId      string
	url           string
	http          *http.Client
	serviceConfig map[string]string
	mut           sync.Mutex
}

func (c *KvClient) Get(key string) (string, bool, error) {
	return c.get(key, true)
}

func (c *KvClient) FastGet(key string) (string, bool, error) {
	return c.get(key, false)
}

func (c *KvClient) get(key string, linearizable bool) (string, bool, error) {
	res, err := Post[GetResponse](c, "get", GetRequest{
		ClientId:     c.id,
		Key:          key,
		Linearizable: linearizable,
	})
	if err != nil {
		return "", false, err
	}
	return res.Value, res.Exists, nil
}

func (c *KvClient) Put(key string, value string) (string, bool, error) {
	res, err := Post[PutResponse](c, "put", PutRequest{
		ClientId: c.id,
		Key:      key,
		Value:    value,
	})
	if err != nil {
		return "", false, err
	}
	return res.PreviousValue, res.Exists, nil
}

func (c *KvClient) PutIfAbsent(key string, value string) (bool, error) {
	res, err := Post[PutIfAbsentResponse](c, "putIfAbsent", PutRequest{
		ClientId: c.id,
		Key:      key,
		Value:    value,
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *KvClient) Delete(key string) (string, bool, error) {
	res, err := Post[DeleteResponse](c, "delete", DeleteRequest{
		ClientId: c.id,
		Key:      key,
	})
	if err != nil {
		return "", false, err
	}
	return res.Value, res.Exists, nil
}

func (c *KvClient) Append(key string, value string) (int, error) {
	res, err := Post[AppendResponse](c, "append", AppendRequest{
		ClientId: c.id,
		Key:      key,
		Value:    value,
	})
	if err != nil {
		return -1, err
	}
	return res.Length, nil
}

func (c *KvClient) Cas(key string, expectedValue string, value string) (bool, string, error) {
	res, err := Post[CasResponse](c, "cas", CasRequest{
		ClientId:      c.id,
		Key:           key,
		ExpectedValue: expectedValue,
		Value:         value,
	})
	if err != nil {
		return false, "", err
	}

	var currValue string
	if res.Exists {
		currValue = res.Value
	} else {
		currValue = ""
	}

	return res.Success, currValue, nil
}

func (c *KvClient) CheckHealthy() error {
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

func (c *KvClient) LeaderId() string {
	return c.leaderId
}

func Post[T any](c *KvClient, path string, body interface{}) (T, error) {
	bodyJson, err := json.Marshal(body)
	if err != nil {
		return *new(T), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

retry:
	u, err := url.JoinPath(c.url, path)
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
				c.leaderId = config.LeaderId
				c.url = "http://" + c.serviceConfig[config.LeaderId] + "/"
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

func NewKvClient(id string, serviceConfig map[string]string) *KvClient {
	var anyId string
	var anyAddress string
	for id, address := range serviceConfig {
		anyAddress = address
		anyId = id
		break
	}
	return &KvClient{
		id:            id,
		serviceConfig: serviceConfig,
		url:           "http://" + anyAddress + "/",
		leaderId:      anyId,
		http:          &http.Client{},
	}
}
