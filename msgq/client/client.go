package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/msgq/service"
	"github.com/mizosoft/graft/server"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type MsgqClient struct {
	id            string
	leaderId      string
	url           string
	http          *http.Client
	serviceConfig map[string]string
	mut           sync.Mutex
}

func (c *MsgqClient) LeaderId() string {
	return c.leaderId
}

func (c *MsgqClient) Enqueue(topic string, data string) (string, error) {
	res, err := Post[service.EnqueueResponse](c, "enqueue", service.EnqueueRequest{
		ClientId: c.id,
		Topic:    topic,
		Data:     data,
	})
	if err != nil {
		return "", err
	}
	return res.Id, nil
}

func (c *MsgqClient) Dequeue(topic string) (service.Message, bool, error) {
	res, err := Post[service.DequeResponse](c, "deque", service.DequeRequest{
		ClientId: c.id,
		Topic:    topic,
	})
	if err != nil {
		return service.Message{}, false, err
	}
	return res.Message, res.Success, nil
}

func Post[T any](c *MsgqClient, path string, body interface{}) (T, error) {
	bodyJson, err := json.Marshal(body)
	if err != nil {
		return *new(T), err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func NewMsgqClient(id string, serviceConfig map[string]string) *MsgqClient {
	var anyId string
	var anyAddress string
	for id, address := range serviceConfig {
		anyAddress = address
		anyId = id
		break
	}
	return &MsgqClient{
		id:            id,
		serviceConfig: serviceConfig,
		url:           "http://" + anyAddress + "/",
		leaderId:      anyId,
		http:          &http.Client{},
	}
}
