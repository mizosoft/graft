package client

import (
	"github.com/mizosoft/graft/infra/client"
	"github.com/mizosoft/graft/msgq/api"
)

type MsgqClient struct {
	client *client.Client
}

func (c *MsgqClient) LeaderId() string {
	return c.client.LeaderId()
}

func (c *MsgqClient) CheckHealthy() error {
	return c.client.CheckHealthy()
}

func (c *MsgqClient) Enqueue(topic string, data string) (string, error) {
	res, err := client.Post[api.EnqueueResponse](c.client, "enqueue", api.EnqueueRequest{
		ClientId: c.client.Id(),
		Topic:    topic,
		Data:     data,
	})
	if err != nil {
		return "", err
	}
	return res.Id, nil
}

func (c *MsgqClient) Dequeue(topic string) (api.Message, bool, error) {
	res, err := client.Post[api.DequeResponse](c.client, "deque", api.DequeRequest{
		ClientId: c.client.Id(),
		Topic:    topic,
	})
	if err != nil {
		return api.Message{}, false, err
	}
	return res.Message, res.Success, nil
}

func NewMsgqClient(id string, serviceConfig map[string]string) *MsgqClient {
	return &MsgqClient{
		client: client.New(id, serviceConfig),
	}
}
