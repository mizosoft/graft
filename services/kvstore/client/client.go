package client

import (
	"github.com/mizosoft/graft/infra/client"
	"github.com/mizosoft/graft/kvstore/api"
)

type KvClient struct {
	client *client.Client
}

func (c *KvClient) CheckHealthy() error {
	return c.client.CheckHealthy()
}

func (c *KvClient) LeaderId() string {
	return c.client.LeaderId()
}

func (c *KvClient) Get(key string) (string, bool, error) {
	return c.get(key, true)
}

func (c *KvClient) FastGet(key string) (string, bool, error) {
	return c.get(key, false)
}

func (c *KvClient) get(key string, linearizable bool) (string, bool, error) {
	res, err := client.Post[api.GetResponse](c.client, "get", api.GetRequest{
		ClientId:     c.client.Id(),
		Key:          key,
		Linearizable: linearizable,
	})
	if err != nil {
		return "", false, err
	}
	return res.Value, res.Exists, nil
}

func (c *KvClient) Put(key string, value string) (string, bool, error) {
	res, err := client.Post[api.PutResponse](c.client, "put", api.PutRequest{
		ClientId: c.client.Id(),
		Key:      key,
		Value:    value,
	})
	if err != nil {
		return "", false, err
	}
	return res.PreviousValue, res.Exists, nil
}

func (c *KvClient) PutIfAbsent(key string, value string) (bool, error) {
	res, err := client.Post[api.PutIfAbsentResponse](c.client, "putIfAbsent", api.PutRequest{
		ClientId: c.client.Id(),
		Key:      key,
		Value:    value,
	})
	if err != nil {
		return false, err
	}
	return res.Success, nil
}

func (c *KvClient) Delete(key string) (string, bool, error) {
	res, err := client.Post[api.DeleteResponse](c.client, "delete", api.DeleteRequest{
		ClientId: c.client.Id(),
		Key:      key,
	})
	if err != nil {
		return "", false, err
	}
	return res.Value, res.Exists, nil
}

func (c *KvClient) Append(key string, value string) (int, error) {
	res, err := client.Post[api.AppendResponse](c.client, "append", api.AppendRequest{
		ClientId: c.client.Id(),
		Key:      key,
		Value:    value,
	})
	if err != nil {
		return -1, err
	}
	return res.Length, nil
}

func (c *KvClient) Cas(key string, expectedValue string, value string) (bool, string, error) {
	res, err := client.Post[api.CasResponse](c.client, "cas", api.CasRequest{
		ClientId:      c.client.Id(),
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

func NewKvClient(id string, serviceConfig map[string]string) *KvClient {
	return &KvClient{
		client: client.New(id, serviceConfig),
	}
}
