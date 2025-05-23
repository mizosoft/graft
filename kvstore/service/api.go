package service

type GetRequest struct {
	ClientId     string `json:"clientId"`
	Key          string `json:"key"`
	Linearizable bool   `json:"linearizable"`
}

type GetResponse struct {
	Exists bool   `json:"exists"`
	Value  string `json:"value"`
}

type PutRequest struct {
	ClientId string `json:"clientId"`
	Key      string `json:"key"`
	Value    string `json:"value"`
}

type PutResponse struct {
	Exists        bool   `json:"exists"`
	PreviousValue string `json:"previousValue"`
}

type CasRequest struct {
	ClientId      string `json:"clientId"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	ExpectedValue string `json:"expectedValue"`
}

type CasResponse struct {
	Success bool   `json:"success"`
	Exists  bool   `json:"exists"`
	Value   string `json:"value"`
}

type DeleteRequest struct {
	ClientId string `json:"clientId"`
	Key      string `json:"key"`
}

type DeleteResponse struct {
	Exists bool   `json:"exists"`
	Value  string `json:"value"`
}

type PutIfAbsentResponse struct {
	Success bool `json:"exists"`
}

type AppendRequest struct {
	ClientId string `json:"clientId"`
	Key      string `json:"key"`
	Value    string `json:"value"`
}

type AppendResponse struct {
	Length int `json:"length"`
}
