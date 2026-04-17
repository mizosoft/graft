package api

type EnqueueRequest struct {
	ClientId string `json:"clientId"`
	Topic    string `json:"topic"`
	Data     string `json:"data"`
}

type EnqueueResponse struct {
	Id string `json:"id"`
}

type DequeRequest struct {
	ClientId string `json:"clientId"`
	Topic    string `json:"topic"`
}

type Message struct {
	Id   string `json:"id"`
	Data string `json:"data"`
}

type DequeResponse struct {
	Success bool    `json:"success"`
	Topic   string  `json:"topic"`
	Message Message `json:"message"`
}
