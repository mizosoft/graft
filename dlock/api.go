package dlock

type LockRequest struct {
	ClientId  string `json:"clientId"`
	Resource  string `json:"resource"`
	TtlMillis int64  `json:"ttlMillis"`
}

type LockResponse struct {
	Success bool   `json:"success"`
	Token   uint64 `json:"token"`
}

type UnlockRequest struct {
	ClientId string `json:"clientId"`
	Resource string `json:"resource"`
	Token    uint64 `json:"token"`
}

type UnlockResponse struct {
	Success bool `json:"success"`
}

type RefreshRequest struct {
	ClientId  string `json:"clientId"`
	Resource  string `json:"resource"`
	Token     uint64 `json:"token"`
	TtlMillis int64  `json:"ttlMillis"`
}

type RefreshResponse struct {
	Success bool `json:"success"`
}
