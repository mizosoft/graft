package infra

type ConfigUpdateRequest struct {
	Add    map[string]string `json:"add"`
	Remove []string          `json:"remove"`
}

type ConfigResponse struct {
	Config map[string]string `json:"config"`
}

type NotLeaderResponse struct {
	LeaderId string `json:"leaderIdd"`
}
