package graft

import (
	"fmt"
	"reflect"
	"time"

	"go.uber.org/zap"
)

type IntRange struct {
	Low  int
	High int
}

func (r IntRange) String() string {
	return fmt.Sprintf("IntRange{Low: %d, High: %d}", r.Low, r.High)
}

type RpcTimeouts struct {
	RequestVote     time.Duration
	AppendEntries   time.Duration
	InstallSnapshot time.Duration
}

func (r RpcTimeouts) String() string {
	return fmt.Sprintf("RpcTimeouts{RequestVote: %v, AppendEntries: %v, InstallSnapshot: %v}",
		r.RequestVote, r.AppendEntries, r.InstallSnapshot)
}

type Config struct {
	Id                    string
	ClusterUrls           map[string]string
	ElectionTimeoutMillis IntRange
	HeartbeatMillis       int
	Persistence           Persistence
	RpcTimeouts           RpcTimeouts
	Logger                *zap.Logger
}

func (c Config) Validate() error {
	if c.Id == "" {
		return fmt.Errorf("Id is required")
	}
	if c.Id == UnknownLeader {
		return fmt.Errorf("Id cannot be %q", UnknownLeader)
	}
	if len(c.ClusterUrls) == 0 {
		return fmt.Errorf("ClusterUrls is required")
	}
	if _, ok := c.ClusterUrls[c.Id]; !ok {
		return fmt.Errorf("ClusterUrls must contain the node's own Id %q", c.Id)
	}
	if c.ElectionTimeoutMillis.Low <= 0 || c.ElectionTimeoutMillis.High <= 0 {
		return fmt.Errorf("ElectionTimeoutMillis.Low and High must be positive: %v", c.ElectionTimeoutMillis)
	}
	if c.ElectionTimeoutMillis.Low > c.ElectionTimeoutMillis.High {
		return fmt.Errorf("ElectionTimeoutMillis.Low must be <= High: %v", c.ElectionTimeoutMillis)
	}
	if c.HeartbeatMillis <= 0 {
		return fmt.Errorf("HeartbeatMillis must be positive: %d", c.HeartbeatMillis)
	}
	if c.Persistence == nil {
		return fmt.Errorf("Persistence is required")
	}
	return nil
}

func (c Config) LoggerOrNoop() *zap.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return zap.NewNop()
}

func (c Config) RpcTimeoutsWithDefaults() RpcTimeouts {
	// Return configured timeouts, filling in zeros with defaults.
	timeouts := c.RpcTimeouts
	if timeouts.RequestVote == 0 {
		timeouts.RequestVote = 200 * time.Millisecond
	}
	if timeouts.AppendEntries == 0 {
		timeouts.AppendEntries = 100 * time.Millisecond
	}
	if timeouts.InstallSnapshot == 0 {
		timeouts.InstallSnapshot = 30 * time.Second
	}
	return timeouts
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Confg{Id: %s, Url: %s, ClusterUrls: %v, ElectionTimeoutMillis: %v, HeartbeatMillis: %v, Persistence: %s, RpcTimeouts: %s}",
		c.Id, c.ClusterUrls, c.ElectionTimeoutMillis, c.HeartbeatMillis, reflect.TypeOf(c.Persistence), c.RpcTimeouts)
}
