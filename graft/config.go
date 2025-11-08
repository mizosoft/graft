package graft

import (
	"fmt"
	"reflect"

	"go.uber.org/zap"
)

type IntRange struct {
	Low  int
	High int
}

func (r IntRange) String() string {
	return fmt.Sprintf("IntRange{Low: %d, High: %d}", r.Low, r.High)
}

type Config struct {
	Id                    string
	Addresses             map[string]string
	ElectionTimeoutMillis IntRange
	HeartbeatMillis       int
	Persistence           Persistence
	Logger                *zap.Logger
}

func (c Config) LoggerOrNoop() *zap.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return zap.NewNop()
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Confg{Id: %s, Addresses: %v, ElectionTimeoutMillis: %v, HeartbeatMillis: %v, Persistence: %s}",
		c.Id, c.Addresses, c.ElectionTimeoutMillis, c.HeartbeatMillis, reflect.TypeOf(c.Persistence))
}
