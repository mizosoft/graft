package main

import (
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore/service"
)

type Factory struct{}

func (f Factory) Create(address string, batchInterval time.Duration, config graft.Config) (*server.Server[service.KvCommand], error) {
	return service.NewKvServer(address, batchInterval, config)
}

func main() {
	server.RunServer[service.KvCommand]("kvstore", Factory{})
}
