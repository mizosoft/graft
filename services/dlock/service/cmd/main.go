package main

import (
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/dlock/service"
	"github.com/mizosoft/graft/infra/server"
)

type Factory struct{}

func (f Factory) Create(address string, batchInterval time.Duration, config graft.Config) (*server.Server[service.LockCommand], error) {
	return service.NewDlockServer(address, batchInterval, server.SystemClock(), config)
}

func main() {
	server.RunServer[service.LockCommand]("dlock", Factory{})
}
