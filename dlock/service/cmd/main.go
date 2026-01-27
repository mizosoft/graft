package main

import (
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/dlock/service"
	"github.com/mizosoft/graft/infra"
	"github.com/mizosoft/graft/infra/server"
)

func main() {
	server.RunServer("dlock", func(address string, batchInterval time.Duration, config graft.Config) (*server.Server, error) {
		return service.NewDlockServer(address, batchInterval, infra.SystemClock(), config)
	})
}
