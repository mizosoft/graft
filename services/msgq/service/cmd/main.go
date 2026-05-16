package main

import (
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/msgq/service"
)

type Factory struct{}

func (f Factory) Create(address string, batchInterval time.Duration, config graft.Config) (*server.Server[service.MsgqCommand], error) {
	return service.NewMsgqServer(address, batchInterval, config)
}

func main() {
	server.RunServer[service.MsgqCommand]("msgq", Factory{})
}
