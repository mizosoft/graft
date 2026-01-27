package main

import (
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/msgq/service"
)

func main() {
	server.RunServer("msgq", service.NewMsgqServer)
}
