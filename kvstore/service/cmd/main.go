package main

import (
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore/service"
)

func main() {
	server.RunServer("kvstore", service.NewKvServer)
}
