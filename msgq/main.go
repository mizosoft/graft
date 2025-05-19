package main

import (
	"bufio"
	"flag"
	"github.com/mizosoft/graft"
	"go.uber.org/zap"
	"os"
	"strings"
)

func main() {
	nilId := ""
	id := flag.String("id", nilId, "Id of this server")
	address := flag.String("address", "", "Address of the keyval server")

	flag.Parse()

	if id == &nilId {
		panic("port not specified")
	}

	file, err := os.Open("config.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	addresses := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		str := strings.TrimSpace(scanner.Text())
		if len(str) > 0 {
			words := strings.Fields(scanner.Text()) // Splits on spaces and removes extra spaces.
			addresses[words[0]] = words[1]
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	walDir := "log" + *id
	if os.MkdirAll(walDir, 0777) != nil {
		panic(err)
	}

	wal, err := graft.OpenWal(walDir, 1*1024*1024, zap.NewExample()) // 1MB
	if err != nil {
		panic(err)
	}

	logger := zap.NewExample()

	g, err := graft.New(graft.Config{
		Id:                        *id,
		Addresses:                 addresses,
		ElectionTimeoutLowMillis:  1500,
		ElectionTimeoutHighMillis: 3000,
		HeartbeatMillis:           500,
		Persistence:               wal,
		Logger:                    logger,
	})
	if err != nil {
		panic(err)
	}

	kvs := newMsgq(g, logger)
	kvs.initialize()
	g.Initialize()

	go func() {
		err := g.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()

	err = kvs.listenAndServe(*address)
	if err != nil {
		panic(err)
	}
}
