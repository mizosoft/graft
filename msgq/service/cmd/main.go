package main

import (
	"bufio"
	"flag"
	"os"
	"strings"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/msgq/service"
	"go.uber.org/zap"
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

	wal, err := graft.OpenWal(graft.WalOptions{
		Dir:             walDir,
		SegmentSize:     64 * 1024 * 1024,
		SuffixCacheSize: 1 * 1024 * 1024,
		Logger:          zap.NewExample()})
	if err != nil {
		panic(err)
	}

	logger := zap.NewExample()

	q, err := service.NewMsgqService(*address, 0, graft.Config{
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

	q.Initialize()
	err = q.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
