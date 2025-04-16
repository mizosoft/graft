package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/mizosoft/graft"
)

func main() {
	nilId := ""
	id := flag.String("id", nilId, "Id of this server")

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

	config := graft.Config{
		Id:                        *id,
		Addresses:                 addresses,
		ElectionTimeoutLowMillis:  150,
		ElectionTimeoutHighMillis: 300,
		HeartbeatMillis:           50,
		Persistence:               graft.NullPersistence(),
		Committed: func(entries []graft.CommittedEntry) {
			fmt.Printf("Committed: %v\n", entries)
		},
	}

	g, e := graft.New(config)
	if e != nil {
		panic(e)
	}

	go func() {
		gErr := g.Serve()
		fmt.Println(gErr)
	}()

	for {
		var input string
		fmt.Print("(y) to send a command, (n) to exit: ")
		fmt.Scanln(&input) // Reads input until the first space or newline

		if input == "n" {
			return
		}

		count := rand.Intn(10)
		commands := make([][]byte, count)
		for i := range count {
			commands[i] = []byte("a")
		}
		fmt.Printf("Appending %v entries\n", commands)
		g.Append(commands)
	}
}
