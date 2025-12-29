package server

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/mizosoft/graft"
	"go.uber.org/zap"
)

type node struct {
	server       *Server
	config       NodeConfig
	startErrChan chan error
	mut          sync.Mutex
}

func (n *node) Start() {
	n.server.Start()
}

func (n *node) Close() error {
	errChan := make(chan error)
	go func() {
		errChan <- n.server.Close()
	}()

	select {
	case err := <-errChan:
		if err != nil {
			log.Println("ListenAndServe error: ", err)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for node (%s) to close", n.config.Id)
	}
	return nil
}

type NodeConfig struct {
	Dir                   string
	Id                    string
	Address               string
	GraftAddresses        map[string]string
	Logger                *zap.Logger
	HeartbeatMillis       int
	ElectionTimeoutMillis graft.IntRange
	ServerFactory         func(address string, config graft.Config) (*Server, error)
	PersistenceFactory    func(dir string) (graft.Persistence, error)
}

func newNode(config NodeConfig) (*node, error) {
	persistence, err := config.PersistenceFactory(config.Dir)
	if err != nil {
		return nil, err
	}

	server, err := config.ServerFactory(config.Address, graft.Config{
		Id:                    config.Id,
		ClusterUrls:           config.GraftAddresses,
		ElectionTimeoutMillis: config.ElectionTimeoutMillis,
		HeartbeatMillis:       config.HeartbeatMillis,
		Persistence:           persistence,
		Logger:                config.Logger,
	})
	if err != nil {
		return nil, err
	}

	return &node{
		server: server,
		config: config,
	}, nil
}

type Cluster struct {
	nodes         map[string]*node
	serverFactory func(address string, config graft.Config) (*Server, error)
}

func (c *Cluster) ServiceConfig() map[string]string {
	config := make(map[string]string)
	for id, node := range c.nodes {
		config[id] = node.server.Address()
	}
	return config
}

func (c *Cluster) Restart(id string) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("node not found: %s", id)
	}

	err := n.Close()
	if err != nil {
		log.Printf("Error shutting down node: %v", err)
	}

	newN, err := newNode(n.config)
	if err != nil {
		return err
	}
	c.nodes[id] = newN
	newN.Start()
	return nil
}

func (c *Cluster) Shutdown() {
	var errs []error
	for _, node := range c.nodes {
		err := node.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		log.Panic(errors.Join(errs...))
	}
}

type ClusterConfig struct {
	Dir                   string
	NodeCount             int
	HeartbeatMillis       int
	ElectionTimeoutMillis graft.IntRange
	ServerFactory         func(address string, config graft.Config) (*Server, error)
	PersistenceFactory    func(dir string) (graft.Persistence, error)
	Logger                *zap.Logger
}

func StartLocalCluster(config ClusterConfig) (*Cluster, error) {
	cluster := &Cluster{
		nodes:         make(map[string]*node),
		serverFactory: config.ServerFactory,
	}

	graftAddresses := make(map[string]string)
	for i := range config.NodeCount {
		graftAddresses[strconv.Itoa(i)] = "127.0.0.1:" + strconv.Itoa(5555+i)
	}

	if config.PersistenceFactory == nil {
		config.PersistenceFactory = func(dir string) (graft.Persistence, error) {
			return graft.OpenWal(graft.WalOptions{
				Dir:             dir,
				SegmentSize:     64 * 1024 * 1024,
				SuffixCacheSize: 1 * 1024 * 1024,
				MemoryMapped:    true,
				Logger:          config.Logger,
			})
		}
	}

	for i := range config.NodeCount {
		id := strconv.Itoa(i)
		dir := filepath.Join(config.Dir, strconv.Itoa(i))
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}

		n, err := newNode(NodeConfig{
			Dir:                   dir,
			Id:                    id,
			Address:               "127.0.0.1:" + strconv.Itoa(2634+i),
			GraftAddresses:        graftAddresses,
			Logger:                config.Logger,
			HeartbeatMillis:       config.HeartbeatMillis,
			ElectionTimeoutMillis: config.ElectionTimeoutMillis,
			ServerFactory:         config.ServerFactory,
			PersistenceFactory:    config.PersistenceFactory,
		})
		if err != nil {
			return nil, err
		}

		cluster.nodes[id] = n
	}

	for _, node := range cluster.nodes {
		node.Start()
	}
	return cluster, nil
}
