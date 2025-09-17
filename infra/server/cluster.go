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

type Service interface {
	Id() string

	Address() string

	ListenAndServe() error

	Shutdown()
}

type node[T Service] struct {
	service T
	config  NodeConfig[T] // For restarting.
	errChan chan error
	mut     sync.Mutex
}

func (n *node[T]) Start() {
	go func() {
		err := n.service.ListenAndServe()

		n.mut.Lock()
		defer n.mut.Unlock()

		if n.errChan != nil {
			n.errChan <- err
			close(n.errChan)
		} else {
			log.Panicf("Unexpected ListenAndServe err: %v", err)
		}
	}()
}

func (n *node[T]) Shutdown() error {
	ch := func() chan error {
		n.mut.Lock()
		defer n.mut.Unlock()

		if n.errChan == nil {
			n.errChan = make(chan error)
		}
		return n.errChan
	}()

	go n.service.Shutdown()

	select {
	case err := <-ch:
		log.Println("Error shutting down node: ", err)
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for node (%s) to shutdown", n.service.Id())
	}
	return nil
}

type NodeConfig[T Service] struct {
	Dir                       string
	Id                        string
	Address                   string
	GraftAddresses            map[string]string
	Logger                    *zap.Logger
	HeartbeatMillis           int
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
	ServiceFactory            func(address string, config graft.Config) (T, error)
	PersistenceFactory        func(dir string) (graft.Persistence, error)
}

func newNode[T Service](config NodeConfig[T]) (*node[T], error) {
	persistence, err := config.PersistenceFactory(config.Dir)
	if err != nil {
		return nil, err
	}

	service, err := config.ServiceFactory(config.Address, graft.Config{
		Id:                        config.Id,
		Addresses:                 config.GraftAddresses,
		ElectionTimeoutLowMillis:  config.ElectionTimeoutLowMillis,
		ElectionTimeoutHighMillis: config.ElectionTimeoutHighMillis,
		HeartbeatMillis:           config.HeartbeatMillis,
		Persistence:               persistence,
		Logger:                    config.Logger,
	})
	if err != nil {
		return nil, err
	}
	return &node[T]{
		service: service,
		config:  config,
	}, nil
}

type Cluster[T Service] struct {
	nodes          map[string]*node[T]
	serviceFactory func(address string, config graft.Config) (T, error)
}

func (c *Cluster[T]) ServiceConfig() map[string]string {
	config := make(map[string]string)
	for id, node := range c.nodes {
		config[id] = node.service.Address()
	}
	return config
}

func (c *Cluster[T]) Restart(id string) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("node not found: %s", id)
	}

	err := n.Shutdown()
	if err != nil {
		log.Printf("Error shutting down node: %v", err)
	}

	newN, err := newNode[T](n.config)
	if err != nil {
		return err
	}
	c.nodes[id] = newN
	newN.Start()
	return nil
}

func (c *Cluster[T]) Shutdown() {
	var errs []error
	for _, node := range c.nodes {
		err := node.Shutdown()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		log.Panic(errors.Join(errs...))
	}
}

type ClusterConfig[T Service] struct {
	Dir                       string
	NodeCount                 int
	HeartbeatMillis           int
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
	ServiceFactory            func(address string, config graft.Config) (T, error)
	PersistenceFactory        func(dir string) (graft.Persistence, error)
	Logger                    *zap.Logger
}

func StartLocalCluster[T Service](config ClusterConfig[T]) (*Cluster[T], error) {
	cluster := &Cluster[T]{
		nodes:          make(map[string]*node[T]),
		serviceFactory: config.ServiceFactory,
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

		n, err := newNode(NodeConfig[T]{
			Dir:                       dir,
			Id:                        id,
			Address:                   "127.0.0.1:" + strconv.Itoa(1569+i),
			GraftAddresses:            graftAddresses,
			Logger:                    config.Logger,
			HeartbeatMillis:           config.HeartbeatMillis,
			ElectionTimeoutLowMillis:  config.ElectionTimeoutLowMillis,
			ElectionTimeoutHighMillis: config.ElectionTimeoutHighMillis,
			ServiceFactory:            config.ServiceFactory,
			PersistenceFactory:        config.PersistenceFactory,
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
