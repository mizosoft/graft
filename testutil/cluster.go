package testutil

import (
	"errors"
	"github.com/mizosoft/graft"
	"go.uber.org/zap"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Service interface {
	Address() string

	ListenAndServe() error

	Shutdown()
}

type node[T Service] struct {
	service T
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

	n.service.Shutdown()

	select {
	case err := <-ch:
		log.Println("Shutdown node: ", err)
	case <-time.After(time.Second):
		return errors.New("timed out waiting for node to shutdown")
	}
	return nil
}

type Cluster[T Service] struct {
	nodes map[string]*node[T]
}

func (c *Cluster[T]) ServiceConfig() map[string]string {
	config := make(map[string]string)
	for id, node := range c.nodes {
		config[id] = node.service.Address()
	}
	return config
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

type ClusterConfig struct {
	Dir                       string
	NodeCount                 int
	HeartbeatMillis           int
	ElectionTimeoutLowMillis  int
	ElectionTimeoutHighMillis int
}

func StartLocalCluster[T Service](config ClusterConfig, serviceFactory func(address string, config graft.Config) (T, error)) (*Cluster[T], error) {
	cluster := &Cluster[T]{
		nodes: make(map[string]*node[T]),
	}

	graftAddresses := make(map[string]string)
	for i := range config.NodeCount {
		graftAddresses[strconv.Itoa(i)] = "127.0.0.1:" + strconv.Itoa(5555+i)
	}

	for i := range config.NodeCount {
		id := strconv.Itoa(i)
		dir := filepath.Join(config.Dir, strconv.Itoa(i))
		walDir := filepath.Join(dir, "wal")
		if err := os.MkdirAll(walDir, 0700); err != nil {
			return nil, err
		}

		logger := zap.NewExample()

		persistence, err := graft.OpenWal(walDir, 1024*1024, logger)
		if err != nil {
			return nil, err
		}

		address := "127.0.0.1:" + strconv.Itoa(1569+i)
		service, err := serviceFactory(address, graft.Config{
			Id:                        id,
			Addresses:                 graftAddresses,
			ElectionTimeoutLowMillis:  config.ElectionTimeoutLowMillis,
			ElectionTimeoutHighMillis: config.ElectionTimeoutHighMillis,
			HeartbeatMillis:           config.HeartbeatMillis,
			Persistence:               persistence,
			Logger:                    logger,
		})
		if err != nil {
			return nil, err
		}
		cluster.nodes[id] = &node[T]{
			service: service,
		}
	}

	for _, node := range cluster.nodes {
		node.Start()
	}
	return cluster, nil
}
