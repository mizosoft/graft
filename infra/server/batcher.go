package server

import (
	"github.com/mizosoft/graft"
	"sync"
	"time"
)

type batcher struct {
	entries   [][]byte
	listeners []chan error
	mut       sync.Mutex

	ticker  *time.Ticker
	closeCh chan struct{}
	g       *graft.Graft
}

func (b *batcher) append(entry []byte) error {
	if b.ticker == nil {
		_, err := b.g.Append([][]byte{entry})
		return err
	}

	ch := make(chan error)
	func() {
		b.mut.Lock()
		defer b.mut.Unlock()

		b.entries = append(b.entries, entry)
		b.listeners = append(b.listeners, ch)
	}()

	err := <-ch
	if err != nil {
		return err
	}
	return nil
}

func (b *batcher) init() {
	if b.ticker != nil {
		go b.batchWorker()
	}
}

func (b *batcher) close() {
	b.ticker.Stop()
	close(b.closeCh)
}

func (b *batcher) batchWorker() {
	for {
		select {
		case <-b.ticker.C:
			var commands [][]byte
			var listeners []chan error
			func() {
				b.mut.Lock()
				defer b.mut.Unlock()

				commands = b.entries
				listeners = b.listeners

				b.entries = nil
				b.listeners = nil
			}()

			if len(commands) > 0 {
				_, err := b.g.Append(commands)
				if err != nil {
					for _, listener := range listeners {
						listener <- err
					}
				} else {
					for _, listener := range listeners {
						close(listener)
					}
				}
			}
		case <-b.closeCh:
			return
		}
	}
}

func newBatcher(g *graft.Graft, batchInterval time.Duration) *batcher {
	var ticker *time.Ticker
	if batchInterval > 0 {
		ticker = time.NewTicker(batchInterval)
	}
	return &batcher{
		closeCh: make(chan struct{}),
		ticker:  ticker,
		g:       g,
	}
}
