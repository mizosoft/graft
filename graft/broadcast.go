package graft

import "sync"

type broadcastChannel struct {
	c      chan struct{}
	closed bool
	mut    sync.Mutex
}

func newBroadcastChannel() *broadcastChannel {
	return &broadcastChannel{
		c: make(chan struct{}, 1), // We only need an additional (keep_broadcasting) signal.
	}
}

func (c *broadcastChannel) notify() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.closed {
		return
	}

	select {
	case c.c <- struct{}{}:
	default:
		// Ignore: c has a capacity of 1 so a new AppendEntries is surely running in the future and that is all
		// what we need.
	}
}

func (c *broadcastChannel) close() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	close(c.c)
}
