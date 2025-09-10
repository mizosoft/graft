package graft

import (
	"math/rand"
	"sync"
	"time"
)

type periodicTimer struct {
	duration func() time.Duration
	timer    *time.Timer
	started  bool
	c        chan struct{}
	mut      sync.Mutex
}

func (t *periodicTimer) start(trigger func()) {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.started {
		return
	}

	go func() {
		for range t.c {
			trigger()
		}
	}()
	t.started = true
}

func (t *periodicTimer) pause() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}
}

func (t *periodicTimer) reset() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.c != nil {
		if t.timer != nil {
			t.timer.Reset(t.duration())
		} else {
			t.timer = time.AfterFunc(t.duration(), t.tick)
		}
	}
}

func (t *periodicTimer) poke() {
	t.pause() // Invalidate schedule event (if any).
	t.tick()
}

func (t *periodicTimer) tick() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.c != nil {
		select {
		case t.c <- struct{}{}:
		default:
			return
		}
	}
}

func (t *periodicTimer) stop() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	close(t.c)
	t.c = nil
}

func newTimer(duration time.Duration) *periodicTimer {
	return &periodicTimer{
		duration: func() time.Duration { return duration },
		c:        make(chan struct{}),
	}
}

func newRandomizedTimer(low time.Duration, high time.Duration) *periodicTimer {
	return &periodicTimer{
		duration: func() time.Duration {
			return low + time.Duration(rand.Int63n(int64(high)-int64(low)+1))
		},
		c: make(chan struct{}),
	}
}
