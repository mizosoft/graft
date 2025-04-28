package graft

import (
	"math/rand"
	"sync"
	"time"
)

type periodicTimer struct {
	duration func() time.Duration
	timer    *time.Timer
	c        chan struct{}
	mut      sync.Mutex
}

func (t *periodicTimer) start(trigger func()) {
	go func() {
		for range t.c {
			trigger()
		}
	}()
}

func (t *periodicTimer) stop() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}

func (t *periodicTimer) reset() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.timer != nil {
		t.timer.Reset(t.duration())
	} else {
		t.timer = time.AfterFunc(t.duration(), t.tick)
	}
}

func (t *periodicTimer) poke() {
	t.stop() // Invalidate schedule event (if any).
	t.tick()
}

func (t *periodicTimer) tick() {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.c <- struct{}{}
}

func newTimer(duration time.Duration) *periodicTimer {
	return &periodicTimer{
		duration: func() time.Duration { return duration },
		c:        make(chan struct{}),
	}
}

func newRandomizedTimer(low time.Duration, high time.Duration) *periodicTimer {
	rnd := rand.New(rand.NewSource(69))
	return &periodicTimer{
		duration: func() time.Duration {
			return low + time.Duration(rnd.Int63n(int64(high)-int64(low)+1))
		},
		c: make(chan struct{}),
	}
}
