package graft

import (
	"math/rand"
	"sync"
	"time"
)

type periodicTimer struct {
	duration func() time.Duration
	timer    *time.Timer
	trigger  func()
	mut      sync.Mutex
}

func (t *periodicTimer) reset() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}
	t.timer = time.AfterFunc(t.duration(), t.trigger)
}

func (t *periodicTimer) stop() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}

func newTimer(duration time.Duration) *periodicTimer {
	return &periodicTimer{duration: func() time.Duration { return duration }}
}

func newRandomizedTimer(low time.Duration, high time.Duration) *periodicTimer {
	rnd := rand.New(rand.NewSource(69))
	return &periodicTimer{
		duration: func() time.Duration {
			return low + time.Duration(rnd.Int63n(int64(high)-int64(low)+1))
		},
	}
}
