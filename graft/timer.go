package graft

import (
	"math/rand"
	"sync"
	"time"
)

type eventTimer[T any] struct {
	duration func() time.Duration
	timer    *time.Timer
	events   chan struct{}
	stopped  bool
	mut      sync.Mutex
	C        chan T
	lastTime time.Time
}

func (t *eventTimer[T]) pause() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.stopped {
		return
	}

	if t.timer != nil {
		t.timer.Stop()
	}
}

func (t *eventTimer[T]) reset() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.stopped {
		return
	}

	if t.timer != nil {
		t.timer.Reset(t.duration())
	} else {
		t.timer = time.AfterFunc(t.duration(), func() {
			t.mut.Lock()
			defer t.mut.Unlock()

			select {
			case t.events <- struct{}{}:
			default: // Notification already present.
			}
		})
	}
}

func (t *eventTimer[T]) poke() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.stopped {
		return
	}

	if t.timer != nil {
		t.timer.Stop()
	}

	select {
	case t.events <- struct{}{}:
	default: // Notification already present.
	}
}

func (t *eventTimer[T]) stop() {
	t.mut.Lock()
	defer t.mut.Unlock()

	if t.stopped {
		return
	}
	t.stopped = true

	if t.timer != nil {
		t.timer.Stop()
	}
	close(t.events)
}

func newTimer[T any](duration time.Duration, nextEvent func() T) *eventTimer[T] {
	return newTimerWithFunc[T](func() time.Duration {
		return duration
	}, nextEvent)
}

func newRandomizedTimer[T any](low time.Duration, high time.Duration, nextEvent func() T) *eventTimer[T] {
	return newTimerWithFunc(func() time.Duration {
		return low + time.Duration(rand.Int63n(int64(high)-int64(low)+1))
	}, nextEvent)
}

func newTimerWithFunc[T any](duration func() time.Duration, nextEvent func() T) *eventTimer[T] {
	t := &eventTimer[T]{
		duration: duration,
		events:   make(chan struct{}, 1),
		C:        make(chan T),
		lastTime: time.Now(),
	}
	go func() {
		for range t.events {
			t.C <- nextEvent()
		}
		close(t.C)
	}()
	return t
}
