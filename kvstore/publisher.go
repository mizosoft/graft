package main

import "sync"

type publisher struct {
	listeners map[string]chan any
	closed    bool
	mut       sync.Mutex
}

func (l *publisher) subscribe(id string) chan any {
	l.mut.Lock()
	defer l.mut.Unlock()

	if l.closed {
		return nil
	}

	listenChan := make(chan any)
	l.listeners[id] = listenChan
	return listenChan
}

func (l *publisher) publish(id string, value any) {
	l.mut.Lock()
	defer l.mut.Unlock()

	if l.closed {
		return
	}

	listenChan, ok := l.listeners[id]
	if ok {
		listenChan <- value
	}
}

func (l *publisher) publishOnce(id string, value any) {
	l.mut.Lock()
	defer l.mut.Unlock()

	if l.closed {
		return
	}

	listenChan, ok := l.listeners[id]
	if ok {
		delete(l.listeners, id)
		listenChan <- value
		close(listenChan)
	}
}

func (l *publisher) unsubscribe(id string) {
	l.mut.Lock()
	defer l.mut.Unlock()

	if l.closed {
		return
	}

	listenChan, ok := l.listeners[id]
	if ok {
		delete(l.listeners, id)
		close(listenChan)
	}
}

func (l *publisher) close() {
	l.mut.Lock()
	defer l.mut.Unlock()

	for _, listenChan := range l.listeners {
		close(listenChan)
	}
	l.listeners = nil
	l.closed = true
}
