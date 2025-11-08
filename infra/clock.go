package infra

import "time"

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (s systemClock) Now() time.Time {
	return time.Now().UTC()
}

var sysClock systemClock

func SystemClock() Clock {
	return sysClock
}
