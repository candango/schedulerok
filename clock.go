package scheduler

import "time"

// Clock provides the current time and creates timers for scheduled work.
//
// Implementations let the scheduler control time deterministically in tests.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Timer waits until a scheduled time.
//
// Chan receives the time at which the timer fires. Stop releases the timer's
// resources and may be called after the timer has fired.
type Timer interface {
	Chan() <-chan time.Time
	Stop()
}

// systemClock implements Clock with the system time package.
type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

func (systemClock) NewTimer(delay time.Duration) Timer {
	return &systemTimer{
		timer: time.NewTimer(delay),
	}
}

// systemTimer adapts time.Timer to the Timer contract.
type systemTimer struct {
	timer *time.Timer
}

func (t *systemTimer) Chan() <-chan time.Time {
	return t.timer.C
}

func (t *systemTimer) Stop() {
	t.timer.Stop()
}
