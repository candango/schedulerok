// Package clocktest provides deterministic clocks for scheduler tests.
package clocktest

import (
	"sync"
	"time"

	"github.com/candango/schedulerok"
)

// FakeClock advances only when Advance is called.
type FakeClock struct {
	mu         sync.Mutex
	now        time.Time
	timers     []*FakeTimer
	timerAdded chan struct{}
}

// New creates a FakeClock starting at now.
func New(now time.Time) *FakeClock {
	return &FakeClock{
		now:        now,
		timerAdded: make(chan struct{}, 16),
	}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer creates a timer controlled by Advance.
func (c *FakeClock) NewTimer(delay time.Duration) scheduler.Timer {
	c.mu.Lock()
	timer := newFakeTimer(c.now.Add(delay))

	if delay > 0 {
		c.timers = append(c.timers, timer)
	}
	c.mu.Unlock()

	select {
	case c.timerAdded <- struct{}{}:
	default:
	}

	if delay <= 0 {
		timer.fire(timer.due)
	}

	return timer
}

// TimerAdded returns a notification channel that receives one signal whenever
// NewTimer creates a timer.
func (c *FakeClock) TimerAdded() <-chan struct{} {
	return c.timerAdded
}

// Advance moves the clock forward and fires timers that are due.
func (c *FakeClock) Advance(delay time.Duration) {
	if delay < 0 {
		panic("clocktest: fake clock cannot move backwards")
	}

	c.mu.Lock()
	c.now = c.now.Add(delay)
	now := c.now

	var ready []*FakeTimer
	pending := c.timers[:0]
	for _, timer := range c.timers {
		if timer.due.After(now) {
			pending = append(pending, timer)
			continue
		}
		ready = append(ready, timer)
	}
	c.timers = pending
	c.mu.Unlock()

	for _, timer := range ready {
		timer.fire(timer.due)
	}
}

// FakeTimer is a timer controlled by a FakeClock.
type FakeTimer struct {
	mu sync.Mutex

	due     time.Time
	ch      chan time.Time
	stopped bool
	fired   bool
}

func newFakeTimer(due time.Time) *FakeTimer {
	return &FakeTimer{
		due: due,
		ch:  make(chan time.Time, 1),
	}
}

func (t *FakeTimer) Chan() <-chan time.Time {
	return t.ch
}

func (t *FakeTimer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

func (t *FakeTimer) fire(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped || t.fired {
		return
	}

	t.fired = true
	t.ch <- now
}
