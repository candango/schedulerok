package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	timers     []*fakeTimer
	timerAdded chan struct{}
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{
		now:        now,
		timerAdded: make(chan struct{}, 16),
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(delay time.Duration) Timer {
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

func (c *fakeClock) Advance(delay time.Duration) {
	if delay < 0 {
		panic("fake clock cannot move backwards")
	}

	c.mu.Lock()
	c.now = c.now.Add(delay)
	now := c.now

	var ready []*fakeTimer
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

type fakeTimer struct {
	mu sync.Mutex

	due     time.Time
	ch      chan time.Time
	stopped bool
	fired   bool
}

func newFakeTimer(due time.Time) *fakeTimer {
	return &fakeTimer{
		due: due,
		ch:  make(chan time.Time, 1),
	}
}

func (t *fakeTimer) Chan() <-chan time.Time {
	return t.ch
}

func (t *fakeTimer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

func (t *fakeTimer) fire(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped || t.fired {
		return
	}

	t.fired = true
	t.ch <- now
}

func TestFakeClockFiresTimerAtDueTime(t *testing.T) {
	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	timer := clock.NewTimer(10 * time.Second)

	clock.Advance(9 * time.Second)

	select {
	case <-timer.Chan():
		t.Fatal("timer fired before its due time")
	default:
	}

	clock.Advance(time.Second)

	select {
	case firedAt := <-timer.Chan():
		want := start.Add(10 * time.Second)
		assert.Equal(t, want, firedAt)
	default:
		t.Fatal("timer did not fire at its due time")
	}
}

func TestFakeClockDoesNotFireStoppedTimer(t *testing.T) {
	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	timer := clock.NewTimer(10 * time.Second)

	timer.Stop()
	clock.Advance(10 * time.Second)

	select {
	case <-timer.Chan():
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestFakeClockFiresImmediateTimer(t *testing.T) {
	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	timer := clock.NewTimer(0)

	select {
	case firedAt := <-timer.Chan():
		assert.Equal(t, start, firedAt)
	default:
		t.Fatal("immediate timer did not fire")
	}
}

func TestFakeClockNowAdvancesWithClock(t *testing.T) {
	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)

	clock.Advance(5 * time.Minute)

	assert.Equal(t, start.Add(5*time.Minute), clock.Now())
}

func TestFakeClockFiresTimerOnlyOnce(t *testing.T) {
	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	timer := clock.NewTimer(time.Second)

	clock.Advance(time.Second)
	<-timer.Chan()

	clock.Advance(time.Second)

	select {
	case <-timer.Chan():
		t.Fatal("timer fired more than once")
	default:
	}
}
