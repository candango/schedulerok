package clocktest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFakeClockFiresTimerAtDueTime(t *testing.T) {
	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	clock := New(start)
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
		assert.Equal(t, start.Add(10*time.Second), firedAt)
	default:
		t.Fatal("timer did not fire at due time")
	}
}

func TestFakeClockDoesNotFireStoppedTimer(t *testing.T) {
	clock := New(time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC))
	timer := clock.NewTimer(10 * time.Second)

	timer.Stop()
	clock.Advance(10 * time.Second)

	select {
	case <-timer.Chan():
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestFakeClockNotifiesTimerCreation(t *testing.T) {
	clock := New(time.Now())
	clock.NewTimer(time.Second)

	select {
	case <-clock.TimerAdded():
	default:
		t.Fatal("timer creation was not reported")
	}
}
