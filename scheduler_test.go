package scheduler

import (
	"context"
	"testing"
	"time"

	intervalcron "github.com/candango/intervalok/cron"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerAddGeneratesID(t *testing.T) {
	scheduler := New()
	schedule, err := NewIntervalSchedule(time.Minute)
	require.NoError(t, err)

	id, err := scheduler.Add(schedule, JobFunc(func(context.Context) error {
		return nil
	}))

	require.NoError(t, err)
	assert.Equal(t, JobID("job-1"), id)
}

func TestSchedulerAddUsesStableID(t *testing.T) {
	scheduler := New()
	schedule, err := NewIntervalSchedule(time.Minute)
	require.NoError(t, err)
	job := JobFunc(func(context.Context) error { return nil })

	id, err := scheduler.Add(schedule, job, WithID("heartbeat"))
	require.NoError(t, err)
	assert.Equal(t, JobID("heartbeat"), id)

	_, err = scheduler.Add(schedule, job, WithID("heartbeat"))
	assert.Error(t, err)
}

func TestSchedulerRunsIntervalFunctionWithConfiguredClock(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	ran := make(chan struct{}, 1)

	_, err := scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		ran <- struct{}{}
		return nil
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- scheduler.Run(ctx)
	}()

	select {
	case <-clock.timerAdded:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not create a timer")
	}

	clock.Advance(time.Second)

	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("scheduled job did not run")
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerRejectsScheduleThatDoesNotAdvance(t *testing.T) {
	scheduler := New()
	_, err := scheduler.Add(ScheduleFunc(func(after time.Time) time.Time {
		return after
	}), JobFunc(func(context.Context) error {
		return nil
	}))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.ErrorIs(t, scheduler.Run(ctx), ErrInvalidSchedule)
}

func TestSchedulerRejectsNilContext(t *testing.T) {
	assert.ErrorIs(t, New().Run(nil), ErrNilContext)
}

func TestSchedulerRejectsAddAfterStart(t *testing.T) {
	scheduler := New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- scheduler.Run(ctx)
	}()

	deadline := time.After(time.Second)
	for {
		scheduler.mu.Lock()
		running := scheduler.running
		scheduler.mu.Unlock()
		if running {
			break
		}

		select {
		case <-deadline:
			t.Fatal("scheduler did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	schedule, err := NewIntervalSchedule(time.Minute)
	require.NoError(t, err)
	_, err = scheduler.Add(schedule, JobFunc(func(context.Context) error {
		return nil
	}))
	assert.ErrorIs(t, err, ErrSchedulerStarted)

	cancel()
	require.NoError(t, <-done)
}

func TestCronSeriesSatisfiesSchedule(t *testing.T) {
	series, err := intervalcron.NewCronSeries("*/5 * * * *")
	require.NoError(t, err)

	var schedule Schedule = series
	assert.False(t, schedule.Next(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)).IsZero())
}

func TestSchedulerAddCronFuncRejectsInvalidExpression(t *testing.T) {
	_, err := New().AddCronFunc("not cron", func(context.Context) error {
		return nil
	})
	assert.Error(t, err)
}
