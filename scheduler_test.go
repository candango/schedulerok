package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/candango/intervalok/backoff"
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

func TestSchedulerWaitsForRunningJobOnCancellation(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	started := make(chan struct{})
	release := make(chan struct{})

	_, err := scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		close(started)
		<-release
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
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("scheduled job did not start")
	}

	cancel()

	select {
	case <-done:
		t.Fatal("scheduler returned before the running job completed")
	default:
	}

	close(release)
	require.NoError(t, <-done)
}

func TestSchedulerRetriesFailedJob(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	var attempts atomic.Int32
	succeeded := make(chan struct{}, 1)
	success := make(chan Event, 1)

	_, err := scheduler.Add(
		ScheduleFunc(func(after time.Time) time.Time { return after.Add(time.Hour) }),
		JobFunc(func(context.Context) error {
			if attempts.Add(1) == 1 {
				return errors.New("transient failure")
			}
			succeeded <- struct{}{}
			return nil
		}),
		WithRetry(2, func() backoff.Backoff {
			return backoff.NewBackoffInterval(time.Second)
		}),
		WithHooks(Hooks{OnSuccess: func(_ context.Context, event Event) {
			success <- event
		}}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-clock.timerAdded
	clock.Advance(time.Hour)
	<-clock.timerAdded
	<-clock.timerAdded
	clock.Advance(time.Second)

	select {
	case <-succeeded:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not retry the failed job")
	}
	assert.Equal(t, int32(2), attempts.Load())
	event := <-success
	assert.Equal(t, JobID("job-1"), event.JobID)
	assert.Equal(t, 2, event.Attempt)

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAppliesTimeoutPerAttempt(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	timedOut := make(chan error, 1)

	_, err := scheduler.Add(
		ScheduleFunc(func(after time.Time) time.Time { return after.Add(time.Hour) }),
		JobFunc(func(ctx context.Context) error {
			<-ctx.Done()
			timedOut <- ctx.Err()
			return ctx.Err()
		}),
		WithTimeout(10*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-clock.timerAdded
	clock.Advance(time.Hour)

	select {
	case err := <-timedOut:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("scheduler did not apply the job timeout")
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAllowsOverlappingJobsByDefault(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	_, err := scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-clock.timerAdded
	clock.Advance(time.Second)
	<-started
	<-clock.timerAdded
	clock.Advance(time.Second)
	<-started

	close(release)
	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerSkipsOverlappingJob(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	skipped := make(chan Event, 1)

	_, err := scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		if runs.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	}, WithOverlap(SkipOverlap), WithHooks(Hooks{OnSkip: func(_ context.Context, event Event) {
		skipped <- event
	}}))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-clock.timerAdded
	clock.Advance(time.Second)
	<-started
	<-clock.timerAdded
	clock.Advance(time.Second)
	<-clock.timerAdded

	assert.Equal(t, int32(1), runs.Load())
	assert.Equal(t, JobID("job-1"), (<-skipped).JobID)
	close(release)
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

func TestSchedulerRemoveStopsFutureRuns(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	ran := make(chan struct{}, 1)

	id, err := scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		ran <- struct{}{}
		return nil
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-clock.timerAdded

	require.NoError(t, scheduler.Remove(id))
	clock.Advance(time.Second)

	select {
	case <-ran:
		t.Fatal("removed job ran")
	case <-time.After(10 * time.Millisecond):
	}

	cancel()
	require.NoError(t, <-done)
	assert.ErrorIs(t, scheduler.Remove(id), ErrUnknownJob)
}

func TestSchedulerRemoveAllowsRunningJobToFinish(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	started := make(chan struct{})
	release := make(chan struct{})
	succeeded := make(chan Event, 1)

	id, err := scheduler.AddIntervalFunc(
		time.Second,
		func(context.Context) error {
			close(started)
			<-release
			return nil
		},
		WithHooks(Hooks{OnSuccess: func(_ context.Context, event Event) {
			succeeded <- event
		}}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-clock.timerAdded
	clock.Advance(time.Second)
	<-started

	require.NoError(t, scheduler.Remove(id))
	close(release)

	event := <-succeeded
	assert.Equal(t, id, event.JobID)

	cancel()
	require.NoError(t, <-done)
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
