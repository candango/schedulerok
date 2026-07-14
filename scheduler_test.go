package scheduler

import (
	"context"
	"errors"
	"sync"
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

func TestSchedulerFreezesRegistrationInsteadOfStoppingRun(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))

	var calls atomic.Int32
	failed := make(chan Event, 1)
	brokenID, err := scheduler.Add(
		ScheduleFunc(func(after time.Time) time.Time {
			if calls.Add(1) == 1 {
				return after.Add(time.Second)
			}
			return after
		}),
		JobFunc(func(context.Context) error { return nil }),
		WithHooks(Hooks{OnFailure: func(_ context.Context, event Event) {
			failed <- event
		}}),
	)
	require.NoError(t, err)

	ran := make(chan struct{}, 2)
	_, err = scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		ran <- struct{}{}
		return nil
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	<-clock.timerAdded
	clock.Advance(time.Second)

	select {
	case event := <-failed:
		assert.Equal(t, brokenID, event.JobID)
		assert.ErrorIs(t, event.Error, ErrInvalidSchedule)
	case <-time.After(time.Second):
		t.Fatal("broken registration did not fire OnFailure")
	}

	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("healthy registration did not run alongside the broken one")
	}

	<-clock.timerAdded
	clock.Advance(time.Second)

	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("healthy registration stopped running after a sibling froze")
	}

	cancel()
	require.NoError(t, <-done)

	assert.Equal(t, int32(2), calls.Load())
	assert.Equal(t, []JobID{brokenID}, scheduler.FrozenIDs())

	scheduler.mu.Lock()
	_, exists := scheduler.registrations[brokenID]
	scheduler.mu.Unlock()
	require.True(t, exists, "frozen registration must stay registered")
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

func waitForRunning(t *testing.T, scheduler *Scheduler) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		scheduler.mu.Lock()
		running := scheduler.running
		scheduler.mu.Unlock()
		if running {
			return
		}

		select {
		case <-deadline:
			t.Fatal("scheduler did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestSchedulerAddRegistersJobAfterStart(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	ran := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	waitForRunning(t, scheduler)

	id, err := scheduler.AddIntervalFunc(time.Second, func(context.Context) error {
		ran <- struct{}{}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, JobID("job-1"), id)

	select {
	case <-clock.timerAdded:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not re-arm its timer for the new registration")
	}

	clock.Advance(time.Second)

	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("job registered after start did not run")
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAddConcurrentDuringRun(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	waitForRunning(t, scheduler)

	const registrants = 20
	var wg sync.WaitGroup
	errs := make(chan error, registrants)
	wg.Add(registrants)
	for i := 0; i < registrants; i++ {
		go func() {
			defer wg.Done()
			_, err := scheduler.AddIntervalFunc(time.Minute, func(context.Context) error {
				return nil
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		assert.NoError(t, err)
	}

	scheduler.mu.Lock()
	assert.Len(t, scheduler.registrations, registrants)
	for _, registration := range scheduler.registrations {
		assert.False(t, registration.next.IsZero())
	}
	scheduler.mu.Unlock()

	cancel()
	require.NoError(t, <-done)
}

type adaptiveTestJob struct {
	run     func(context.Context) error
	next    func(Schedule) (Schedule, error)
	current Schedule
}

func (j *adaptiveTestJob) Run(ctx context.Context) error {
	return j.run(ctx)
}

func (j *adaptiveTestJob) NextSchedule(current Schedule) (Schedule, error) {
	j.current = current
	return j.next(current)
}

type adaptiveFuncTestJob struct {
	fn       func(context.Context) (Schedule, error)
	schedule Schedule
	mu       sync.Mutex
}

func (j *adaptiveFuncTestJob) Run(ctx context.Context) error {
	schedule, err := j.fn(ctx)

	j.mu.Lock()
	j.schedule = schedule
	j.mu.Unlock()

	return err
}

func (j *adaptiveFuncTestJob) NextSchedule(Schedule) (Schedule, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.schedule, nil
}

func newAdaptiveFuncTestJob(fn func(context.Context) (Schedule, error)) Job {
	return &adaptiveFuncTestJob{fn: fn}
}

func addAdaptiveIntervalTestJob(
	scheduler *Scheduler,
	interval time.Duration,
	fn func(context.Context) (Schedule, error),
	options ...RegistrationOption,
) (JobID, error) {
	schedule, err := NewIntervalSchedule(interval)
	if err != nil {
		return "", err
	}

	return scheduler.Add(schedule, &adaptiveFuncTestJob{fn: fn}, options...)
}

func TestAdaptiveJobReceivesCurrentSchedule(t *testing.T) {
	current := IntervalSchedule{interval: time.Minute}
	job := &adaptiveTestJob{
		run:  func(context.Context) error { return nil },
		next: func(Schedule) (Schedule, error) { return nil, nil },
	}

	_, err := job.NextSchedule(current)
	require.NoError(t, err)
	assert.Equal(t, current, job.current)
}

func TestAddAdaptiveFuncRejectsNilRun(t *testing.T) {
	scheduler := New()
	schedule := IntervalSchedule{interval: time.Minute}

	_, err := scheduler.AddAdaptiveFunc(schedule, nil, nil)
	assert.ErrorIs(t, err, ErrNilJob)
}

func TestAddAdaptiveFuncNilNextUsesRegularJob(t *testing.T) {
	scheduler := New()
	schedule := IntervalSchedule{interval: time.Minute}
	ran := false

	id, err := scheduler.AddAdaptiveFunc(schedule, func(context.Context) error {
		ran = true
		return nil
	}, nil)
	require.NoError(t, err)

	scheduler.mu.Lock()
	job := scheduler.registrations[id].job
	scheduler.mu.Unlock()

	_, fixed := job.(fixedScheduleJob)
	assert.True(t, fixed)
	require.NoError(t, job.Run(context.Background()))
	assert.True(t, ran)
}

func TestAddAdaptiveFuncBuildsTwoFunctionAdaptiveJob(t *testing.T) {
	scheduler := New()
	current := IntervalSchedule{interval: time.Minute}
	replacement := IntervalSchedule{interval: 5 * time.Minute}
	var received Schedule

	id, err := scheduler.AddAdaptiveFunc(
		current,
		func(context.Context) error { return nil },
		func(schedule Schedule) (Schedule, error) {
			received = schedule
			return replacement, nil
		},
	)
	require.NoError(t, err)

	scheduler.mu.Lock()
	job := scheduler.registrations[id].job
	scheduler.mu.Unlock()

	adaptive, ok := job.(AdaptiveJob)
	require.True(t, ok)
	require.NoError(t, adaptive.Run(context.Background()))
	got, err := adaptive.NextSchedule(current)
	require.NoError(t, err)
	assert.Equal(t, current, received)
	assert.Equal(t, replacement, got)
}

func TestSchedulerAdaptiveJobAdoptsReturnedSchedule(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	ran := make(chan struct{}, 1)

	id, err := addAdaptiveIntervalTestJob(scheduler, time.Second, func(context.Context) (Schedule, error) {
		ran <- struct{}{}
		return NewIntervalSchedule(5 * time.Second)
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	<-clock.timerAdded
	clock.Advance(time.Second)

	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("adaptive job did not run")
	}

	want := clock.Now().Add(5 * time.Second)
	deadline := time.After(time.Second)
	for {
		scheduler.mu.Lock()
		next := scheduler.registrations[id].next
		scheduler.mu.Unlock()
		if next.Equal(want) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("registration did not adopt the returned schedule; next=%v want=%v", next, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAdaptiveJobNilScheduleKeepsCurrent(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	ran := make(chan struct{}, 2)

	_, err := addAdaptiveIntervalTestJob(scheduler, time.Second, func(context.Context) (Schedule, error) {
		ran <- struct{}{}
		return nil, nil
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	<-clock.timerAdded
	clock.Advance(time.Second)
	<-ran

	<-clock.timerAdded
	clock.Advance(time.Second)
	<-ran

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAdaptiveJobInvalidScheduleFreezes(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	failed := make(chan Event, 1)

	id, err := addAdaptiveIntervalTestJob(scheduler, time.Second, func(context.Context) (Schedule, error) {
		return ScheduleFunc(func(after time.Time) time.Time { return after }), nil
	}, WithHooks(Hooks{OnFailure: func(_ context.Context, event Event) {
		failed <- event
	}}))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	<-clock.timerAdded
	clock.Advance(time.Second)

	select {
	case event := <-failed:
		assert.Equal(t, id, event.JobID)
		assert.ErrorIs(t, event.Error, ErrInvalidSchedule)
	case <-time.After(time.Second):
		t.Fatal("invalid adaptive schedule did not fire OnFailure")
	}

	deadline := time.After(time.Second)
	for {
		frozen := scheduler.FrozenIDs()
		if len(frozen) == 1 && frozen[0] == id {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("registration did not freeze after an invalid adaptive schedule; frozen=%v", frozen)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAdaptiveJobHonorsScheduleDespiteError(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	failed := make(chan Event, 1)
	jobErr := errors.New("transient")

	id, err := addAdaptiveIntervalTestJob(scheduler, time.Second, func(context.Context) (Schedule, error) {
		sched, _ := NewIntervalSchedule(5 * time.Second)
		return sched, jobErr
	}, WithHooks(Hooks{OnFailure: func(_ context.Context, event Event) {
		failed <- event
	}}))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	<-clock.timerAdded
	clock.Advance(time.Second)

	select {
	case event := <-failed:
		assert.ErrorIs(t, event.Error, jobErr)
	case <-time.After(time.Second):
		t.Fatal("job failure did not fire OnFailure")
	}

	want := clock.Now().Add(5 * time.Second)
	deadline := time.After(time.Second)
	for {
		scheduler.mu.Lock()
		next := scheduler.registrations[id].next
		scheduler.mu.Unlock()
		if next.Equal(want) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("schedule was not adopted despite job error; next=%v want=%v", next, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAdaptiveJobRetryUsesOnlyFinalAttemptSchedule(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	var attempts atomic.Int32
	succeeded := make(chan Event, 1)

	first, err := NewIntervalSchedule(2 * time.Second)
	require.NoError(t, err)
	final, err := NewIntervalSchedule(9 * time.Second)
	require.NoError(t, err)

	id, err := scheduler.Add(
		ScheduleFunc(func(after time.Time) time.Time { return after.Add(time.Hour) }),
		newAdaptiveFuncTestJob(func(context.Context) (Schedule, error) {
			if attempts.Add(1) == 1 {
				return first, errors.New("transient failure")
			}
			return final, nil
		}),
		WithRetry(2, func() backoff.Backoff {
			return backoff.NewBackoffInterval(time.Second)
		}),
		WithHooks(Hooks{OnSuccess: func(_ context.Context, event Event) {
			succeeded <- event
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
		t.Fatal("adaptive job did not succeed on retry")
	}
	assert.Equal(t, int32(2), attempts.Load())

	want := clock.Now().Add(9 * time.Second)
	deadline := time.After(time.Second)
	for {
		scheduler.mu.Lock()
		next := scheduler.registrations[id].next
		scheduler.mu.Unlock()
		if next.Equal(want) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("final attempt schedule not adopted; next=%v want=%v", next, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSchedulerAdaptiveJobSkipsRescheduleAfterRemove(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC))
	scheduler := New(WithClock(clock))
	started := make(chan struct{})
	release := make(chan struct{})

	id, err := addAdaptiveIntervalTestJob(scheduler, time.Second, func(context.Context) (Schedule, error) {
		close(started)
		<-release
		return NewIntervalSchedule(5 * time.Second)
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()

	<-clock.timerAdded
	clock.Advance(time.Second)
	<-started

	require.NoError(t, scheduler.Remove(id))
	close(release)

	cancel()
	require.NoError(t, <-done)

	scheduler.mu.Lock()
	_, exists := scheduler.registrations[id]
	scheduler.mu.Unlock()
	assert.False(t, exists, "removed registration must not be reinserted by a late reschedule")
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
