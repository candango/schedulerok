package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrSchedulerRunning indicates that Run was called while the scheduler is active.
	ErrSchedulerRunning = errors.New("scheduler is already running")
	// ErrSchedulerStarted indicates that a registration was attempted after Run started.
	ErrSchedulerStarted = errors.New("scheduler has already started")
	// ErrNilSchedule indicates that a registration has no schedule.
	ErrNilSchedule = errors.New("scheduler schedule must not be nil")
	// ErrNilJob indicates that a registration has no job.
	ErrNilJob = errors.New("scheduler job must not be nil")
	// ErrInvalidSchedule indicates that a schedule did not advance in time.
	ErrInvalidSchedule = errors.New("scheduler schedule must return a future time")
	// ErrNilContext indicates that Run received a nil context.
	ErrNilContext = errors.New("scheduler context must not be nil")
)

// JobID identifies one job registration in a Scheduler.
type JobID string

// Scheduler coordinates registered jobs and schedules.
type Scheduler struct {
	clock Clock

	mu            sync.Mutex
	registrations map[JobID]*registration
	nextID        uint64
	running       bool
}

type registration struct {
	schedule Schedule
	job      Job
	next     time.Time
}

// New creates a Scheduler with production defaults.
func New(options ...Option) *Scheduler {
	scheduler := &Scheduler{
		clock:         systemClock{},
		registrations: make(map[JobID]*registration),
	}

	for _, option := range options {
		if option == nil {
			panic("scheduler: option must not be nil")
		}
		option(scheduler)
	}

	return scheduler
}

// Add registers a Job to run according to a Schedule.
func (s *Scheduler) Add(schedule Schedule, job Job, options ...RegistrationOption) (JobID, error) {
	if schedule == nil {
		return "", ErrNilSchedule
	}
	if job == nil {
		return "", ErrNilJob
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return "", ErrSchedulerStarted
	}

	s.nextID++
	id, err := registrationID(s.nextID, options)
	if err != nil {
		return "", err
	}
	if _, exists := s.registrations[id]; exists {
		return "", fmt.Errorf("scheduler: job ID %q already exists", id)
	}

	s.registrations[id] = &registration{
		schedule: schedule,
		job:      job,
	}
	return id, nil
}

// AddFunc adapts fn to Job and registers it according to schedule.
func (s *Scheduler) AddFunc(
	schedule Schedule,
	fn JobFunc,
	options ...RegistrationOption,
) (JobID, error) {
	if fn == nil {
		return "", ErrNilJob
	}

	return s.Add(schedule, fn, options...)
}

// AddIntervalJob creates a fixed interval schedule and registers job.
func (s *Scheduler) AddIntervalJob(
	interval time.Duration,
	job Job,
	options ...RegistrationOption,
) (JobID, error) {
	schedule, err := NewIntervalSchedule(interval)
	if err != nil {
		return "", err
	}

	return s.Add(schedule, job, options...)
}

// AddIntervalFunc creates a fixed interval schedule and registers fn.
func (s *Scheduler) AddIntervalFunc(
	interval time.Duration,
	fn JobFunc,
	options ...RegistrationOption,
) (JobID, error) {
	if fn == nil {
		return "", ErrNilJob
	}

	return s.AddIntervalJob(interval, fn, options...)
}

// AddCronJob parses spec as an intervalok cron schedule and registers job.
func (s *Scheduler) AddCronJob(
	spec string,
	job Job,
	options ...RegistrationOption,
) (JobID, error) {
	schedule, err := NewCronSchedule(spec)
	if err != nil {
		return "", err
	}

	return s.Add(schedule, job, options...)
}

// AddCronFunc parses spec as an intervalok cron schedule and registers fn.
func (s *Scheduler) AddCronFunc(
	spec string,
	fn JobFunc,
	options ...RegistrationOption,
) (JobID, error) {
	if fn == nil {
		return "", ErrNilJob
	}

	return s.AddCronJob(spec, fn, options...)
}

// Run starts the scheduler and blocks until ctx is cancelled. New job runs stop
// when cancellation begins; jobs already running receive ctx and are awaited.
func (s *Scheduler) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrSchedulerRunning
	}

	now := s.clock.Now()
	for _, registration := range s.registrations {
		registration.next = registration.schedule.Next(now)
		if !registration.next.After(now) {
			s.mu.Unlock()
			return ErrInvalidSchedule
		}
	}
	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	var jobs sync.WaitGroup
	for {
		next, ok := s.nextRun()
		if !ok {
			<-ctx.Done()
			jobs.Wait()
			return nil
		}

		delay := next.Sub(s.clock.Now())
		if delay < 0 {
			delay = 0
		}
		timer := s.clock.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			jobs.Wait()
			return nil
		case now = <-timer.Chan():
			timer.Stop()
		}

		if err := s.runDue(ctx, now, &jobs); err != nil {
			jobs.Wait()
			return err
		}
	}
}

func (s *Scheduler) nextRun() (time.Time, bool) {
	var next time.Time
	for _, registration := range s.registrations {
		if next.IsZero() || registration.next.Before(next) {
			next = registration.next
		}
	}
	return next, !next.IsZero()
}

func (s *Scheduler) runDue(ctx context.Context, now time.Time, jobs *sync.WaitGroup) error {
	for _, registration := range s.registrations {
		if registration.next.After(now) {
			continue
		}

		previous := registration.next
		registration.next = registration.schedule.Next(previous)
		if !registration.next.After(previous) {
			return ErrInvalidSchedule
		}

		jobs.Add(1)
		go func(job Job) {
			defer jobs.Done()
			_ = job.Run(ctx)
		}(registration.job)
	}
	return nil
}
