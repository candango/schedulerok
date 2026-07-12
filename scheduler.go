package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSchedulerRunning indicates that Run was called while the scheduler is active.
	ErrSchedulerRunning = errors.New("scheduler is already running")
	// ErrNilSchedule indicates that a registration has no schedule.
	ErrNilSchedule = errors.New("scheduler schedule must not be nil")
	// ErrNilJob indicates that a registration has no job.
	ErrNilJob = errors.New("scheduler job must not be nil")
	// ErrInvalidSchedule indicates that a schedule did not advance in time.
	ErrInvalidSchedule = errors.New("scheduler schedule must return a future time")
	// ErrNilContext indicates that Run received a nil context.
	ErrNilContext = errors.New("scheduler context must not be nil")
	// ErrUnknownJob indicates that Remove did not find the requested registration.
	ErrUnknownJob = errors.New("scheduler job ID was not found")
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
	wake          chan struct{}
}

type registration struct {
	id       JobID
	schedule Schedule
	job      AdaptiveJob
	policy   executionPolicy
	hooks    Hooks
	next     time.Time
	running  atomic.Bool
	// frozen marks a registration whose Schedule stopped advancing. A frozen
	// registration is excluded from due consideration and from the central
	// loop's wake calculation; it stays registered until an explicit Remove.
	frozen bool
}

// New creates a Scheduler with production defaults.
func New(options ...Option) *Scheduler {
	scheduler := &Scheduler{
		clock:         systemClock{},
		registrations: make(map[JobID]*registration),
		wake:          make(chan struct{}, 1),
	}

	for _, option := range options {
		if option == nil {
			panic("scheduler: option must not be nil")
		}
		option(scheduler)
	}

	return scheduler
}

// Add registers a Job to run according to a Schedule. It may be called before
// or after Run starts; a registration added while running is scheduled from
// the current time and wakes the scheduler's timer loop.
func (s *Scheduler) Add(schedule Schedule, job Job, options ...RegistrationOption) (JobID, error) {
	if schedule == nil {
		return "", ErrNilSchedule
	}
	if job == nil {
		return "", ErrNilJob
	}

	config, err := registrationConfig(options)
	if err != nil {
		return "", err
	}

	adaptive, ok := job.(AdaptiveJob)
	if !ok {
		adaptive = fixedScheduleJob{job: job}
	}

	s.mu.Lock()

	s.nextID++
	id := registrationID(s.nextID, config)
	if _, exists := s.registrations[id]; exists {
		s.mu.Unlock()
		return "", fmt.Errorf("scheduler: job ID %q already exists", id)
	}

	reg := &registration{
		id:       id,
		schedule: schedule,
		job:      adaptive,
		policy:   config.policy,
		hooks:    config.hooks,
	}

	running := s.running
	if running {
		now := s.clock.Now()
		reg.next = schedule.Next(now)
		if !reg.next.After(now) {
			s.mu.Unlock()
			return "", ErrInvalidSchedule
		}
	}

	s.registrations[id] = reg
	s.mu.Unlock()

	if running {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}

	return id, nil
}

// Remove stops future runs for id. A job that is already running is not
// interrupted and remains subject to normal shutdown handling.
func (s *Scheduler) Remove(id JobID) error {
	s.mu.Lock()
	if _, exists := s.registrations[id]; !exists {
		s.mu.Unlock()
		return ErrUnknownJob
	}
	delete(s.registrations, id)
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

// FrozenIDs returns the IDs of registrations whose Schedule stopped
// advancing. A frozen registration stays registered and excluded from due
// consideration until Remove is called explicitly.
func (s *Scheduler) FrozenIDs() []JobID {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ids []JobID
	for id, registration := range s.registrations {
		if registration.frozen {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
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

// AddAdaptiveFunc adapts fn to AdaptiveJob and registers it according to schedule.
func (s *Scheduler) AddAdaptiveFunc(
	schedule Schedule,
	fn AdaptiveJobFunc,
	options ...RegistrationOption,
) (JobID, error) {
	if fn == nil {
		return "", ErrNilJob
	}

	return s.Add(schedule, fn, options...)
}

// AddAdaptiveIntervalJob creates a fixed interval schedule and registers an
// AdaptiveJob that may redefine it after each run.
func (s *Scheduler) AddAdaptiveIntervalJob(
	interval time.Duration,
	job AdaptiveJob,
	options ...RegistrationOption,
) (JobID, error) {
	schedule, err := NewIntervalSchedule(interval)
	if err != nil {
		return "", err
	}

	return s.Add(schedule, job, options...)
}

// AddAdaptiveIntervalFunc creates a fixed interval schedule and registers fn
// as an AdaptiveJob that may redefine it after each run.
func (s *Scheduler) AddAdaptiveIntervalFunc(
	interval time.Duration,
	fn AdaptiveJobFunc,
	options ...RegistrationOption,
) (JobID, error) {
	if fn == nil {
		return "", ErrNilJob
	}

	return s.AddAdaptiveIntervalJob(interval, fn, options...)
}

// AddAdaptiveCronJob parses spec as an intervalok cron schedule and registers
// an AdaptiveJob that may redefine it after each run.
func (s *Scheduler) AddAdaptiveCronJob(
	spec string,
	job AdaptiveJob,
	options ...RegistrationOption,
) (JobID, error) {
	schedule, err := NewCronSchedule(spec)
	if err != nil {
		return "", err
	}

	return s.Add(schedule, job, options...)
}

// AddAdaptiveCronFunc parses spec as an intervalok cron schedule and
// registers fn as an AdaptiveJob that may redefine it after each run.
func (s *Scheduler) AddAdaptiveCronFunc(
	spec string,
	fn AdaptiveJobFunc,
	options ...RegistrationOption,
) (JobID, error) {
	if fn == nil {
		return "", ErrNilJob
	}

	return s.AddAdaptiveCronJob(spec, fn, options...)
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
			select {
			case <-ctx.Done():
				jobs.Wait()
				return nil
			case <-s.wake:
				continue
			}
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
		case <-s.wake:
			timer.Stop()
			continue
		case now = <-timer.Chan():
			timer.Stop()
		}

		s.runDue(ctx, now, &jobs)
	}
}

func (s *Scheduler) nextRun() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var next time.Time
	for _, registration := range s.registrations {
		if registration.frozen {
			continue
		}
		if next.IsZero() || registration.next.Before(next) {
			next = registration.next
		}
	}
	return next, !next.IsZero()
}

// runDue dispatches every due registration. A registration whose Schedule
// stops advancing is isolated: it fires OnFailure and freezes in place
// instead of stopping the scheduler for every other registration.
func (s *Scheduler) runDue(ctx context.Context, now time.Time, jobs *sync.WaitGroup) {
	s.mu.Lock()
	var due []*registration
	var skipped []*registration
	var invalid []*registration
	for _, registration := range s.registrations {
		if registration.frozen || registration.next.After(now) {
			continue
		}

		previous := registration.next
		registration.next = registration.schedule.Next(previous)
		if !registration.next.After(previous) {
			registration.frozen = true
			invalid = append(invalid, registration)
			continue
		}

		if registration.policy.overlap == SkipOverlap &&
			!registration.running.CompareAndSwap(false, true) {
			skipped = append(skipped, registration)
			continue
		}
		due = append(due, registration)
	}
	s.mu.Unlock()

	for _, registration := range invalid {
		registration.callHook(ctx, registration.hooks.OnFailure,
			Event{JobID: registration.id, Error: ErrInvalidSchedule})
	}
	for _, registration := range skipped {
		registration.callHook(ctx, registration.hooks.OnSkip, Event{JobID: registration.id})
	}
	for _, registration := range due {
		jobs.Add(1)
		go s.runJob(ctx, registration, jobs)
	}
}

func (s *Scheduler) runJob(ctx context.Context, registration *registration, jobs *sync.WaitGroup) {
	defer jobs.Done()
	if registration.policy.overlap == SkipOverlap {
		defer registration.running.Store(false)
	}

	attempts := registration.policy.retries
	if attempts == 0 {
		attempts = 1
	}

	var backoffStrategy BackoffFactory
	if registration.policy.retries > 0 {
		backoffStrategy = registration.policy.backoff
	}
	var retryBackoff interface {
		Next() time.Duration
	}
	if backoffStrategy != nil {
		retryBackoff = backoffStrategy()
	}

	// Only the attempt that exits the retry loop owns the returned Schedule;
	// intermediate attempts are overwritten and never applied.
	var lastSchedule Schedule
	defer func() {
		s.applyReschedule(ctx, registration, lastSchedule)
	}()

	for attempt := 0; attempt < attempts; attempt++ {
		event := Event{JobID: registration.id, Attempt: attempt + 1}
		registration.callHook(ctx, registration.hooks.OnStart, event)

		attemptCtx := ctx
		cancel := func() {}
		if registration.policy.timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, registration.policy.timeout)
		}

		newSchedule, err := registration.job.RunAdaptive(attemptCtx)
		cancel()
		lastSchedule = newSchedule
		if err == nil {
			registration.callHook(ctx, registration.hooks.OnSuccess, event)
			return
		}

		event.Error = err
		registration.callHook(ctx, registration.hooks.OnFailure, event)
		if ctx.Err() != nil || attempt == attempts-1 || retryBackoff == nil {
			return
		}

		event.RetryDelay = retryBackoff.Next()
		registration.callHook(ctx, registration.hooks.OnRetry, event)
		if !s.wait(ctx, event.RetryDelay) {
			return
		}
	}
}

// applyReschedule adopts newSchedule for registration's future runs. A nil
// newSchedule means the job did not ask to change its Schedule. If the
// registration was removed while the job ran, the update is skipped. A
// Schedule that does not advance freezes the registration the same way
// runDue does for interval and cron schedules.
func (s *Scheduler) applyReschedule(ctx context.Context, registration *registration, newSchedule Schedule) {
	if newSchedule == nil {
		return
	}

	s.mu.Lock()
	if _, exists := s.registrations[registration.id]; !exists {
		s.mu.Unlock()
		return
	}

	now := s.clock.Now()
	next := newSchedule.Next(now)
	if !next.After(now) {
		registration.frozen = true
		s.mu.Unlock()
		registration.callHook(ctx, registration.hooks.OnFailure,
			Event{JobID: registration.id, Error: ErrInvalidSchedule})
		return
	}

	registration.schedule = newSchedule
	registration.next = next
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (registration *registration) callHook(ctx context.Context, hook func(context.Context, Event), event Event) {
	if hook != nil {
		hook(ctx, event)
	}
}

func (s *Scheduler) wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}

	timer := s.clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.Chan():
		return true
	}
}
