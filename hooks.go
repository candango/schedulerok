package scheduler

import (
	"context"
	"time"
)

// Event describes one scheduler lifecycle event.
type Event struct {
	JobID      JobID
	Attempt    int
	Error      error
	RetryDelay time.Duration
}

// Hooks receives lifecycle events for one job registration. Hook functions run
// synchronously and must return promptly.
type Hooks struct {
	OnStart   func(context.Context, Event)
	OnSuccess func(context.Context, Event)
	OnFailure func(context.Context, Event)
	OnRetry   func(context.Context, Event)
	OnSkip    func(context.Context, Event)
}

// TickEvent describes one scheduler timer tick.
type TickEvent struct {
	At         time.Time
	DueJobs    []JobID
	Dispatched []JobID
}

// SchedulerHooks receives lifecycle events for the scheduler itself.
// Hook functions run synchronously and must return promptly.
type SchedulerHooks struct {
	OnStart    func(context.Context)
	OnStopping func(context.Context)
	OnStopped  func(context.Context)
	OnTick     func(context.Context, TickEvent)
}

// WithHooks attaches lifecycle hooks to one job registration.
func WithHooks(hooks Hooks) RegistrationOption {
	return func(options *registrationOptions) {
		options.hooks = hooks
	}
}
