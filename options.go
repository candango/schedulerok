package scheduler

import "fmt"

// Option configures a Scheduler during construction.
type Option func(*Scheduler)

// WithClock configures the clock used by the scheduler and its timers.
//
// The clock must be set before the scheduler starts. Passing a nil clock is a
// programmer error.
func WithClock(clock Clock) Option {
	if clock == nil {
		panic("scheduler: clock must not be nil")
	}

	return func(s *Scheduler) {
		s.clock = clock
	}
}

// RegistrationOption configures one registered job.
type RegistrationOption func(*registrationOptions)

type registrationOptions struct {
	id JobID
}

// WithID assigns a stable ID to one registered job.
func WithID(id JobID) RegistrationOption {
	if id == "" {
		panic("scheduler: job ID must not be empty")
	}

	return func(options *registrationOptions) {
		options.id = id
	}
}

func registrationID(next uint64, options []RegistrationOption) (JobID, error) {
	config := registrationOptions{}
	for _, option := range options {
		if option == nil {
			return "", fmt.Errorf("scheduler: registration option must not be nil")
		}
		option(&config)
	}

	if config.id != "" {
		return config.id, nil
	}

	return JobID(fmt.Sprintf("job-%d", next)), nil
}
