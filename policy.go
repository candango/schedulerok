package scheduler

import (
	"time"

	"github.com/candango/intervalok/backoff"
)

// OverlapPolicy controls what happens when a job is due while it is running.
type OverlapPolicy uint8

const (
	// AllowOverlap starts every due run, even if an earlier run is still active.
	AllowOverlap OverlapPolicy = iota
	// SkipOverlap discards a due run while an earlier run is still active.
	SkipOverlap
)

// BackoffFactory creates isolated backoff state for one job execution.
type BackoffFactory func() backoff.Backoff

type executionPolicy struct {
	timeout time.Duration
	retries int
	backoff BackoffFactory
	overlap OverlapPolicy
}

// WithTimeout limits each job attempt to timeout.
func WithTimeout(timeout time.Duration) RegistrationOption {
	if timeout <= 0 {
		panic("scheduler: timeout must be greater than zero")
	}

	return func(options *registrationOptions) {
		options.policy.timeout = timeout
	}
}

// WithRetry retries a failed job up to attempts total attempts. factory creates
// a new backoff for each scheduled execution.
func WithRetry(attempts int, factory BackoffFactory) RegistrationOption {
	if attempts < 2 {
		panic("scheduler: retry attempts must be at least two")
	}
	if factory == nil {
		panic("scheduler: retry backoff factory must not be nil")
	}

	return func(options *registrationOptions) {
		options.policy.retries = attempts
		options.policy.backoff = factory
	}
}

// WithOverlap configures overlap behavior for one job registration.
func WithOverlap(policy OverlapPolicy) RegistrationOption {
	if policy != AllowOverlap && policy != SkipOverlap {
		panic("scheduler: invalid overlap policy")
	}

	return func(options *registrationOptions) {
		options.policy.overlap = policy
	}
}
