package scheduler

import "context"

// Job performs one unit of scheduled work.
//
// Run receives a context that is canceled when the scheduler stops. It returns
// a non-nil error when the scheduled work fails.
type Job interface {
	Run(context.Context) error
}

// JobFunc adapts a function to Job.
type JobFunc func(context.Context) error

// Run executes fn.
func (fn JobFunc) Run(ctx context.Context) error {
	return fn(ctx)
}

// AdaptiveJob is a Job that may redefine its own Schedule after each run.
//
// RunAdaptive returns the Schedule to use for future executions, or nil to
// keep the current one. The scheduler adopts the returned Schedule under
// synchronization once RunAdaptive returns.
type AdaptiveJob interface {
	Job
	RunAdaptive(context.Context) (Schedule, error)
}

// AdaptiveJobFunc adapts a function to AdaptiveJob.
type AdaptiveJobFunc func(context.Context) (Schedule, error)

// Run executes fn and discards the returned Schedule.
func (fn AdaptiveJobFunc) Run(ctx context.Context) error {
	_, err := fn(ctx)
	return err
}

// RunAdaptive executes fn.
func (fn AdaptiveJobFunc) RunAdaptive(ctx context.Context) (Schedule, error) {
	return fn(ctx)
}

// fixedScheduleJob adapts a Job to AdaptiveJob. RunAdaptive always returns a
// nil Schedule, so the registration's Schedule never changes.
type fixedScheduleJob struct {
	job Job
}

func (f fixedScheduleJob) Run(ctx context.Context) error {
	return f.job.Run(ctx)
}

func (f fixedScheduleJob) RunAdaptive(ctx context.Context) (Schedule, error) {
	return nil, f.job.Run(ctx)
}
