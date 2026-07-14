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

// AdaptiveJob is a Job that may provide a replacement Schedule after each
// execution.
//
// The scheduler calls Run and then NextSchedule with the current Schedule. A
// nil Schedule preserves the current schedule. A non-nil Schedule replaces it
// for the next execution. An error means that the replacement could not be
// calculated.
type AdaptiveJob interface {
	Job
	NextSchedule(current Schedule) (Schedule, error)
}

// fixedScheduleJob adapts a Job to AdaptiveJob. NextSchedule always returns a
// nil Schedule, so the registration's Schedule never changes.
type fixedScheduleJob struct {
	job Job
}

func (f fixedScheduleJob) Run(ctx context.Context) error {
	return f.job.Run(ctx)
}

func (f fixedScheduleJob) NextSchedule(Schedule) (Schedule, error) {
	return nil, nil
}
