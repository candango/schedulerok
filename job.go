package scheduler

import "context"

// Job performs one unit of scheduled work.
//
// Run receives a context that is canceled when the scheduler stops. It returns
// a non-nil error when the scheduled work fails.
type Job interface {
	Run(context.Context) error
}
