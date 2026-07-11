package scheduler

import "time"

// Schedule calculates the next time to run after a reference time.
//
// Callers should pass the previous scheduled time so interval schedules do not
// drift when job execution takes longer than expected.
type Schedule interface {
	Next(after time.Time) time.Time
}
