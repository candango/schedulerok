package scheduler

import "time"

// Schedule calculates the next time to run after a reference time.
//
// Callers should pass the previous scheduled time so interval schedules do not
// drift when job execution takes longer than expected.
type Schedule interface {
	Next(after time.Time) time.Time
}

// ScheduleFunc adapts a function to Schedule.
type ScheduleFunc func(time.Time) time.Time

// Next returns the next time calculated by fn.
func (fn ScheduleFunc) Next(after time.Time) time.Time {
	return fn(after)
}
