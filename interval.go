package scheduler

import (
	"errors"
	"time"
)

// ErrInvalidInterval indicates that an interval is not positive.
var ErrInvalidInterval = errors.New("interval must be greater than zero")

// IntervalSchedule calculates execution times separated by a fixed interval.
type IntervalSchedule struct {
	interval time.Duration
}

// NewIntervalSchedule returns a schedule with a positive fixed interval.
func NewIntervalSchedule(interval time.Duration) (IntervalSchedule, error) {
	if interval <= 0 {
		return IntervalSchedule{}, ErrInvalidInterval
	}

	return IntervalSchedule{interval: interval}, nil
}

// Next returns the time one interval after after.
func (s IntervalSchedule) Next(after time.Time) time.Time {
	return after.Add(s.interval)
}
