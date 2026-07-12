package scheduler

import "github.com/candango/intervalok/cron"

// NewCronSchedule parses expr with intervalok and returns it as a Schedule.
func NewCronSchedule(expr string) (Schedule, error) {
	return cron.NewCronSeries(expr)
}
