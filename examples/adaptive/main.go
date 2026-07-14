package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/candango/schedulerok"
)

type pollingJob struct {
	delay time.Duration
}

func (j *pollingJob) Run(context.Context) error {
	// A real job would use the context to poll a remote service and store the
	// returned delay in j.delay.
	j.delay += time.Second
	if j.delay == 0 {
		j.delay = time.Second
	}

	log.Printf("[job.Run] next delay: %s", j.delay)
	return nil
}

func (j *pollingJob) NextSchedule(current scheduler.Schedule) (scheduler.Schedule, error) {
	if j.delay <= 0 {
		return nil, nil
	}

	return scheduler.NewIntervalSchedule(j.delay)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	initial, err := scheduler.NewIntervalSchedule(time.Second)
	if err != nil {
		log.Fatal(err)
	}

	s := scheduler.New()
	_, err = s.Add(initial, &pollingJob{}, scheduler.WithID("adaptive-poll"))
	if err != nil {
		log.Fatal(err)
	}

	if err := s.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
