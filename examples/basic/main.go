package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/candango/schedulerok"
)

type heartbeatJob struct{}

func (heartbeatJob) Run(context.Context) error {
	log.Println("[job.Run] heartbeat")
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := scheduler.New()
	_, err := s.AddIntervalJob(
		5*time.Second,
		heartbeatJob{},
		scheduler.WithID("heartbeat"),
		scheduler.WithHooks(scheduler.Hooks{
			OnStart: func(_ context.Context, event scheduler.Event) {
				log.Printf("[hook.OnStart] %s started (attempt %d)", event.JobID, event.Attempt)
			},
			OnSuccess: func(_ context.Context, event scheduler.Event) {
				log.Printf("[hook.OnSuccess] %s completed", event.JobID)
			},
			OnFailure: func(_ context.Context, event scheduler.Event) {
				log.Printf("[hook.OnFailure] %s failed: %v", event.JobID, event.Error)
			},
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := s.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
