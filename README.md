# schedulerok

A minimal, Go-native scheduler that owns its timer loop end to end and lets
schedules change their own mind.

> **Status:** early development. The API is not stable yet.

## About

`schedulerok` separates two concerns that most schedulers blur together: a
`Job` only knows how to run, and a `Schedule` only knows how to calculate its
next execution time. The scheduler owns everything in between — startup,
timing, cancellation, and graceful shutdown — and reacts to either side
changing, including a schedule that redefines itself after every run.

```go
s := scheduler.New()

_, err := s.AddIntervalFunc(5*time.Minute, heartbeat)
if err != nil {
	return err
}

return s.Run(ctx)
```

Configuration loading stays outside the library. Applications translate their
own YAML, environment variables, or database rows into `Add` calls with
stable IDs; `schedulerok` never parses configuration itself.

## Registering jobs

`Add` and `AddFunc` take any `Schedule`. `AddIntervalJob`/`AddIntervalFunc`
and `AddCronJob`/`AddCronFunc` build the schedule for you. Every one of them
has an `AddAdaptiveXxx` counterpart that takes an `AdaptiveJob` instead:

```go
// Generic: bring your own Schedule.
s.Add(schedule, job)
s.AddFunc(schedule, func(ctx context.Context) error { return nil })

// Fixed interval and cron, job or plain function.
s.AddIntervalJob(time.Minute, job)
s.AddIntervalFunc(time.Minute, func(ctx context.Context) error { return nil })
s.AddCronJob("*/5 * * * *", job)
s.AddCronFunc("*/5 * * * *", func(ctx context.Context) error { return nil })

// Adaptive: the job decides its next Schedule after each run.
s.AddAdaptiveFunc(schedule, func(ctx context.Context) (scheduler.Schedule, error) { return nil, nil })
s.AddAdaptiveIntervalJob(time.Minute, adaptiveJob)
s.AddAdaptiveIntervalFunc(time.Minute, func(ctx context.Context) (scheduler.Schedule, error) { return nil, nil })
s.AddAdaptiveCronJob("*/5 * * * *", adaptiveJob)
s.AddAdaptiveCronFunc("*/5 * * * *", func(ctx context.Context) (scheduler.Schedule, error) { return nil, nil })
```

## Adaptive schedules

Not every job runs on a fixed cadence. A poll loop driven by a server
response — a chat API's `PollingIntervalMillis`, a rate-limited endpoint's
retry hint — needs to pick its own next execution after it runs, not before.
`AdaptiveJob` covers that case without touching the plain `Job` contract:

```go
_, err := s.AddAdaptiveIntervalFunc(2*time.Second, func(ctx context.Context) (scheduler.Schedule, error) {
	delay, err := poll(ctx)
	if err != nil {
		return nil, err
	}
	return scheduler.NewIntervalSchedule(delay)
})
```

Returning `nil` keeps the current schedule. A plain `Job` passed to `Add` is
wrapped internally, so `Add` accepts either kind through the same call.

## Lifecycle, policies, and runtime control

Per-registration policies cover timeout, retry with isolated backoff state,
and overlap behavior. Lifecycle hooks fire around each attempt without
requiring a logging dependency in the core:

```go
s.AddIntervalFunc(time.Minute, job,
	scheduler.WithTimeout(10*time.Second),
	scheduler.WithRetry(3, backoffFactory),
	scheduler.WithOverlap(scheduler.SkipOverlap),
	scheduler.WithHooks(scheduler.Hooks{
		OnFailure: func(_ context.Context, e scheduler.Event) {
			log.Printf("%s failed: %v", e.JobID, e.Error)
		},
	}),
)
```

`Add` and `Remove` both work while `Run` is already executing — a job can be
registered or pulled out mid-flight, and the central loop wakes up to react
immediately instead of waiting for its next tick.

A registration whose `Schedule` stops advancing does not take the rest of
the scheduler down with it. It freezes in place, fires `OnFailure`, and
stays out of consideration until `Remove` is called explicitly;
`FrozenIDs()` reports which registrations are stuck.

## Example

Run the basic interval scheduler and stop it with `Ctrl+C`:

```bash
go run ./examples/basic
```

The example demonstrates interval registration, a stable job ID, lifecycle
hooks, and graceful shutdown. Output identifies its source:

```text
[hook.OnStart] heartbeat started (attempt 1)
[job.Run] heartbeat
[hook.OnSuccess] heartbeat completed
```

## Requirements

- Go 1.24.6 or later

## Support

schedulerok is one of [Candango Open Source Group
](http://www.candango.org/projects/) initiatives. It is available under
the [MIT License](./LICENSE).
