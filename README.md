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
and `AddCronJob`/`AddCronFunc` build the schedule for you. An `AdaptiveJob`
can be passed through the same registration methods; the scheduler detects it
and asks for its next schedule after each execution:

```go
// Generic: bring your own Schedule.
s.Add(schedule, job)
s.AddFunc(schedule, func(ctx context.Context) error { return nil })

// Fixed interval and cron, job or plain function.
s.AddIntervalJob(time.Minute, job)
s.AddIntervalFunc(time.Minute, func(ctx context.Context) error { return nil })
s.AddCronJob("*/5 * * * *", job)
s.AddCronFunc("*/5 * * * *", func(ctx context.Context) error { return nil })

// Adaptive: use the regular registration methods. The scheduler detects
// AdaptiveJob and asks it for the next Schedule after Run returns.
s.Add(schedule, adaptiveJob)
s.AddIntervalJob(time.Minute, adaptiveJob)
s.AddCronJob("*/5 * * * *", adaptiveJob)
```

## Adaptive schedules

Not every job runs on a fixed cadence. A poll loop driven by a server
response — a chat API's `PollingIntervalMillis`, a rate-limited endpoint's
retry hint — needs to pick its own next execution after it runs, not before.
`AdaptiveJob` covers that case without changing the plain `Job` contract:

```go
type pollJob struct {
	delay time.Duration
}

func (j *pollJob) Run(ctx context.Context) error {
	delay, err := poll(ctx)
	if err != nil {
		return err
	}

	j.delay = delay
	return nil
}

func (j *pollJob) NextSchedule(current scheduler.Schedule) (scheduler.Schedule, error) {
	if j.delay <= 0 {
		return nil, nil // keep the current schedule
	}

	return scheduler.NewIntervalSchedule(j.delay)
}

job := &pollJob{}
schedule, err := scheduler.NewIntervalSchedule(2 * time.Second)
if err != nil {
	return err
}

_, err = s.Add(schedule, job)
```

The scheduler calls `Run(ctx)` and then `NextSchedule(current)`. Returning
`nil, nil` keeps the current schedule; returning a schedule replaces it for
the next execution. An error reports that the replacement could not be
calculated. The `current` argument is available to stateful schedules that
need to inspect the existing schedule before producing a replacement.

A plain `Job` passed to `Add` is wrapped internally, so both job kinds use the
same registration API. For function-based jobs, `AddAdaptiveFunc` accepts the
execution and schedule-selection functions separately:

```go
_, err := s.AddAdaptiveFunc(
	schedule,
	func(ctx context.Context) error {
		return poll(ctx)
	},
	func(current scheduler.Schedule) (scheduler.Schedule, error) {
		return scheduler.NewIntervalSchedule(5 * time.Minute)
	},
)
```

If the schedule-selection function is `nil`, `AddAdaptiveFunc` delegates to
`AddFunc` and keeps the schedule fixed.

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

Scheduler-level observability is separate from per-job hooks. Configure
`WithSchedulerHooks` to receive lifecycle callbacks and optional tick events
with the tick time, the `JobID`s that were due, and the `JobID`s actually
dispatched. With tick observation disabled, the scheduler does not allocate
tick events or job ID slices, keeping the normal path close to zero overhead.
Callbacks must be fast and non-blocking.

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

- Go 1.25 or later

## Support

schedulerok is one of [Candango Open Source Group
](http://www.candango.org/projects/) initiatives. It is available under
the [MIT License](./LICENSE).
