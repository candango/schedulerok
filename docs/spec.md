# Scheduler Core API Specification

> **Status:** Core API implemented for `0.2.0`, including adaptive schedules.

## Scope

`schedulerok` is an in-process scheduler. It owns registration, scheduling,
startup, cancellation, and graceful shutdown. It does not parse configuration
or own application job factories.

## Existing contracts

```go
type Job interface {
	Run(context.Context) error
}

type Schedule interface {
	Next(after time.Time) time.Time
}

type AdaptiveJob interface {
	Job
	NextSchedule(current Schedule) (Schedule, error)
}
```

`IntervalSchedule` is the built-in fixed-interval implementation. A parsed
`*cron.CronSeries` from `github.com/candango/intervalok/cron` also satisfies
`Schedule` structurally.

## Construction

The default public constructor follows the `robfig/cron` style:

```go
scheduler := scheduler.New()
```

The scheduler owns the runtime clock and timers. It uses the system clock by
default; tests and advanced callers configure a clock at construction time,
never with `nil` arguments or mutable post-construction setup:

```go
scheduler := scheduler.New(scheduler.WithClock(fakeClock))
```

Changing the clock after the scheduler starts is unsupported.

## Registration API

The primary API receives the two core contracts directly and returns the ID of
the registered job:

```go
id, err := scheduler.Add(schedule, job)
id, err := scheduler.AddFunc(schedule, fn)
```

The scheduler generates a unique ID when the caller does not supply one.
Applications that need a stable configuration, logging, or metrics key provide
one explicitly:

```go
id, err := scheduler.Add(schedule, job, scheduler.WithID("heartbeat"))
```

The ID belongs to the registration of a `Schedule` with a `Job`; a `Schedule`
remains a reusable, identity-free rule. `Remove(id)` stops future runs for a
registration; an execution already in progress is allowed to finish.

`JobFunc` adapts `func(context.Context) error` to `Job`, following the same
pattern as `http.HandlerFunc` and `cron.FuncJob`.

```go
type JobFunc func(context.Context) error
```

## Convenience registration

Applications commonly have a cron expression or fixed duration rather than a
constructed `Schedule`. These methods are thin adapters over `Add`:

```go
scheduler.AddCronJob(spec, job)
scheduler.AddCronFunc(spec, fn)
scheduler.AddIntervalJob(interval, job)
scheduler.AddIntervalFunc(interval, fn)
```

`AddInterval...` is preferred over `AddDuration...`: it matches the existing
`IntervalSchedule` type and describes scheduling semantics, whereas a duration
could mean a timeout or execution length. These methods return the same
registration ID as `Add`.

An `AdaptiveJob` uses the same registration methods as a plain `Job`. After
each execution attempt, the scheduler calls `NextSchedule(current Schedule)`;
only the schedule returned by the final attempt is applied. A nil schedule
preserves the current schedule; a non-nil schedule replaces it. An error reports
that the replacement could not be calculated. The current schedule is passed to
support stateful schedule implementations.

The former `RunAdaptive` and `AdaptiveJobFunc` contracts are removed in
`0.2.0`; adaptive jobs must implement `Run` and `NextSchedule` separately.
Function-based jobs can use `AddAdaptiveFunc(schedule, run, next, options...)`.
It delegates to `AddFunc` when `next` is nil, preserving a fixed schedule.

Cron parsing remains owned by `intervalok`; a cron convenience method delegates
to that parser and then calls `Add`. The generic `Add` API remains independent
of any cron parser.

## Execution policies

Policies are configured per registration:

```go
scheduler.Add(
	schedule,
	job,
	scheduler.WithTimeout(30*time.Second),
	scheduler.WithRetry(3, newBackoff),
	scheduler.WithOverlap(scheduler.SkipOverlap),
)
```

`WithTimeout` creates a fresh deadline for each attempt. `WithRetry` receives a
factory so concurrent scheduled runs never share mutable backoff state. The
default overlap policy is `AllowOverlap`; `SkipOverlap` discards a due run when
the previous run is still active.

## Lifecycle hooks

`WithHooks` provides synchronous per-registration callbacks for `OnStart`,
`OnSuccess`, `OnFailure`, `OnRetry`, and `OnSkip`. Each callback receives an
`Event` with the job ID, attempt, failure error, and retry delay when relevant.
Hooks must return promptly and do not choose a logging or metrics dependency.

Scheduler-level hooks observe the coordinator lifecycle separately from
per-registration hooks. The `OnTick` event exposes the tick time, the
registrations due at that tick, and the registrations actually dispatched:

```go
type TickEvent struct {
    At         time.Time
    DueJobs    []JobID
    Dispatched []JobID
}
```

Tick observability is opt-in. When no `OnTick` callback is configured, the
scheduler must not allocate a `TickEvent` or job ID slices. Callback execution
must remain fast and must not block the scheduler loop or run while holding
scheduler locks. The cost when enabled is proportional to tick frequency and
the number of observed jobs.

## Lifecycle

`Scheduler` is the public coordinator. It owns multiple registered jobs,
calculates each next execution through `Schedule.Next`, and stops accepting new
runs when its runtime context is cancelled. It then waits for active jobs to
finish according to the configured shutdown behavior.

There is no public `Runner`, `Task`, `Manager`, `Entry`, or `Orchestrator` in
the core API at this stage. Private runtime state may associate a job ID with a
schedule and job, but that implementation detail is not part of the API.

## Adaptive scheduling

An adaptive job performs one bounded unit of work in `Run` and can return a
replacement schedule through `NextSchedule(current Schedule)`:

```go
type PollJob struct {
	delay time.Duration
}

func (j *PollJob) Run(ctx context.Context) error {
	delay, err := poll(ctx)
	if err != nil {
		return err
	}

	j.delay = delay
	return nil
}

func (j *PollJob) NextSchedule(current Schedule) (Schedule, error) {
	if j.delay <= 0 {
		return nil, nil
	}

	return NewIntervalSchedule(j.delay)
}
```

Returning `nil, nil` preserves the current schedule. Returning a schedule
replaces it for the next execution. Returning an error reports a failed
schedule adaptation. The scheduler remains responsible for applying the
replacement and validating that it advances into the future.

A long-lived loop that never returns is still a service rather than a
scheduled `Job`; it should run alongside the scheduler and participate in
application shutdown.

## Scheduler observability

Scheduler-level lifecycle callbacks are distinct from per-registration hooks.
`SchedulerHooks` covers scheduler start, stopping, and stopped states, plus
`OnTick` for timer ticks. A tick reports `DueJobs` and `Dispatched` job IDs so
applications can observe the scheduler without exposing private registrations.
