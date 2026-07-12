# Scheduler Core API Specification

> **Status:** Core API implemented. Reliability policies and adaptive polling
> remain follow-up work.

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
remains a reusable, identity-free rule.

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

Cron parsing remains owned by `intervalok`; a cron convenience method delegates
to that parser and then calls `Add`. The generic `Add` API remains independent
of any cron parser.

## Lifecycle

`Scheduler` is the public coordinator. It owns multiple registered jobs,
calculates each next execution through `Schedule.Next`, and stops accepting new
runs when its runtime context is cancelled. It then waits for active jobs to
finish according to the configured shutdown behavior.

There is no public `Runner`, `Task`, `Manager`, `Entry`, or `Orchestrator` in
the core API at this stage. Private runtime state may associate a job ID with a
schedule and job, but that implementation detail is not part of the API.

## Adaptive polling

A long-lived loop that resets its own timer, such as the current YouTube chat
collector, is a service rather than a scheduled `Job`: it should run alongside
the scheduler and participate in application shutdown.

A future adaptive-polling extension may execute one poll, receive a delay from
the remote service, and let the scheduler arm the next timer with its own
clock. This is deliberately out of the initial core; it must not be modeled by
stateful cron schedules or by a job that never returns.
