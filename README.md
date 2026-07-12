# schedulerok

`schedulerok` is a Go library for defining and running scheduled jobs.

> **Status:** early development. The API is not stable yet.

## Direction

The goal is to provide a quick, configuration-oriented scheduler experience
with a Go-native runtime.

The scheduler owns the scheduling timer and lifecycle: registration, startup,
cancellation, graceful shutdown, and waiting for running jobs.

## Building blocks

`schedulerok` coordinates the scheduling concerns needed by applications:

- calculating the next execution for cron schedules;
- retry backoff policies;
- intervals, timeouts, retries, overlap behavior, and job lifecycle.

## Intended developer experience

Applications register work directly and start a scheduler with a small amount
of code:

```go
s := scheduler.New()

_, err := s.AddIntervalFunc(5*time.Minute, heartbeat)
if err != nil {
	return err
}

return s.Run(ctx)
```

Configuration loading and job factories belong to consuming applications. They
can translate their configuration into `Add` calls with stable IDs.

Configuration loading belongs to the consuming application. The library should
remain agnostic about YAML, environment variables, databases, logging, and
metrics while exposing the hooks needed for those integrations.

## Initial scope

- interval and cron schedules;
- job timeouts and retry policies;
- explicit overlap behavior per job;
- context-based cancellation and graceful shutdown;
- application-owned configuration translated into scheduler registrations.

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
