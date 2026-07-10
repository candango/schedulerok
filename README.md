# schedulerok

`schedulerok` is a Go library for defining and running scheduled jobs.

> **Status:** early development. The API is not stable yet.

## Direction

The goal is to provide a quick, configuration-oriented scheduler experience
with a Go-native runtime.

Each job will own its scheduling timer. The scheduler will own the lifecycle:
registration, startup, cancellation, graceful shutdown, and waiting for running
jobs.

## Building blocks

`schedulerok` coordinates the scheduling concerns needed by applications:

- calculating the next execution for cron schedules;
- retry backoff policies;
- intervals, timeouts, retries, overlap behavior, and job lifecycle.

## Intended developer experience

Applications should be able to register job factories, load their own
configuration, and start a runner with a small amount of code:

```go
registry := scheduler.NewRegistry()
registry.Register("heartbeat", jobs.NewHeartbeat)

runner, err := scheduler.NewFromConfig(cfg, registry)
if err != nil {
	return err
}

return runner.Run(ctx)
```

Configuration loading belongs to the consuming application. The library should
remain agnostic about YAML, environment variables, databases, logging, and
metrics while exposing the hooks needed for those integrations.

## Initial scope

- interval and cron schedules;
- job timeouts and retry policies;
- explicit overlap behavior per job;
- context-based cancellation and graceful shutdown;
- a registry-based path for configuration-driven job creation.

## Requirements

- Go 1.24.6 or later

## License

TBD
