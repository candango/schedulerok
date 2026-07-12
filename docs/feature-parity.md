# Scheduler Feature Parity

This document is a strategic comparison, not a release promise. Everything
marked **Backlog** is intentionally out of the current delivery scope.

## Product thesis

`schedulerok` is a configuration-friendly Go scheduler runtime. Applications
own configuration, secrets, and job factories. The runtime owns scheduling,
execution lifecycle, and operational policies.

`candango/intervalok` owns cron expression parsing. `schedulerok` consumes
any schedule implementation that satisfies `Schedule`.

## Parity matrix

| Capability | schedulerok | gocron | go-quartz | Direction |
|---|---|---|---|---|
| Job contract with `context.Context` | Available | Available | Available | Keep minimal `Job.Run(context.Context) error`. |
| Schedule contract | Available | Built in | Trigger interface | Keep `Schedule.Next(time.Time) time.Time`. |
| Fixed interval schedule | Available | Available | Simple trigger | Complete. |
| Cron expression parsing | External (`intervalok`) | Available | Available | Consume a compatible schedule; do not parse here. |
| Scheduler start and cancellation | Available | Available | Available | Scheduler owns the timer loop and reacts to context cancellation. |
| Graceful shutdown | Available | Available | Available | Stop new scheduling, then wait for active jobs. |
| Job registry and configuration factories | Backlog | Programmatic jobs | Programmatic jobs | Preserve application-owned configuration and factories. |
| Timeout policy | Available | Available | Available through configuration | Per-job, context-based timeout. |
| Retry and backoff | Available | Available | Available | Per-registration retry with isolated backoff state. |
| Overlap policy | Available | Available | Available through configuration | Per-job allow or skip semantics; queue remains future work. |
| Global concurrency limit | Not planned yet | Available | Available | Add only when real workloads require it. |
| Pause, resume, remove, and update jobs | Not planned yet | Available | Available | Add after the scheduler API is stable. |
| Lifecycle hooks | Available | Available | Listener-based extensions | Hooks for start, success, failure, skip, and retry. |
| Logs and metrics integration | Available through hooks | Available | Available | Applications choose logging and metrics. |
| Misfire handling | Backlog | Available | Available | Define explicit missed-schedule semantics before adding it. |
| Distributed coordination | Long-term backlog | Available | Available | Add only for multi-replica ownership requirements. |
| Durable jobs and schedule state | Long-term backlog | Limited | Queue-backed options | Add only when restart recovery is required. |

## References

- [gocron](https://github.com/go-co-op/gocron) provides jobs, scheduler
  lifecycle, execution limits, event listeners, and distributed options.
- [go-quartz](https://github.com/reugn/go-quartz) separates scheduler, jobs,
  and triggers, with context-aware lifecycle controls.
- [robfig/cron](https://github.com/robfig/cron) defines the compact
  `Schedule.Next(time.Time) time.Time` contract used as a model for schedule
  interoperability.

## Guardrails

- Do not add a feature merely to match another library.
- Keep configuration loading outside the library.
- Prefer small contracts and application-controlled integrations.
- Treat distributed coordination and persistence as separate product layers.
