# Distributed Task Scheduler

## What this is

A job scheduler that distributes background work across a pool of workers,
using Redis as both the message broker and the shared state store.

Core capabilities:
- Clients submit jobs through a REST gateway and poll for status
- A dynamic pool of Python workers claims and executes jobs
- Failed jobs retry with exponential backoff and jitter
- Workers that die mid-job have their work reclaimed via heartbeat timeout
- Jobs that exhaust retries land in a dead-letter queue
- The gateway rate-limits submissions and applies backpressure on queue depth
- A CLI submits jobs, checks status, watches cluster state live, and
  inspects the dead-letter queue

## Stack

- Workers: Python 3.14
- API gateway: Node.js + Express
- CLI: Python, argparse, standard library only
- Broker and state store: Redis 7
- Local orchestration: Docker Compose

No Celery, no RQ, no BullMQ. The queue mechanics are the point of this
project — implement them directly against Redis.

## Code conventions

- Prefer boring, readable code over clever code.
- Comment the non-obvious parts, especially anything involving atomicity,
  race conditions, or failure handling.
- Any Redis operation that must be atomic goes in a Lua script, with a
  comment explaining what breaks without atomicity.

## DECISIONS.md

Maintain a `DECISIONS.md` in the repo root. Every time a non-obvious
technical choice is made, append an entry:

```
## <Decision>
**Chose:** what we did
**Over:** the alternative
**Because:** the actual reason
**Breaks if:** the failure mode this exposes us to
```

Add entries as they happen, not at the end. Write for a reader who
wasn't present for the decision.

## Build order

Build strictly in this order. Do not skip ahead.

1. **Vertical slice.** One script enqueues a job, one worker pops it,
   executes, writes the result back. No retries, no API, no Docker.
   Must run end to end before anything else is added.
2. **Reliable claiming.** Multiple workers. A job must not be lost if a
   worker dies between claiming and finishing.
3. **Retries.** Exponential backoff with jitter, a max attempt count,
   and a dead-letter queue for exhausted jobs.
4. **Heartbeats.** Workers register and heartbeat. A coordinator (or peer
   worker) detects stale heartbeats and requeues the orphaned job.
5. **API gateway.** Express server for job submission and status polling,
   with token-bucket rate limiting backed by Redis.
6. **CLI.** See the CLI section below. Built here so it can be used to
   exercise stages 7 and 8.
7. **Backpressure.** Reject or shed submissions when queue depth exceeds
   a threshold, rather than accepting work the pool can't absorb.
8. **Docker Compose.** Redis, gateway, and a scalable worker service.

## CLI

Five commands, thin wrappers over the REST gateway:

```
scheduler submit <task> [args]   # enqueue, print job id
scheduler status <job-id>        # state, attempt count, last error
scheduler stats                  # queue depth, active workers, DLQ size
scheduler watch                  # stats on a 1s refresh loop
scheduler dlq [--limit N]        # dead-lettered jobs: id, task, attempts, last error
```

Rules:
- The CLI talks to the gateway over HTTP. It must never connect to Redis
  directly — two paths into job state will drift.
- `argparse` only. No Click, no Typer, no Rich, no curses. `watch` is a
  sleep loop that clears the screen and reprints.
- Keep it small. If it is growing past a few hundred lines, cut a command.
- Gateway URL from an env var, defaulting to `http://localhost:3000`.

## Constraints

Out of scope. Do not suggest or add these:
- Web UI or dashboard
- Job priorities or cron/scheduled jobs
- Kubernetes
- A database other than Redis
- Any distributed queue library
- Any TUI or CLI framework beyond the standard library

If something seems to require one of these, say so and explain why, but
do not add it.

## Invariants

These are the properties the system must hold. Flag any change that
would violate one.

- A job is never silently lost. It is either pending, claimed by a live
  worker, completed, or in the DLQ.
- Two workers never execute the same job concurrently. Claiming must be
  atomic.
- Delivery is at-least-once. Job handlers must therefore be idempotent,
  and this is documented for anyone writing a handler.
- A worker crash is always recoverable without manual intervention.
