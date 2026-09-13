# Decisions

## Queue mechanism: Lists, not Streams
**Chose:** Redis Lists (`LPUSH` to enqueue, `BRPOP` to dequeue) for the job queue,
for the entire project, not just stage 1.
**Over:** Redis Streams with consumer groups (`XADD` / `XREADGROUP` / `XACK` / `XCLAIM`).
**Because:** The point of this project is to implement claim tracking, ownership,
and reclaim-on-timeout ourselves, by hand, against a primitive that doesn't already
solve it. Streams would hand us that for free, which defeats the purpose of building
it. Concretely, Streams give up, and we are deliberately not using:
  - **A pending-entries list (PEL)** — per-consumer-group bookkeeping of which
    message is owned by which consumer, since when, with delivery counts, all
    maintained atomically by Redis. We will build the equivalent ourselves in
    stage 2 (something like a `claimed` sorted set keyed by claim time).
  - **`XCLAIM` / `XAUTOCLAIM`** — atomic "steal this message from a dead consumer"
    primitives that hand a stalled message to a new owner in one call, with
    Redis itself enforcing that only one consumer ends up owning it. We will
    build the equivalent ourselves in stage 4 via heartbeat timeouts, and we
    own the atomicity of that handoff (it goes in a Lua script).
**Breaks if:** Our hand-rolled claim/reclaim logic has a race condition that
`XCLAIM` would have ruled out structurally (e.g. two workers both deciding a
heartbeat is stale and both reclaiming the same job at once). Streams don't
make that class of bug impossible for us — we're choosing to be responsible for
finding and closing it ourselves, via the atomic Lua scripts CLAUDE.md requires
for anything that must be atomic.

## Job id: UUID4, not a Redis INCR counter
**Chose:** `uuid.uuid4()`, generated client-side by whatever process submits the job,
before it ever touches Redis.
**Over:** `INCR job:counter` to hand out sequential integer ids.
**Because:** Stage 5 adds an HTTP gateway that can receive concurrent submissions.
A UUID needs no round trip to Redis and no shared counter key to generate — every
submitter mints its own id independently with zero coordination and zero
contention. An INCR counter is a single hot key that every submission serializes
through; at this project's scale that's not a performance problem, but it is an
unnecessary coordination point for something that doesn't need one.
**Breaks if:** Two processes generate the same UUID and collide — a real
possibility in theory, but at 2^122 bits of randomness it will not happen in the
lifetime of this project, so it's an acceptable risk. The counter alternative
trades that away for a different, more plausible risk: if the counter key were
ever reset (Redis restored from a stale backup, a `FLUSHDB` in dev, a typo)
newly issued ids would collide with existing `job:<id>` hashes and silently
overwrite old job records. The cost of the UUID choice is losing sortable,
human-readable ids — worth it for the CLI's `status <job-id>` command, you'll
be copy-pasting a UUID instead of typing `42`.

## Job storage: Hash + List split, not a single JSON blob in the list
**Chose:** Job payload and state live in a Redis Hash at `job:<id>`. The list at
`queue:pending` holds only the id, used purely for ordering.
**Over:** Pushing the entire job (task, args, status) as one JSON string directly
into the list.
**Because:** The hash needs to persist and be updated after the job leaves the
queue — a worker writes `status`/`result`/`error` back into it once the id has
already been popped. If the payload only existed inside the list entry, that
record would be gone the moment `BRPOP` removed it, with nowhere to write the
outcome. This split is also what the CLI's `status <job-id>` command (stage 6)
will read from directly, independent of queue position.
**Breaks if:** A hash and its queue entry can drift apart — e.g. a job id sits
in `queue:pending` but its hash was never written (or the reverse). Stage 1's
submit script writes both un-conditionally, so this isn't a concern yet; it
becomes one once retries and reclaiming can independently touch either side.

## Task failures caught in the worker, not left to crash it
**Chose:** `run_job` wraps `func(*args)` in a bare `except Exception`. A
failing task writes `status: error` + the exception message to the job's
hash and the worker loops around to the next `BRPOP`.
**Over:** Letting the exception propagate and kill the worker process.
**Because:** A task function's failure is job data, not a scheduler failure —
the invariant is that a job ends up pending, claimed, completed, or in the
DLQ, and "crashed the worker" isn't one of those states. Task code is
arbitrary and will throw for reasons that have nothing to do with the
scheduler (bad input, a bug in someone's handler); the worker's job is to
record that outcome and keep serving the queue, not to treat every handler
bug as fatal to the whole process.
**Breaks if:** A task raises something that indicates the *worker* is in a
bad state rather than the job being bad input (e.g. a `MemoryError`, or a
future Redis connection exception once we're doing more than one round trip
per job) — `except Exception` swallows those the same way and reports them
as an ordinary job error, when the correct response might be to let the
worker die and get restarted instead of limping on.

## Worker polls with a bounded BRPOP, not an indefinite block
**Chose:** `BRPOP queue:pending 5` in a loop, looping again whenever the
5-second timeout expires with nothing to pop. The client is also constructed
with `socket_timeout=10` (BRPOP's timeout + 5s of slack).
**Over:** `BRPOP queue:pending 0` (block forever until an item shows up).
**Because:** The actual bug: redis-py gives its client a default
`socket_timeout` (5s on the version installed here) — the client's own limit
on how long it'll wait to read a reply, independent of whatever timeout you
hand to the command itself. With `BRPOP ... 0`, the server blocks forever as
told, but the client gives up reading after its own 5s and raises
`redis.exceptions.TimeoutError`, misreported as a broken connection when the
server was behaving correctly the whole time. Switching to a bounded
`BRPOP ... 5` doesn't fix this by itself — it just makes the client and
server timeouts equal, so the client now races the server's legitimate
"nothing arrived" reply and loses intermittently instead of always. The real
fix is the `socket_timeout=10` pairing: it gives the server's 5s window room
to reply before the client's own patience runs out, so an empty queue
produces a clean `None` return instead of an exception. Once that pairing is
right, bounded polling over indefinite blocking is worth doing anyway: this
loop is where a stage-4 heartbeat write will go, and an indefinite block
would mean the worker never gets control back to say "I'm alive" while a job
isn't already flowing.
**Breaks if:** `socket_timeout` and `BRPOP`'s timeout aren't kept in that
order (client slack > server window) — pull them back to equal, or worse,
invert them, and this exact bug returns. The 5s bound also adds up to 5s of
latency between a job landing and an idle worker noticing, and costs an idle
worker one wasted round trip every 5 seconds — both negligible here, real at
large worker counts.
