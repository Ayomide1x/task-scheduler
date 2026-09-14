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

## Claiming: BLMOVE into a per-worker list, not a Lua script
**Chose:** `BLMOVE queue:pending processing:<worker_id> RIGHT LEFT <timeout>`
to atomically move a job id out of the pending queue and into a list scoped
to the worker that claimed it, followed by a plain `HSET` onto the job's
hash for `status`/`worker_id`/`claimed_at`. The list membership is the
durable claim record; the hash fields are convenience metadata that may
legitimately lag or be missing if the worker dies before writing them.
**Over:** A single Lua script doing `RPOP queue:pending` followed by the
`HSET`, as one atomic unit (drafted and rejected during design — see the
tradeoff below).
**Because:** The two options close different gaps, and the Lua version
closes the less important one. Under Lua, `RPOP` returns the job id
directly into the worker process's memory — nowhere in Redis. If the worker
dies after the script returns but before anything downstream reads that id,
the id exists nowhere in Redis at all. Recovering it means scanning every
`job:*` key (or trusting a secondary index that itself has to be kept in
sync and can drift out from the actual claim state) to notice a job stuck at
`status: pending` that's no longer in the queue. Under `BLMOVE`, the id
never leaves Redis — it's relocated, atomically, from one list Redis
manages to another. If the worker dies immediately after, the id is sitting
in plain sight in `processing:<worker_id>`, discoverable by reading one
list, no scan and no secondary index required. What Lua would have bought
us — `claimed_at` and `worker_id` landing in the same atomic step as the
pop — is metadata convenience. What it would have cost us — the job id
itself becoming unrecoverable on a worker crash — is the exact failure this
stage exists to close. Losing metadata timing is a smaller problem than
losing the id.
**Breaks if:** A worker dies between the `BLMOVE` returning and the `HSET`
landing. The claim is still fully durable (the id is in
`processing:<worker_id>`), but the job's hash will show stale or missing
`status`/`worker_id`/`claimed_at` until something reconciles it — meaning
stage 4's orphan detection cannot trust the hash alone and must treat each
worker's `processing:<worker_id>` list, not `job:*` hash contents, as the
source of truth for "what is this worker currently holding." That's a
deliberate design constraint this decision places on stage 4, not an
oversight.

## Retries: attempts on the hash, delay via a sorted set, promotion via Lua
**Chose:** An `attempts` field on `job:<id>`, updated with a plain `HSET`
(no atomic increment) only in the failure path. A separate sorted set,
`retry:scheduled` (member = job id, score = eligible unix timestamp), holds
jobs waiting out their backoff. A Lua script (`promote_retries.lua`) atomically
moves due jobs from `retry:scheduled` back onto `queue:pending`, called by
every worker once per loop iteration, ahead of its `BLMOVE`.
**Over:** Immediately re-queuing a failed job and having the worker
`time.sleep(backoff)` in-process before running it; a TTL-key +
keyspace-notification wakeup instead of a polled sorted set; an atomic
`HINCRBY` for `attempts` instead of a plain read-increment-write.
**Because:** In-process sleep ties up a worker slot for the entire backoff
window instead of freeing it for other ready jobs, and makes a
sleeping-but-alive worker indistinguishable from a dying one to whatever
staleness check stage 4 adds. Keyspace notifications depend on Pub/Sub,
already rejected in stage 1 for the same durability reason: a notification
missed at the exact expiry instant loses the retry silently. The plain
`HSET` for `attempts` is safe today only because stage 2's claim guarantees
exactly one worker is ever touching a given job's hash at a time — there is
no concurrent writer to race.
**Breaks if (promotion):** `promote_retries.lua` skips atomicity. If two
workers each read the same due job id and independently `ZREM`+`LPUSH` it,
the id lands in `queue:pending` twice — two list entries, two separate
`BLMOVE` calls can each claim one, and the same job runs concurrently on two
workers. That's the exact thing stage 2's claiming exists to prevent, so
this has to be one atomic script, not two round trips.
**Breaks if (attempts):** Stage 4 is exactly what invalidates the "one
writer at a time" assumption behind the plain `HSET`. A worker a peer has
declared dead on heartbeat grounds may not actually be dead — just slow, or
partitioned from Redis but still running. If stage 4's reclaim hands the job
to a second worker while the first is still alive and later writes its own
`attempts` update, the two writes race and the increment can be lost (last
write wins, not last-plus-one). This is the same category of constraint as
the `processing:<worker_id>`-is-source-of-truth note above: stage 4 inherits
it and has to design reclaim so a declared-dead worker's own writes can't
silently clobber the reclaiming worker's, e.g. by having reclaim invalidate
the old claim in a way the original worker can detect before it writes again.
**Breaks if (promotion lag):** Promotion is piggybacked on the same loop
that also runs jobs, not a dedicated process. A worker executing a
long-running job isn't calling `promote_retries.lua` during that time, so a
job whose backoff has already elapsed can sit past its `next_attempt_at`
until some worker in the pool is free to loop again. With every worker busy
simultaneously, every due retry waits. The fix not being built here: a
small dedicated promoter (its own process or thread, or the stage-4
coordinator once one exists) that does nothing but call this script on a
fixed interval, independent of whether any worker is free. Not needed yet
at this project's scale, but it's the first thing to add if retry latency
under load ever becomes a real problem.

## PermanentError: an escape hatch for task-level "never retry this"
**Chose:** A `PermanentError` exception class in `tasks.py` that task code
can raise deliberately; the worker catches it before the generic
`except Exception` and routes straight to the DLQ, skipping backoff
entirely.
**Over:** Leaving all task-raised exceptions to go through the normal
retry path regardless of cause, distinguishing only the scheduler-level
"unknown task" failure as immediately permanent.
**Because:** The scheduler has no general way to know whether an
exception a task raised is transient or will always happen given the same
input — that's the whole reason task-raised exceptions default to
retryable. But task authors often *do* know: a validation failure on a
malformed job argument will fail identically on attempt 5 as it did on
attempt 1. Without an explicit signal, that job burns all `MAX_ATTEMPTS`
attempts and the full backoff schedule before landing in the DLQ anyway --
`PermanentError` lets the task say so up front and skip straight there.
**Breaks if:** A task author reaches for `PermanentError` reflexively on
something that was actually transient (e.g. wrapping a flaky network call
and misjudging one of its failure modes as permanent) — that job gets one
attempt and no backoff at all, the opposite failure mode from the one this
exists to fix. The class carries no enforcement of correct usage; it's a
convention task authors have to apply carefully.

## Heartbeat runs on a background thread, not the main loop
**Chose:** A daemon thread, started once at worker startup, doing nothing
but `ZADD workers:heartbeats {worker_id: now}` every `HEARTBEAT_INTERVAL_
SECONDS`, independent of the main loop.
**Over:** Writing the heartbeat at the top of the main loop, in the same
place `promote_retries`/`reclaim_dead_workers` run.
**Because:** `run_job` blocks the whole process for the duration of
whatever the task does. A heartbeat that can only update between jobs
provides no information during a job's execution — it's indistinguishable
from `claimed_at`, which stage 2 already has. Given this project has a
`slow(seconds)` task with caller-controlled duration, any worker running one
would be wrongly declared dead the moment its runtime passed
`HEARTBEAT_TIMEOUT_SECONDS`, deterministically, not as a rare fluke. The
thread is daemonized specifically so it's never the reason the process
survives a Ctrl+C — it holds no state worth a clean shutdown for, and a
missed final heartbeat on exit is indistinguishable from a crash, which
every other part of this design already has to tolerate.
**Breaks if:** The heartbeat thread hangs or dies independently of the main
thread (a bug, not modeled here) — the worker keeps processing jobs
normally while looking dead to its peers, which is the same false-positive
failure mode discussed below, just from a different cause. Also: sharing
one `redis.Redis` client between the main thread and this one is safe
because redis-py's client is backed by a connection pool (separate sockets
per concurrent caller), not one socket the two threads take turns on — if
that stopped being true this would need its own client.

## Reclaim: peer workers, sorted-set heartbeats, LMOVE with no Lua
**Chose:** No dedicated coordinator process. Every worker, once per loop
iteration, reads `workers:heartbeats` (sorted set: member = worker id,
score = last heartbeat unix ts) for ids older than `HEARTBEAT_TIMEOUT_
SECONDS`, and for each one, moves whatever sits in `processing:<dead_id>`
back into `queue:pending` (or `queue:dead`, if reclaimed too many times)
via a plain `LMOVE` — no Lua script.
**Over:** A dedicated coordinator process for detection+reclaim (closer to
the "coordinator" half of CLAUDE.md's own "a coordinator (or peer worker)"
phrasing); a Lua script wrapping the reclaim move.
**Because:** A separate coordinator is a new component for a job this loop
can already piggyback, the same reasoning stage 3 used for retry promotion.
The Lua script would be solving a problem that doesn't exist here: unlike
`promote_retries` (which reads many due ids and acts on all of them in one
call, requiring one atomic step to avoid double-promoting), reclaiming one
worker's list is always a single, specific relocation. `LMOVE`'s own
atomicity already gives us what we need — if two peers race to reclaim the
same dead worker, the first successful `LMOVE` empties the source, and the
second peer's call simply returns `None`. Two peers can (harmlessly) both
compute the same reclaim decision via their own `LINDEX` read; only one of
them ever succeeds in actually moving anything, because that part is atomic
and the loser observes an empty source.
**Breaks if:** The over-`MAX_RECLAIMS` case moves straight to `queue:dead`
in one `LMOVE`, deliberately not via a provisional landing in
`queue:pending` followed by a correction. Landing in `queue:pending` first
would leave a window where a live worker's `BLMOVE` could claim and start
running the job before this code redirects it to the DLQ — leaving it
simultaneously "in the DLQ" and "being executed," a real collision of the
four-state invariant, not a remote one. Going straight to the decided
destination in one atomic move removes that window entirely.

## Reclaims counted separately from attempts, with their own cap
**Chose:** A `reclaims` field on `job:<id>`, distinct from `attempts`,
incremented only by `reclaim_dead_workers`/`_reclaim_job`, capped at
`MAX_RECLAIMS` before the job goes to the DLQ instead of back to
`queue:pending`.
**Over:** Leaving `attempts` untouched by reclaim entirely, with no cap on
how many times a job can be reclaimed.
**Because:** A worker dying isn't the job's fault, so folding reclaim into
the same counter as task-level failures would penalize a job for its
infrastructure's problems. But leaving reclaim completely unbounded means a
job that reliably kills every worker that touches it (an OOM trigger, say)
cycles pending → claimed → orphaned → reclaimed forever, satisfying "never
silently lost" on a technicality while never actually resolving. A separate
bounded counter keeps the two failure categories — the task failed vs. the
task's worker died — independently accounted for while still guaranteeing
termination for a job that's poison in either sense.
**Breaks if:** Nothing structurally, but the two counters can both be
non-zero and telling different stories on the same job (failed twice on its
own, then orphaned once) — the DLQ's `last_error` only reflects whichever
one happened last, so the full history of *why* a job died has to be read
from `attempts` + `reclaims` + `last_error` together, not from `last_error`
alone.
**Also inherited from stage 3:** the plain `HSET` used for `attempts` was
justified there by "only one worker is ever touching a given job's hash at
a time." Stage 4 is exactly what breaks that assumption — a worker declared
dead on heartbeat grounds may still be alive and mid-write to the same
hash. Both `attempts` and `reclaims` remain plain reads-then-writes, not
atomic increments; see the fencing entry below for how (and how
incompletely) this is addressed.

## Fencing gap: compare-before-write narrows, does not close
**Chose:** Before `run_job` writes a final outcome (`done`, DLQ via
`PermanentError`, DLQ via exhausted attempts, or a scheduled retry) for a
job that actually called `func()`, it re-reads `status` and `worker_id`
from the hash and skips the write if either no longer matches (`_still_
owns`). If a reclaim has already moved the job on, this worker's result is
logged and discarded instead of written.
**Over:** No check at all (the pre-stage-4 behavior); a real fencing token.
**Because:** Without any check, a worker that heartbeat-timed-out but is
still alive and running writes its result over whatever the reclaiming
worker (or a second claim after reclaim) has since written — silently,
since a plain `HSET` doesn't know or care what was there before. The check
narrows this: it shrinks the vulnerable window from "the job's entire
execution time" (could be minutes, for a slow task) down to "the time
between this read and this worker's own subsequent write" — one round
trip instead of a whole job's runtime.
**Breaks if:** Be precise about what this is: check-then-write is not
atomic. A reclaim can land in the gap between `_still_owns` reading
`status`/`worker_id` and the caller's `HSET` actually executing —
narrower than before, but still a real window, still open. This does not
close the problem the previous design conversation identified; it reduces
its probability, nothing more.
**The actual fix, not built:** a fencing token — a monotonically increasing
number written atomically with each claim (e.g. `INCR job:<id>:epoch` at
claim time, stored alongside `worker_id`), that every subsequent write for
that job must present and have checked, atomically, against the job's
current epoch (a Lua script: compare-and-set in one step, not two separate
round trips like `_still_owns` does now). Reclaiming a job would bump the
epoch; a zombie worker's write, presenting the old epoch, would be rejected
by the script itself rather than merely discouraged by a stale-but-still-
racy Python-side check. This is the standard fix for exactly this class of
problem (the same idea behind Kubernetes leases, Chubby/ZooKeeper session
fencing, etc.) and is deliberately not being built here — it's a
meaningfully larger piece of machinery than this project's scope calls for,
and the residual risk without it is already bounded by the idempotent-
handler invariant this project has required since stage 1.

## Known simplification: no clock-skew correction
**Chose:** Heartbeat scores and the reclaimer's staleness cutoff both use
each machine's own local wall clock (`time.time()`), with no correction for
drift between hosts.
**Because:** For a single-host Docker Compose deployment (stage 8), every
worker container shares the host's clock, so there is no skew to correct
for — this is a real simplification, not a false economy, given the
project's actual deployment target.
**Breaks if:** This project (or a task modeled on it) ever runs workers
across multiple machines. Clock drift between hosts would then be
indistinguishable from an actually-stale heartbeat, or could mask a
genuinely stale one, without either side doing anything wrong. The fix
would be routing both the heartbeat write and the staleness comparison
through a single shared clock (e.g. Redis's own `TIME` command) instead of
each host's local clock — not needed at this project's current scope, but
the first thing to revisit if workers ever stop sharing a machine.

## Rate limiting: token bucket in Lua, IP as client identity
**Chose:** A single Lua script (`token_bucket.lua`) doing the full
refill-check-decrement sequence against a per-client hash
(`ratelimit:<ip>`) in one atomic round trip, applied only to `POST /jobs`.
Client identity is `req.socket.remoteAddress` — the actual TCP peer
address — with Express's `trust proxy` left off.
**Over:** `WATCH`/`MULTI`/`EXEC` for the atomicity; a client-supplied
header (`X-Client-Id`) or `X-Forwarded-For` for identity.
**Because:** `WATCH`/`MULTI`/`EXEC` costs at least three round trips in the
uncontended case and needs client-side retry logic for when `EXEC` aborts;
the Lua script does the read, refill math, and write server-side in one
call, with nothing to race against and nothing to retry. On identity: this
project has no authentication layer, so every identification scheme has a
bypass — a self-declared header is trivially defeated by sending a
different value per request, and reading `X-Forwarded-For` with nothing in
front of this gateway stripping or overwriting it would be worse than no
rate limiting at all (a free, trivial bypass for anyone willing to set a
header). The TCP peer address is the one thing a client can't just declare
a different value for.
**Breaks if:** Multiple real clients share one public IP (home network,
corporate NAT, office proxy) — they share one bucket, so one heavy client
can get its innocent neighbors 429'd for requests they never made. A single
client that legitimately spans multiple source addresses (horizontal
scaling, rotating IPs) gets a fresh bucket per address and evades the limit
entirely, intentionally or not. And concretely for this project: with
everything running on one machine during actual use (a student's laptop, or
one Docker Compose host), most or all traffic plausibly arrives from the
same source address, making per-client limiting behave like a single global
bucket in practice — worth knowing going in, not a bug to chase down later.
If a real reverse proxy is ever put in front of this gateway,
`X-Forwarded-For` becomes the right source for client identity and
`trust proxy` should be revisited then, not before.

## Cross-language duplication: job schema, and HEARTBEAT_TIMEOUT_SECONDS
**Chose:** The gateway (`gateway.js`) writes the same job hash shape
(`task`/`args`/`status`/`created_at`) that `worker.py` reads and writes, as
an independent implementation in a second language. It also redefines
`HEARTBEAT_TIMEOUT_SECONDS = 15` locally, to compute `/stats`'
`workers_alive` the same way `worker.py`'s reclaim logic defines staleness.
Nothing links these two copies of either value.
**Over:** A shared schema/config definition (e.g. a JSON or YAML file both
languages load at startup); making the gateway the only writer of job state
and having workers read jobs via the gateway too, rather than each talking
to Redis directly.
**Because:** Neither alternative was worth building for two small, rarely-
changing values at this project's current size. A shared config file is
real infrastructure — a new artifact both a Python and a Node process must
agree on the location and format of, for something that's currently just
one string schema and one integer. Making the gateway the sole writer would
be a much larger architectural change: it would mean workers no longer talk
to Redis directly for claiming, which contradicts the entire point of this
project (owning the queue mechanics directly against Redis, not through an
intermediary service) and would turn the gateway into a second bottleneck
and single point of failure between every worker and its queue.
**Breaks if:** Either copy changes without the other. If `worker.py`'s job
hash schema changes (a renamed field, a new required one) and the gateway
isn't updated to match, `POST /jobs` will silently write jobs the worker
can't correctly parse, or `GET /jobs/:id` will silently omit or mis-type a
field the worker relies on — nothing detects this; it just produces wrong
behavior downstream with no error at the point of drift. If
`HEARTBEAT_TIMEOUT_SECONDS` changes in `worker.py` but not in `gateway.js`
(or vice versa), `/stats`' `workers_alive` count and the workers that
`reclaim_dead_workers` actually treats as dead silently disagree — the
gateway could report a worker "alive" that peers have already reclaimed
from, or the reverse.
**The real fix, not built:** either a single schema definition both
languages load (even a plain JSON file listing field names and the shared
constants, read at startup by both `worker.py` and `gateway.js`, so a
change in one place is visible to both even if not enforced), or collapsing
to one writer of job state so there's only one implementation to keep
correct. Retiring `submit_job.py` in favor of the gateway (see below)
removes one of what were three independent writers of this schema — a real
reduction in the drift surface, though it doesn't touch the
`HEARTBEAT_TIMEOUT_SECONDS` duplication, which remains open.

## submit_job.py retired now that the gateway exists
**Chose:** Delete `submit_job.py`. Job submission goes through the gateway
(`POST /jobs`) from stage 5 onward; there is no longer a script that writes
job state directly to Redis.
**Over:** Keeping it as a direct-to-Redis dev/testing convenience alongside
the gateway.
**Because:** CLAUDE.md already states the reasoning that applies here,
originally about the CLI: "It must never connect to Redis directly — two
paths into job state will drift." `submit_job.py` writing the job schema
independently is exactly that risk, just via a script instead of a second
service — and it's the same duplication problem the cross-language entry
above documents, except this copy had no justification left once the
gateway could do everything it did. Keeping it "just for quick manual
testing" would mean maintaining a third implementation of the job-write
path in lockstep with the other two, forever, for a convenience every test
in this project can now get from `curl` against the gateway instead.
**Breaks if:** Nothing today — its functionality is fully covered.
Recoverable from git history if a direct-to-Redis debug path is ever
genuinely needed again, but that should be a deliberate decision at the
time, not a leftover script kept around by default.
