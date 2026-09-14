#!/usr/bin/env python3
"""Reliable-claiming worker: move a job id out of queue:pending into this
worker's own processing list, run it, write the result back onto its hash.
Failures either get rescheduled with backoff or sent to the DLQ. A
background thread heartbeats this worker's liveness; the main loop reclaims
jobs left behind by peers whose heartbeat has gone stale.

The processing:<worker_id> list is the durable claim record. The
status/worker_id/claimed_at fields on job:<id> are convenience metadata,
written in a separate step after the claim, and may be stale or missing if
this process dies between the claim and that write -- see DECISIONS.md
("Claiming: BLMOVE into a per-worker list, not a Lua script").

See DECISIONS.md for the retry/DLQ design (stage 3) and the heartbeat/
reclaim design (stage 4), including the fencing gap the compare-before-write
guard in run_job narrows but does not close.
"""
import json
import os
import random
import threading
import time
import uuid

import redis

from tasks import REGISTRY, PermanentError

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BLMOVE_TIMEOUT_SECONDS = 5

MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 1
BACKOFF_CAP_SECONDS = 60
PROMOTE_BATCH_SIZE = 50

HEARTBEAT_INTERVAL_SECONDS = 2
HEARTBEAT_TIMEOUT_SECONDS = 15
MAX_RECLAIMS = 3

PROMOTE_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "promote_retries.lua")


def schedule_retry(r, key, job_id, attempts, error):
    # Full jitter (AWS's standard shape): a random point between zero and
    # the exponential ceiling, so jobs that failed at the same moment don't
    # all retry at the same moment too.
    ceiling = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * 2 ** (attempts - 1))
    next_attempt_at = time.time() + random.uniform(0, ceiling)
    r.hset(
        key,
        mapping={
            "status": "retry_scheduled",
            "attempts": attempts,
            "last_error": error,
            "next_attempt_at": str(int(next_attempt_at)),
        },
    )
    r.zadd("retry:scheduled", {job_id: next_attempt_at})


def send_to_dlq(r, key, job_id, attempts, error):
    r.hset(key, mapping={"status": "dead", "attempts": attempts, "last_error": error})
    r.lpush("queue:dead", job_id)


def _still_owns(r, key, worker_id):
    # Not atomic with whatever write follows -- see DECISIONS.md ("Fencing
    # gap: compare-before-write narrows, does not close"). A reclaim landing
    # between this read and the caller's HSET still clobbers; this only
    # rules out the common case where the reclaim happened well before now
    # (i.e. sometime during func()'s potentially long execution).
    status, owner = r.hmget(key, "status", "worker_id")
    return status == "claimed" and owner == worker_id


def run_job(r, job_id, worker_id):
    key = f"job:{job_id}"
    job = r.hgetall(key)
    if not job:
        # The id came out of the queue but its hash is missing. Shouldn't
        # happen in stage 1 (submit_job.py writes the hash before the list
        # entry), but a missing hash shouldn't take the worker process down.
        print(f"[worker] {job_id}: no job hash found, skipping")
        return

    task_name = job["task"]
    args = json.loads(job["args"])
    func = REGISTRY.get(task_name)

    if func is None:
        # A missing registry entry is a scheduler-level failure: retrying
        # looks up the same name in the same REGISTRY and gets the same
        # None every time. No attempt was made, so attempts is left as-is.
        # No call to func() happens on this path, so there's no time window
        # for a reclaim to land -- no ownership check needed here.
        attempts = int(job.get("attempts", 0))
        send_to_dlq(r, key, job_id, attempts, f"unknown task: {task_name}")
        print(f"[worker] {job_id}: unknown task {task_name!r}, sent to DLQ")
        return

    attempts = int(job.get("attempts", 0)) + 1

    try:
        result = func(*args)
    except PermanentError as exc:
        if _still_owns(r, key, worker_id):
            send_to_dlq(r, key, job_id, attempts, str(exc))
            print(f"[worker] {job_id}: permanent failure - {exc}")
        else:
            print(f"[worker] {job_id}: permanent failure - {exc} (discarded, reclaimed elsewhere)")
    except Exception as exc:
        if not _still_owns(r, key, worker_id):
            print(f"[worker] {job_id}: failed - {exc} (discarded, reclaimed elsewhere)")
        elif attempts >= MAX_ATTEMPTS:
            send_to_dlq(r, key, job_id, attempts, str(exc))
            print(f"[worker] {job_id}: attempts exhausted ({attempts}) - {exc}")
        else:
            schedule_retry(r, key, job_id, attempts, str(exc))
            print(f"[worker] {job_id}: attempt {attempts} failed, retry scheduled - {exc}")
    else:
        if _still_owns(r, key, worker_id):
            r.hset(key, mapping={"status": "done", "result": json.dumps(result)})
            print(f"[worker] {job_id}: done - {result!r}")
        else:
            print(f"[worker] {job_id}: succeeded - {result!r} (discarded, reclaimed elsewhere)")


def heartbeat_loop(r, worker_id):
    # Runs on its own thread specifically so it keeps beating while the main
    # thread is blocked inside a long-running func() call -- a heartbeat
    # that can only update between jobs is just claimed_at with extra steps.
    # Safe to share `r` with the main thread: redis-py's client is backed by
    # a connection pool, which hands out separate sockets per concurrent
    # caller rather than one socket two threads take turns on.
    while True:
        try:
            r.zadd("workers:heartbeats", {worker_id: time.time()})
        except redis.RedisError as exc:
            print(f"[worker] {worker_id}: heartbeat write failed - {exc}")
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def reclaim_dead_workers(r):
    now = time.time()
    stale_worker_ids = r.zrangebyscore("workers:heartbeats", "-inf", now - HEARTBEAT_TIMEOUT_SECONDS)
    for dead_id in stale_worker_ids:
        processing_key = f"processing:{dead_id}"
        while True:
            # LINDEX only peeks -- it doesn't remove anything, so two peers
            # racing on the same dead worker can both see the same job_id
            # here and both decide the same destination. That's harmless:
            # the actual state change is the LMOVE below, and LMOVE is
            # atomic, so only one of the two racing LMOVE calls actually
            # moves anything -- the second finds the source already empty
            # and returns None. Redundant reads are fine; only one mutation
            # ever lands.
            job_id = r.lindex(processing_key, -1)
            if job_id is None:
                break
            _reclaim_job(r, processing_key, dead_id, job_id)
        r.zrem("workers:heartbeats", dead_id)


def _reclaim_job(r, processing_key, dead_worker_id, job_id):
    key = f"job:{job_id}"
    job = r.hgetall(key)
    reclaims = int(job.get("reclaims", 0)) + 1

    if reclaims >= MAX_RECLAIMS:
        # A single LMOVE straight to queue:dead, not a provisional landing
        # in queue:pending followed by a correction: if we moved it into
        # queue:pending first, a live worker's BLMOVE could grab and start
        # running it before we redirect it to the DLQ, leaving the job
        # simultaneously "in the DLQ" and "being executed" -- a real state
        # collision, not just an unlikely one. Going directly to the final
        # destination in one atomic move avoids that window entirely.
        moved = r.lmove(processing_key, "queue:dead", "RIGHT", "LEFT")
        if moved is None:
            return  # a racing peer already reclaimed this entry
        r.hset(
            key,
            mapping={
                "status": "dead",
                "reclaims": reclaims,
                "last_error": f"exceeded max reclaims ({MAX_RECLAIMS}); last owner {dead_worker_id} went stale",
            },
        )
        print(f"[worker] {job_id}: reclaimed from {dead_worker_id}, exceeded max reclaims, sent to DLQ")
    else:
        moved = r.lmove(processing_key, "queue:pending", "RIGHT", "LEFT")
        if moved is None:
            return  # a racing peer already reclaimed this entry
        r.hset(key, mapping={"status": "pending", "reclaims": reclaims})
        print(f"[worker] {job_id}: reclaimed from stale worker {dead_worker_id} (reclaim {reclaims}/{MAX_RECLAIMS})")


def main():
    worker_id = str(uuid.uuid4())
    processing_key = f"processing:{worker_id}"

    # socket_timeout must exceed BLMOVE_TIMEOUT_SECONDS -- same client/server
    # timeout race as stage 1's BRPOP (see DECISIONS.md), now against BLMOVE.
    r = redis.Redis.from_url(
        REDIS_URL, decode_responses=True, socket_timeout=BLMOVE_TIMEOUT_SECONDS + 5
    )

    with open(PROMOTE_SCRIPT_PATH) as f:
        promote_retries = r.register_script(f.read())

    # daemon=True: this thread must never be the reason the process outlives
    # a Ctrl+C. It holds no state that needs a clean shutdown -- missing a
    # last heartbeat on exit is indistinguishable from a crash, which is
    # already a case every part of this design has to handle regardless.
    threading.Thread(target=heartbeat_loop, args=(r, worker_id), daemon=True).start()

    print(f"[worker] {worker_id}: waiting for jobs on queue:pending")

    while True:
        # Piggybacked on this loop instead of a separate promoter process --
        # see DECISIONS.md for the cost this carries: a worker stuck running
        # a long job isn't calling this, so a due retry waits until some
        # worker in the pool is free to loop again.
        promote_retries(
            keys=["retry:scheduled", "queue:pending"],
            args=[int(time.time()), PROMOTE_BATCH_SIZE],
        )

        # Same piggyback reasoning as promote_retries: a busy pool delays
        # noticing a dead peer, it doesn't corrupt anything by being late.
        reclaim_dead_workers(r)

        # BLMOVE atomically relocates the id from queue:pending into this
        # worker's own processing list -- the id is never held only in this
        # process's memory with no trace in Redis. That relocation is the
        # claim, durable the instant this call returns. The HSET below is
        # deliberately not part of that atomic step (see DECISIONS.md for
        # why a Lua script doing RPOP+HSET together was rejected): a crash
        # between the two leaves the id correctly sitting in processing_key
        # with possibly-stale hash metadata, but never loses the id itself.
        job_id = r.blmove(
            "queue:pending", processing_key, BLMOVE_TIMEOUT_SECONDS, src="RIGHT", dest="LEFT"
        )
        if job_id is None:
            continue

        r.hset(
            f"job:{job_id}",
            mapping={
                "status": "claimed",
                "worker_id": worker_id,
                "claimed_at": str(int(time.time())),
            },
        )

        run_job(r, job_id, worker_id)

        # Only this worker pushes to and pops from its own processing_key,
        # so this is safe without a Lua script: no other process can be
        # racing us on it, unlike queue:pending which every worker shares.
        r.lrem(processing_key, 1, job_id)


if __name__ == "__main__":
    main()
