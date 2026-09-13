#!/usr/bin/env python3
"""Reliable-claiming worker: move a job id out of queue:pending into this
worker's own processing list, run it, write the result back onto its hash.
Failures either get rescheduled with backoff or sent to the DLQ.

The processing:<worker_id> list is the durable claim record. The
status/worker_id/claimed_at fields on job:<id> are convenience metadata,
written in a separate step after the claim, and may be stale or missing if
this process dies between the claim and that write -- see DECISIONS.md
("Claiming: BLMOVE into a per-worker list, not a Lua script"). Stage 4
(heartbeats + reclaim) is what makes an abandoned processing:<worker_id>
entry recoverable by another worker; this stage only makes the claim itself
durable and visible.

See DECISIONS.md for the retry/DLQ design: why attempts don't need an
atomic increment yet, why promotion out of retry:scheduled must be atomic,
and the promotion-lag cost of piggybacking it on this loop.
"""
import json
import os
import random
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


def run_job(r, job_id):
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
        attempts = int(job.get("attempts", 0))
        send_to_dlq(r, key, job_id, attempts, f"unknown task: {task_name}")
        print(f"[worker] {job_id}: unknown task {task_name!r}, sent to DLQ")
        return

    attempts = int(job.get("attempts", 0)) + 1

    try:
        result = func(*args)
    except PermanentError as exc:
        send_to_dlq(r, key, job_id, attempts, str(exc))
        print(f"[worker] {job_id}: permanent failure - {exc}")
    except Exception as exc:
        if attempts >= MAX_ATTEMPTS:
            send_to_dlq(r, key, job_id, attempts, str(exc))
            print(f"[worker] {job_id}: attempts exhausted ({attempts}) - {exc}")
        else:
            schedule_retry(r, key, job_id, attempts, str(exc))
            print(f"[worker] {job_id}: attempt {attempts} failed, retry scheduled - {exc}")
    else:
        r.hset(key, mapping={"status": "done", "result": json.dumps(result)})
        print(f"[worker] {job_id}: done - {result!r}")


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

        run_job(r, job_id)

        # Only this worker pushes to and pops from its own processing_key,
        # so this is safe without a Lua script: no other process can be
        # racing us on it, unlike queue:pending which every worker shares.
        r.lrem(processing_key, 1, job_id)


if __name__ == "__main__":
    main()
