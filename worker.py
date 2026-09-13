#!/usr/bin/env python3
"""Reliable-claiming worker: move a job id out of queue:pending into this
worker's own processing list, run it, write the result back onto its hash.

The processing:<worker_id> list is the durable claim record. The
status/worker_id/claimed_at fields on job:<id> are convenience metadata,
written in a separate step after the claim, and may be stale or missing if
this process dies between the claim and that write -- see DECISIONS.md
("Claiming: BLMOVE into a per-worker list, not a Lua script"). Stage 4
(heartbeats + reclaim) is what makes an abandoned processing:<worker_id>
entry recoverable by another worker; this stage only makes the claim itself
durable and visible.
"""
import json
import os
import time
import uuid

import redis

from tasks import REGISTRY

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BLMOVE_TIMEOUT_SECONDS = 5


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
        r.hset(key, mapping={"status": "error", "error": f"unknown task: {task_name}"})
        print(f"[worker] {job_id}: unknown task {task_name!r}")
        return

    try:
        result = func(*args)
    except Exception as exc:
        r.hset(key, mapping={"status": "error", "error": str(exc)})
        print(f"[worker] {job_id}: error - {exc}")
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
    print(f"[worker] {worker_id}: waiting for jobs on queue:pending")

    while True:
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
