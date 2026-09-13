#!/usr/bin/env python3
"""Vertical-slice worker: pop one job id at a time, run it, write the result
back onto its hash.

Known gap, left open on purpose: if this process dies between BRPOP
returning a job id and the final HSET, that job is lost with no record of
it ever having been claimed. Stage 2 (reliable claiming) closes this.
Stage 1 is only proving the enqueue -> execute -> write-back path works.
"""
import json
import os

import redis

from tasks import REGISTRY

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BRPOP_TIMEOUT_SECONDS = 5


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
    # socket_timeout must exceed BRPOP_TIMEOUT_SECONDS. BRPOP's timeout is a
    # server-side instruction to Redis; socket_timeout is the client's own
    # limit on how long it will wait to read a response. redis-py defaults
    # socket_timeout to 5s, same as our BRPOP window, so the client was
    # racing the server and losing -- giving up and raising before the
    # server's "nothing arrived" reply could get back, indistinguishable
    # from a real connection failure. Padding it past BRPOP's window is
    # what makes an empty-queue timeout a normal `None` return instead.
    r = redis.Redis.from_url(
        REDIS_URL, decode_responses=True, socket_timeout=BRPOP_TIMEOUT_SECONDS + 5
    )
    print("[worker] waiting for jobs on queue:pending")
    while True:
        # BRPOP's atomicity only guarantees a given id is handed to exactly
        # one caller -- it is not claiming. No ownership or claim time is
        # recorded anywhere, so once this pop returns, Redis has no memory
        # that this worker is the one holding the job (see module docstring).
        #
        # Bounded timeout rather than an indefinite block -- see DECISIONS.md.
        # A timeout means `popped` comes back None; this is also where a
        # stage-4 heartbeat write will go, since it fires on a fixed cadence
        # regardless of whether a job was there.
        popped = r.brpop("queue:pending", timeout=BRPOP_TIMEOUT_SECONDS)
        if popped is None:
            continue
        _, job_id = popped
        run_job(r, job_id)


if __name__ == "__main__":
    main()
