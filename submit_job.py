#!/usr/bin/env python3
"""Vertical-slice job submitter. No API, no CLI framework yet -- talks to
Redis directly. That direct-to-Redis path goes away once the gateway exists
in stage 5; only the gateway and workers touch Redis after that.

Usage:
    python3 submit_job.py <task> [arg ...]

Each arg is JSON-decoded individually, so `submit_job.py add 2 3` sends the
integers 2 and 3, not the strings "2" and "3". Anything that isn't valid
JSON (like a bare word) is kept as a plain string.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def parse_arg(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def main():
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <task> [arg ...]", file=sys.stderr)
        sys.exit(1)

    task = sys.argv[1]
    args = [parse_arg(a) for a in sys.argv[2:]]

    job_id = str(uuid.uuid4())
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    # Write the job hash before pushing the id onto the queue. A worker can
    # only ever see the id after BRPOP pops it, and by then this HSET has
    # already happened -- no race between "job exists" and "job is queued".
    r.hset(
        f"job:{job_id}",
        mapping={
            "task": task,
            "args": json.dumps(args),
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    r.lpush("queue:pending", job_id)

    print(job_id)


if __name__ == "__main__":
    main()
