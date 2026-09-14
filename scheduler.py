#!/usr/bin/env python3
"""CLI for the task scheduler. Thin wrapper over the REST gateway -- never
touches Redis directly, so there is exactly one path into job state (see
CLAUDE.md: "two paths into job state will drift").

Usage:
    python3 scheduler.py submit <task> [arg ...]
    python3 scheduler.py status <job-id>
    python3 scheduler.py stats
    python3 scheduler.py watch
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:3000")


def _request(method, path, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        f"{GATEWAY_URL}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read()), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read()), dict(e.headers)
    except urllib.error.URLError as e:
        print(f"error: can't reach gateway at {GATEWAY_URL} ({e.reason})", file=sys.stderr)
        sys.exit(1)


def _parse_arg(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def cmd_submit(args):
    job_args = [_parse_arg(a) for a in args.args]
    status, body, headers = _request("POST", "/jobs", {"task": args.task, "args": job_args})

    if status == 201:
        print(body["job_id"])
        return

    if status == 429:
        retry_after = headers.get("Retry-After", "?")
        print(f"error: rate limited, retry after {retry_after}s", file=sys.stderr)
    else:
        print(f"error: {body.get('error', status)}", file=sys.stderr)
    sys.exit(1)


def cmd_status(args):
    status, body, _headers = _request("GET", f"/jobs/{args.job_id}")

    if status == 404:
        print(f"error: no job {args.job_id}", file=sys.stderr)
        sys.exit(1)
    if status != 200:
        print(f"error: {body.get('error', status)}", file=sys.stderr)
        sys.exit(1)

    print(f"job_id:     {body['job_id']}")
    print(f"task:       {body.get('task', '?')}")
    print(f"status:     {body.get('status', '?')}")
    print(f"attempts:   {body.get('attempts', 0)}")
    print(f"last_error: {body.get('last_error', '(none)')}")
    if "result" in body:
        print(f"result:     {body['result']!r}")


def _print_stats(body):
    print(f"pending:       {body['pending']}")
    print(f"workers_alive: {body['workers_alive']}")
    print(f"dlq:           {body['dlq']}")


def cmd_stats(args):
    status, body, _headers = _request("GET", "/stats")
    if status != 200:
        print(f"error: {body.get('error', status)}", file=sys.stderr)
        sys.exit(1)
    _print_stats(body)


def cmd_watch(args):
    # Plain sleep loop, no curses -- clear via an ANSI reset, not a
    # subprocess call to `clear`, so this stays standard-library only.
    try:
        while True:
            status, body, _headers = _request("GET", "/stats")
            print("\033c", end="")
            if status == 200:
                _print_stats(body)
            else:
                print(f"error: {body.get('error', status)}", file=sys.stderr)
            print("\n(refreshing every 1s, Ctrl+C to exit)")
            time.sleep(1)
    except KeyboardInterrupt:
        pass


def main():
    parser = argparse.ArgumentParser(prog="scheduler")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_submit = subparsers.add_parser("submit", help="enqueue a job")
    p_submit.add_argument("task")
    p_submit.add_argument("args", nargs="*")
    p_submit.set_defaults(func=cmd_submit)

    p_status = subparsers.add_parser("status", help="show a job's state")
    p_status.add_argument("job_id")
    p_status.set_defaults(func=cmd_status)

    p_stats = subparsers.add_parser("stats", help="show queue/worker/DLQ counts")
    p_stats.set_defaults(func=cmd_stats)

    p_watch = subparsers.add_parser("watch", help="stats on a 1s refresh loop")
    p_watch.set_defaults(func=cmd_watch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
