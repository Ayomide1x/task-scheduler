"""Task functions the worker knows how to execute.

A task is just a plain function. The worker looks it up by name and calls
it with the args stored in the job. Keep these deterministic and cheap.

Raise PermanentError instead of a plain exception when the failure is one
no amount of retrying will fix (bad input, a value that will never
validate). Anything else raised is treated as possibly transient and goes
through the normal backoff-and-retry path up to the attempt limit.
"""
import time


class PermanentError(Exception):
    """Raised by a task to skip retries and go straight to the DLQ."""


def add(a, b):
    return a + b


def echo(message):
    return message

def slow(seconds):
    time.sleep(seconds)
    return "finished"


REGISTRY = {
    "add": add,
    "echo": echo,
    "slow": slow,
}
