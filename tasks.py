"""Task functions the worker knows how to execute.

A task is just a plain function. The worker looks it up by name and calls
it with the args stored in the job. Keep these deterministic and cheap for
now -- retry semantics come in stage 3.
"""
import time

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
