"""
Reference checker: try every ordering, keep the ones that respect real time,
and run the model over each. Exponential on purpose, so it is obviously
correct; the fast checker is validated against it on small histories
"""

import itertools

from harness.checker.model import INITIAL, prepare, step

MAX_CALLS = 9


def respects_real_time(order):
    """No call may come after a call that was invoked after it completed"""
    return all(not (later.complete < earlier.invoke) for i, earlier in enumerate(order) for later in order[i + 1:])


def legal(order):
    state = INITIAL
    for call in order:
        ok, state = step(state, call)
        if not ok:
            return False
    return True


def find_linearization(calls):
    if len(calls) > MAX_CALLS:
        raise ValueError(f"brute force is limited to {MAX_CALLS} calls, got {len(calls)}")
    for order in itertools.permutations(calls):
        if respects_real_time(order) and legal(order):
            return list(order)
    return None


def check(ops):
    """Map key -> a linearization (list of calls) or None when none exists"""
    return {key: find_linearization(calls) for key, calls in prepare(ops).items()}
