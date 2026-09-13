"""
Linearizability checker for the register model

Wing & Gong search over a timeline of call and return events, with Lowe's
memoisation of (set of linearized calls, register state), the approach used by
Porcupine. A call may be linearized next if its call event comes before the
first remaining return event and the model accepts it. Reaching a return whose
call is not yet linearized means the current prefix cannot be extended, so the
search backtracks
"""

import math
from dataclasses import dataclass, field

from harness.checker.model import INITIAL, prepare, step

DEFAULT_MAX_STEPS = 5_000_000


class _Event:
    __slots__ = ("call", "index", "is_call", "time", "match", "prev", "next")

    def __init__(self, call, index, is_call, time):
        self.call = call
        self.index = index
        self.is_call = is_call
        self.time = time
        self.match = None
        self.prev = None
        self.next = None


@dataclass
class KeyResult:
    key: str
    linearizable: bool | None
    calls: list
    order: list | None = None
    deepest: list = field(default_factory=list)
    deepest_state: object = INITIAL
    blocked_by: object = None
    steps: int = 0


def _timeline(calls):
    head = _Event(None, -1, False, -math.inf)
    events = []
    for i, c in enumerate(calls):
        call, ret = _Event(c, i, True, c.invoke), _Event(c, i, False, c.complete)
        call.match = ret
        events += [call, ret]
    # Calls sort before returns at the same instant: touching intervals count as concurrent
    events.sort(key=lambda e: (e.time, not e.is_call, e.index))
    prev = head
    for e in events:
        prev.next, e.prev = e, prev
        prev = e
    return head


def _lift(call):
    call.prev.next = call.next
    call.next.prev = call.prev
    ret = call.match
    ret.prev.next = ret.next
    if ret.next is not None:
        ret.next.prev = ret.prev


def _unlift(call):
    ret = call.match
    ret.prev.next = ret
    if ret.next is not None:
        ret.next.prev = ret
    call.prev.next = call
    call.next.prev = call


def check_key(key, calls, max_steps=DEFAULT_MAX_STEPS):
    result = KeyResult(key=key, linearizable=None, calls=calls)
    head = _timeline(calls)
    state = INITIAL
    linearized = 0
    stack = []
    cache = set()
    deepest = -1
    entry = head.next

    while head.next is not None:
        result.steps += 1
        if result.steps > max_steps:
            return result
        if entry.is_call:
            ok, next_state = step(state, entry.call)
            if ok:
                bits = linearized | (1 << entry.index)
                if (bits, next_state) not in cache:
                    cache.add((bits, next_state))
                    stack.append((entry, state))
                    state, linearized = next_state, bits
                    _lift(entry)
                    entry = head.next
                    continue
            entry = entry.next
            continue

        if len(stack) > deepest:
            deepest = len(stack)
            result.deepest = [e.call for e, _ in stack]
            result.deepest_state = state
            result.blocked_by = entry.call
        if not stack:
            result.linearizable = False
            return result
        call, state = stack.pop()
        linearized &= ~(1 << call.index)
        _unlift(call)
        entry = call.next

    result.linearizable = True
    result.order = [e.call for e, _ in stack]
    return result


def check(ops, max_steps=DEFAULT_MAX_STEPS):
    """Map key -> KeyResult"""
    return {key: check_key(key, calls, max_steps) for key, calls in prepare(ops).items()}
