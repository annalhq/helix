"""
Register-with-CAS model and history preparation for linearizability checking

Each key is an independent register that starts absent (None). A history is
reduced per key to calls with real-time intervals [invoke, complete]:

  ok ops              kept with their observed result
  fail + cas_mismatch kept: the CAS was applied and saw a value != old
  other fail ops      dropped: they definitely had no effect
  info get            dropped: no effect and nothing observed
  info put / cas      kept with complete = inf

An info write that never took effect is indistinguishable from one linearized
after every other op, and applying an info put or CAS is always a valid step.
So checkers may treat every info write as happening, with no separate branch
for "never happened".
"""

import math
from collections import defaultdict
from dataclasses import dataclass

INITIAL = None


@dataclass(frozen=True)
class Call:
    id: int
    client: int
    key: str
    op: str
    value: int | None
    old: int | None
    new: int | None
    status: str
    invoke: int
    complete: float

    def describe(self):
        if self.op == "get":
            what = f"get -> {self.value}"
        elif self.op == "put":
            what = f"put {self.value}"
        else:
            what = f"cas {self.old}->{self.new}"
        suffix = {"ok": "", "mismatch": " (mismatch)", "info": " (unknown outcome)"}[self.status]
        return f"op {self.id} client {self.client} {self.key} {what}{suffix}"


def step(state, call):
    """(valid, next state) of applying call to the register state"""
    if call.op == "get":
        return state == call.value, state
    if call.op == "put":
        return True, call.value
    if call.status == "ok":
        return state == call.old, call.new
    if call.status == "mismatch":
        return state != call.old, state
    return True, call.new if state == call.old else state


def prepare(ops):
    """Map key -> calls sorted by invoke time"""
    by_key = defaultdict(list)
    for o in ops:
        if o.type == "fail" and o.err != "cas_mismatch":
            continue
        if o.type == "info" and o.op == "get":
            continue
        if o.type == "ok":
            status = "ok"
        elif o.type == "fail":
            status = "mismatch"
        else:
            status = "info"
        by_key[o.key].append(Call(
            id=o.id, client=o.client, key=o.key, op=o.op, value=o.value, old=o.old, new=o.new, status=status,
            invoke=o.invoke_ns, complete=math.inf if o.type == "info" else o.complete_ns,
        ))
    return {key: sorted(calls, key=lambda c: (c.invoke, c.id)) for key, calls in sorted(by_key.items())}
