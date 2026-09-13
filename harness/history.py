"""
History format: one JSON object per line, one line per operation

  id           sequence number assigned when the op completes
  client       workload client (Jepsen "process")
  node         node the client talked to
  op           get | put | cas
  key          register name
  value        put: value written; get: value read (null if absent or not ok)
  old, new     cas arguments
  invoke_ns    monotonic ns since run start, taken before the request is sent
  complete_ns  monotonic ns since run start, taken after the reply; null for info
  type         ok   the op took effect with the recorded result
               fail the op definitely did not take effect, except err=cas_mismatch,
                    where the CAS was applied and observed a different value
               info unknown; the checker must treat it as possibly taking effect
                    at any point after invoke
  err          null for ok, otherwise a reason code
"""

import json
import threading
import time
from dataclasses import asdict, dataclass

from harness.client import Indeterminate, NotSent

OPS = ("get", "put", "cas")
TYPES = ("ok", "fail", "info")


@dataclass(frozen=True)
class Op:
    id: int
    client: int
    node: str
    op: str
    key: str
    value: int | None
    old: int | None
    new: int | None
    invoke_ns: int
    complete_ns: int | None
    type: str
    err: str | None


class Clock:
    def __init__(self):
        self._start = time.monotonic_ns()

    def now(self):
        return time.monotonic_ns() - self._start


def classify(op, response=None, error=None):
    """Map a client outcome to (type, value read, err)"""
    if isinstance(error, NotSent):
        return "fail", None, "not_sent"
    if isinstance(error, Indeterminate):
        return "info", None, "indeterminate"
    if error is not None:
        return "info", None, type(error).__name__

    outcome = response.get("outcome") if isinstance(response, dict) else None
    if outcome == "applied":
        if op == "get":
            return "ok", response.get("value"), None
        if op == "cas" and not response.get("ok"):
            return "fail", None, "cas_mismatch"
        return "ok", None, None
    if outcome == "definite":
        return "fail", None, response.get("err") or "definite"
    if outcome == "unknown":
        return "info", None, response.get("err") or "unknown"
    return "info", None, "unexpected_reply"


class HistoryWriter:
    def __init__(self, path):
        self._file = open(path, "w")
        self._lock = threading.Lock()
        self._next_id = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def record(self, *, client, node, op, key, invoke_ns, complete_ns, type, value=None, old=None, new=None, err=None):
        with self._lock:
            entry = Op(
                id=self._next_id, client=client, node=node, op=op, key=key, value=value, old=old, new=new,
                invoke_ns=invoke_ns, complete_ns=complete_ns if type != "info" else None, type=type, err=err,
            )
            validate(entry)
            self._file.write(json.dumps(asdict(entry)) + "\n")
            self._file.flush()
            self._next_id += 1
            return entry

    def close(self):
        with self._lock:
            self._file.close()


def validate(o):
    if o.op not in OPS:
        raise ValueError(f"op {o.id}: unknown op {o.op!r}")
    if o.type not in TYPES:
        raise ValueError(f"op {o.id}: unknown type {o.type!r}")
    if o.type == "info":
        if o.complete_ns is not None:
            raise ValueError(f"op {o.id}: info op must not have complete_ns")
    elif o.complete_ns is None or o.complete_ns < o.invoke_ns:
        raise ValueError(f"op {o.id}: {o.type} op needs complete_ns >= invoke_ns")
    if o.type == "ok" and o.err is not None or o.type != "ok" and o.err is None:
        raise ValueError(f"op {o.id}: err must be set exactly when type is not ok")
    if o.op == "put" and o.value is None:
        raise ValueError(f"op {o.id}: put without value")
    if o.op == "cas" and (o.old is None or o.new is None):
        raise ValueError(f"op {o.id}: cas without old/new")
    if o.op == "get" and o.type != "ok" and o.value is not None:
        raise ValueError(f"op {o.id}: only an ok get may carry a read value")


def read_history(path):
    ops = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                o = Op(**json.loads(line))
                validate(o)
            except (TypeError, ValueError) as e:
                raise ValueError(f"{path}:{lineno}: {e}") from e
            ops.append(o)
    return ops
