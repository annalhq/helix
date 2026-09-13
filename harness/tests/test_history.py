import json
import threading

import pytest

from harness.client import Indeterminate, NotSent
from harness.history import Clock, HistoryWriter, classify, read_history


@pytest.mark.parametrize(
    "op, response, error, expected",
    [
        ("get", {"outcome": "applied", "ok": True, "value": 3}, None, ("ok", 3, None)),
        ("get", {"outcome": "applied", "ok": True, "value": None}, None, ("ok", None, None)),
        ("put", {"outcome": "applied", "ok": True, "value": None}, None, ("ok", None, None)),
        ("cas", {"outcome": "applied", "ok": True}, None, ("ok", None, None)),
        ("cas", {"outcome": "applied", "ok": False, "err": "cas_mismatch"}, None, ("fail", None, "cas_mismatch")),
        ("put", {"outcome": "definite", "ok": False, "err": "unavailable"}, None, ("fail", None, "unavailable")),
        ("cas", {"outcome": "definite", "ok": False, "err": "not_leader"}, None, ("fail", None, "not_leader")),
        ("put", {"outcome": "unknown", "ok": False, "err": "timeout"}, None, ("info", None, "timeout")),
        ("get", {"outcome": "unknown", "ok": False, "err": "leadership_lost"}, None, ("info", None, "leadership_lost")),
        ("put", {"ok": True}, None, ("info", None, "unexpected_reply")),
        ("put", None, NotSent("refused"), ("fail", None, "not_sent")),
        ("cas", None, Indeterminate("timed out"), ("info", None, "indeterminate")),
        ("put", None, RuntimeError("boom"), ("info", None, "RuntimeError")),
    ],
)
def test_classify(op, response, error, expected):
    assert classify(op, response, error) == expected


def base(**overrides):
    fields = dict(client=0, node="kv-0", op="put", key="k0", value=1, invoke_ns=10, complete_ns=20, type="ok")
    fields.update(overrides)
    return fields


def test_round_trip(tmp_path):
    path = tmp_path / "history.jsonl"
    with HistoryWriter(path) as w:
        w.record(**base())
        w.record(**base(op="get", value=1, client=1, node="kv-1"))
        w.record(**base(op="cas", value=None, old=1, new=2, type="fail", err="cas_mismatch"))
        w.record(**base(op="put", value=4, type="info", err="timeout", complete_ns=99))

    ops = read_history(path)
    assert [o.id for o in ops] == [0, 1, 2, 3]
    assert (ops[1].op, ops[1].value, ops[1].node) == ("get", 1, "kv-1")
    assert (ops[2].type, ops[2].err, ops[2].old, ops[2].new) == ("fail", "cas_mismatch", 1, 2)
    assert (ops[3].type, ops[3].complete_ns) == ("info", None)


def test_concurrent_writers_get_unique_sequential_ids(tmp_path):
    path = tmp_path / "history.jsonl"
    with HistoryWriter(path) as w:
        def worker(client):
            for i in range(200):
                w.record(**base(client=client, value=i))

        threads = [threading.Thread(target=worker, args=(c,)) for c in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    ops = read_history(path)
    assert sorted(o.id for o in ops) == list(range(1600))
    for c in range(8):
        assert [o.value for o in ops if o.client == c] == list(range(200))


@pytest.mark.parametrize(
    "overrides, message",
    [
        (dict(op="del"), "unknown op"),
        (dict(type="maybe", err="x"), "unknown type"),
        (dict(complete_ns=None), "complete_ns >= invoke_ns"),
        (dict(complete_ns=5), "complete_ns >= invoke_ns"),
        (dict(err="timeout"), "err must be set"),
        (dict(type="fail"), "err must be set"),
        (dict(value=None), "put without value"),
        (dict(op="cas", value=None, old=1), "cas without old/new"),
        (dict(op="get", value=2, type="info", err="timeout"), "only an ok get"),
    ],
)
def test_writer_rejects_invalid_records(tmp_path, overrides, message):
    with HistoryWriter(tmp_path / "history.jsonl") as w:
        with pytest.raises(ValueError, match=message):
            w.record(**base(**overrides))


def test_reader_reports_line_of_invalid_record(tmp_path):
    path = tmp_path / "history.jsonl"
    with HistoryWriter(path) as w:
        w.record(**base())
    with open(path, "a") as f:
        bad = dict(read_history(path)[0].__dict__, id=1, type="info", err="timeout", complete_ns=30)
        f.write(json.dumps(bad) + "\n")
    with pytest.raises(ValueError, match=r"history.jsonl:2: .*info op must not have complete_ns"):
        read_history(path)


def test_clock_is_relative_and_monotonic():
    clock = Clock()
    readings = [clock.now() for _ in range(1000)]
    assert readings[0] >= 0
    assert readings == sorted(readings)
