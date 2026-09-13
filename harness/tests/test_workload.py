import random
from collections import Counter
from itertools import pairwise

import pytest

from harness.client import Indeterminate, NotSent
from harness.history import Clock, read_history
from harness.localcluster import LocalCluster, free_port
from harness.workload import Config, generate, main, parse_nodes, run, run_op


def test_generate_is_seeded_and_follows_the_mix():
    cfg = Config(nodes={}, keys=5, values=5, read_ratio=0.5, write_ratio=0.25)
    rng1, rng2 = random.Random("42:0"), random.Random("42:0")
    ops1 = [generate(rng1, cfg) for _ in range(4000)]
    ops2 = [generate(rng2, cfg) for _ in range(4000)]
    assert ops1 == ops2
    assert ops1 != [generate(random.Random("42:1"), cfg) for _ in range(4000)]

    mix = Counter(op for op, _, _ in ops1)
    assert abs(mix["get"] / 4000 - 0.5) < 0.03
    assert abs(mix["put"] / 4000 - 0.25) < 0.03
    assert abs(mix["cas"] / 4000 - 0.25) < 0.03
    assert {key for _, key, _ in ops1} == {f"k{i}" for i in range(5)}
    values = {v for _, _, args in ops1 for v in args.values()}
    assert values == set(range(5))


class FakeClient:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.calls = response, error, []

    def call(self, op, key, **fields):
        self.calls.append((op, key, fields))
        if self.error:
            raise self.error
        return self.response


@pytest.mark.parametrize(
    "op, args, client, expected",
    [
        ("get", {}, FakeClient({"outcome": "applied", "ok": True, "value": 4}),
         dict(value=4, old=None, new=None, type="ok", err=None)),
        ("put", {"value": 2}, FakeClient({"outcome": "applied", "ok": True, "value": None}),
         dict(value=2, old=None, new=None, type="ok", err=None)),
        ("cas", {"old": 1, "new": 3}, FakeClient({"outcome": "applied", "ok": False, "err": "cas_mismatch"}),
         dict(value=None, old=1, new=3, type="fail", err="cas_mismatch")),
        ("put", {"value": 1}, FakeClient(error=NotSent("refused")),
         dict(value=1, type="fail", err="not_sent")),
        ("cas", {"old": 0, "new": 1}, FakeClient(error=Indeterminate("timeout")),
         dict(old=0, new=1, type="info", err="indeterminate")),
    ],
)
def test_run_op(op, args, client, expected):
    fields = run_op(client, Clock(), op, "k1", args)
    assert client.calls == [(op, "k1", args)]
    assert fields["op"] == op and fields["key"] == "k1"
    assert 0 <= fields["invoke_ns"] <= fields["complete_ns"]
    assert {k: fields[k] for k in expected} == expected


def test_unreachable_nodes_record_definite_failures(tmp_path):
    path = tmp_path / "history.jsonl"
    nodes = {"kv-0": f"127.0.0.1:{free_port()}"}
    summary = run(Config(nodes=nodes, clients=2, duration=0.3, min_delay=0.01, max_delay=0.02, timeout=0.2), path)
    ops = read_history(path)
    assert summary["ops"] == len(ops) > 0
    assert {(o.type, o.err) for o in ops} == {("fail", "not_sent")}


def test_parse_nodes():
    assert parse_nodes("kv-0=a:1, kv-1=b:2") == {"kv-0": "a:1", "kv-1": "b:2"}
    with pytest.raises(Exception):
        parse_nodes("kv-0")


def test_workload_against_local_cluster(kvnode_binary, tmp_path):
    with LocalCluster(kvnode_binary, tmp_path / "cluster") as cluster:
        cluster.wait_leader()
        nodes = {n.id: n.client_addr for n in cluster.nodes}
        path = tmp_path / "history.jsonl"
        assert main([
            "--nodes", ",".join(f"{k}={v}" for k, v in nodes.items()),
            "--out", str(path), "--clients", "6", "--duration", "2",
            "--min-delay", "0.005", "--max-delay", "0.02", "--seed", "7",
        ]) == 0

    ops = read_history(path)
    assert len(ops) > 150
    assert {o.type for o in ops} <= {"ok", "fail"}
    assert {o.err for o in ops if o.type == "fail"} <= {"cas_mismatch"}
    assert {o.client: o.node for o in ops} == {c: f"kv-{c % 3}" for c in range(6)}
    assert any(o.op == "get" and o.type == "ok" and o.value is not None for o in ops)

    for c in range(6):
        mine = sorted((o for o in ops if o.client == c), key=lambda o: o.invoke_ns)
        for prev, nxt in pairwise(mine):
            assert prev.complete_ns <= nxt.invoke_ns, f"client {c} overlapped its own ops"
