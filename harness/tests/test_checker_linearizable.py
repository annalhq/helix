import dataclasses
import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from harness.checker import bruteforce, linearizable
from harness.tests.histories import Builder, linearizable_histories, simulated_history
from harness.tests.test_checker_model import CASES


def assert_witness(result):
    order = result.order
    assert sorted(c.id for c in order) == sorted(c.id for c in result.calls)
    assert bruteforce.respects_real_time(order)
    assert bruteforce.legal(order)


@pytest.mark.parametrize("name, builder, expected", CASES, ids=[c[0] for c in CASES])
def test_hand_written(name, builder, expected):
    results = linearizable.check(builder.ops)
    assert all(r.linearizable is not None for r in results.values())
    assert all(r.linearizable for r in results.values()) is expected
    for r in results.values():
        if r.linearizable:
            assert_witness(r)


def test_ops_touching_at_an_instant_are_concurrent():
    b = Builder().put(1, 0, 10).get(None, 10, 20)
    [fast] = linearizable.check(b.ops).values()
    [slow] = bruteforce.check(b.ops).values()
    assert fast.linearizable is True and slow is not None


def test_empty_history():
    assert linearizable.check([]) == {}
    result = linearizable.check_key("k", [])
    assert result.linearizable is True and result.order == []


def test_violation_reports_deepest_prefix_and_blocking_op():
    b = Builder().put(1, 0, 10).put(2, 20, 30).get(1, 40, 50)
    [result] = linearizable.check(b.ops).values()
    assert result.linearizable is False
    assert [c.id for c in result.deepest] == [0, 1]
    assert result.deepest_state == 2
    assert result.blocked_by.id == 2


def test_step_limit_reports_unknown():
    ops = simulated_history(clients=4, ops_per_client=40, keys=1, seed=3)
    [result] = linearizable.check(ops, max_steps=5).values()
    assert result.linearizable is None


@st.composite
def mutated_histories(draw):
    ops = list(draw(linearizable_histories(max_ops=7)))
    for _ in range(draw(st.integers(0, 3))):
        i = draw(st.integers(0, len(ops) - 1))
        o = ops[i]
        kind = draw(st.sampled_from(["value", "status", "shift"]))
        if kind == "value" and o.op == "get" and o.type == "ok":
            o = dataclasses.replace(o, value=draw(st.one_of(st.none(), st.integers(0, 3))))
        elif kind == "value" and o.op == "put":
            o = dataclasses.replace(o, value=draw(st.integers(0, 3)))
        elif kind == "status" and o.op == "cas" and o.type != "info":
            flipped = dict(type="fail", err="cas_mismatch") if o.type == "ok" else dict(type="ok", err=None)
            o = dataclasses.replace(o, **flipped)
        elif kind == "shift" and o.type != "info":
            invoke = max(0, o.invoke_ns + draw(st.integers(-30, 30)))
            o = dataclasses.replace(o, invoke_ns=invoke, complete_ns=invoke + o.complete_ns - o.invoke_ns)
        ops[i] = o
    return ops


@settings(max_examples=500, deadline=None)
@given(linearizable_histories())
def test_accepts_histories_from_a_real_register(ops):
    for result in linearizable.check(ops).values():
        assert result.linearizable is True
        assert_witness(result)


@settings(max_examples=3000, deadline=None)
@given(mutated_histories())
def test_agrees_with_bruteforce(ops):
    fast = linearizable.check(ops)
    slow = bruteforce.check(ops)
    assert fast.keys() == slow.keys()
    for key, order in slow.items():
        assert fast[key].linearizable == (order is not None)
        if fast[key].linearizable:
            assert_witness(fast[key])


def test_large_workload_shaped_history_is_accepted_quickly():
    ops = simulated_history(clients=6, ops_per_client=300, keys=5, info_rate=0.02, seed=1)
    start = time.perf_counter()
    results = linearizable.check(ops)
    elapsed = time.perf_counter() - start
    assert sum(len(r.calls) for r in results.values()) > 1700
    assert all(r.linearizable for r in results.values())
    assert elapsed < 10, f"checking took {elapsed:.1f}s"


def test_large_history_with_one_corrupted_read_is_rejected():
    ops = simulated_history(clients=6, ops_per_client=300, keys=5, seed=2)
    reads = [o for o in ops if o.op == "get" and o.type == "ok" and o.key == "k0"]
    target = reads[len(reads) // 2]
    ops = [dataclasses.replace(o, value=99) if o is target else o for o in ops]

    start = time.perf_counter()
    results = linearizable.check(ops)
    elapsed = time.perf_counter() - start
    assert results["k0"].linearizable is False
    assert all(r.linearizable for key, r in results.items() if key != "k0")
    blocked = results["k0"].blocked_by
    assert blocked.invoke <= target.complete_ns and target.invoke_ns <= blocked.complete
    assert elapsed < 10, f"checking took {elapsed:.1f}s"
