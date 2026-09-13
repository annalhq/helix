import math

import pytest
from hypothesis import given, settings

from harness.checker import bruteforce
from harness.checker.model import prepare, step
from harness.tests.histories import Builder, linearizable_histories


def linearizable(builder):
    verdicts = bruteforce.check(builder.ops)
    return all(order is not None for order in verdicts.values())


def test_prepare_filters_and_opens_unknown_writes():
    b = (Builder()
         .put(1, 0, 10)
         .failed_put(2, 5, 15)
         .cas_mismatch(3, 4, 20, 30)
         .info_put(5, 40)
         .get(1, 50, 60)
         ._add("get", 70, None, type="info", err="timeout")
         .put(9, 0, 5, key="other"))
    prepared = prepare(b.ops)

    assert list(prepared) == ["k", "other"]
    calls = prepared["k"]
    assert [c.id for c in calls] == [0, 2, 3, 4]
    assert [c.status for c in calls] == ["ok", "mismatch", "info", "ok"]
    assert calls[2].complete == math.inf
    assert [c.id for c in prepared["other"]] == [6]


@pytest.mark.parametrize(
    "state, builder, expected",
    [
        (None, Builder().get(None, 0, 1), (True, None)),
        (1, Builder().get(2, 0, 1), (False, 1)),
        (1, Builder().put(2, 0, 1), (True, 2)),
        (1, Builder().cas(1, 3, 0, 1), (True, 3)),
        (2, Builder().cas(1, 3, 0, 1), (False, 3)),
        (2, Builder().cas_mismatch(1, 3, 0, 1), (True, 2)),
        (1, Builder().cas_mismatch(1, 3, 0, 1), (False, 1)),
        (None, Builder().cas_mismatch(0, 3, 0, 1), (True, None)),
        (1, Builder().info_cas(1, 3, 0), (True, 3)),
        (2, Builder().info_cas(1, 3, 0), (True, 2)),
    ],
)
def test_step(state, builder, expected):
    [call] = prepare(builder.ops)["k"]
    valid, next_state = step(state, call)
    assert valid == expected[0]
    if valid:
        assert next_state == expected[1]


CASES = [
    ("empty register reads absent", Builder().get(None, 0, 10), True),
    ("read of a value never written", Builder().get(1, 0, 10), False),
    ("sequential put then read", Builder().put(1, 0, 10).get(1, 20, 30), True),
    ("stale read after a completed overwrite", Builder().put(1, 0, 10).put(2, 20, 30).get(1, 40, 50), False),
    ("read concurrent with a write may see either value",
     Builder().put(1, 0, 10).put(2, 20, 100).get(1, 30, 40).get(2, 50, 60), True),
    ("once the new value is seen the old one cannot return",
     Builder().put(1, 0, 10).put(2, 20, 100).get(2, 30, 40).get(1, 50, 60), False),
    ("overlapping writes linearizable only as put2 then put1",
     Builder().put(1, 0, 50).put(2, 10, 60).get(1, 70, 80), True),
    ("final read contradicts the only valid order",
     Builder().put(1, 0, 50).put(2, 10, 60).get(1, 70, 80).get(2, 90, 95), False),
    ("cas succeeding from the wrong old value", Builder().put(1, 0, 10).cas(2, 3, 20, 30), False),
    ("cas from the right old value", Builder().put(1, 0, 10).cas(1, 3, 20, 30).get(3, 40, 50), True),
    ("consistent cas mismatch", Builder().put(1, 0, 10).cas_mismatch(2, 3, 20, 30).get(1, 40, 50), True),
    ("cas mismatch although the value matched", Builder().put(2, 0, 10).cas_mismatch(2, 3, 20, 30), False),
    ("cas on an absent key never matches", Builder().cas_mismatch(0, 1, 0, 10).get(None, 20, 30), True),
    ("cas on an absent key cannot succeed", Builder().cas(0, 1, 0, 10), False),
    ("unknown write that took effect", Builder().put(1, 0, 10).info_put(2, 20).get(2, 30, 40), True),
    ("unknown write that never took effect", Builder().put(1, 0, 10).info_put(2, 20).get(1, 30, 40), True),
    ("unknown write cannot take effect before it was invoked",
     Builder().get(2, 0, 10).info_put(2, 20), False),
    ("unknown cas that took effect", Builder().put(1, 0, 10).info_cas(1, 2, 20).get(2, 30, 40), True),
    ("unknown cas that did not take effect", Builder().put(1, 0, 10).info_cas(1, 2, 20).get(1, 30, 40), True),
    ("definitely failed write is ignored", Builder().put(1, 0, 10).failed_put(2, 20, 30).get(1, 40, 50), True),
    ("definitely failed write cannot be observed", Builder().put(1, 0, 10).failed_put(2, 20, 30).get(2, 40, 50), False),
    ("keys are independent registers", Builder().put(1, 0, 10).put(2, 20, 30, key="j").get(1, 40, 50), True),
]


@pytest.mark.parametrize("name, builder, expected", CASES, ids=[c[0] for c in CASES])
def test_bruteforce_hand_written(name, builder, expected):
    assert linearizable(builder) is expected


def test_bruteforce_returns_a_valid_witness():
    b = Builder().put(1, 0, 50).put(2, 10, 60).get(1, 70, 80)
    [order] = bruteforce.check(b.ops).values()
    assert [c.id for c in order] == [1, 0, 2]


def test_bruteforce_refuses_large_histories():
    b = Builder()
    for i in range(bruteforce.MAX_CALLS + 1):
        b.put(i, i, i)
    with pytest.raises(ValueError):
        bruteforce.check(b.ops)


@settings(max_examples=300, deadline=None)
@given(linearizable_histories())
def test_bruteforce_accepts_histories_from_a_real_register(ops):
    assert all(order is not None for order in bruteforce.check(ops).values())
