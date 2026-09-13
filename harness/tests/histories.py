import random

from hypothesis import strategies as st

from harness.history import Op


class Builder:
    """Hand-written histories: b.put(1, 0, 10) adds a put of 1 on [0, 10], one client per op unless given"""

    def __init__(self, key="k"):
        self.key = key
        self.ops = []

    def _add(self, op, invoke, complete, type="ok", err=None, value=None, old=None, new=None, key=None, client=None):
        if type == "info":
            complete = None
        self.ops.append(Op(
            id=len(self.ops), client=len(self.ops) if client is None else client, node="n", op=op,
            key=key or self.key, value=value, old=old, new=new, invoke_ns=invoke, complete_ns=complete,
            type=type, err=err,
        ))
        return self

    def get(self, value, invoke, complete, **kw):
        return self._add("get", invoke, complete, value=value, **kw)

    def put(self, value, invoke, complete, **kw):
        return self._add("put", invoke, complete, value=value, **kw)

    def cas(self, old, new, invoke, complete, **kw):
        return self._add("cas", invoke, complete, old=old, new=new, **kw)

    def cas_mismatch(self, old, new, invoke, complete, **kw):
        return self._add("cas", invoke, complete, old=old, new=new, type="fail", err="cas_mismatch", **kw)

    def info_put(self, value, invoke, **kw):
        return self._add("put", invoke, None, value=value, type="info", err="timeout", **kw)

    def info_cas(self, old, new, invoke, **kw):
        return self._add("cas", invoke, None, old=old, new=new, type="info", err="timeout", **kw)

    def failed_put(self, value, invoke, complete, **kw):
        return self._add("put", invoke, complete, value=value, type="fail", err="unavailable", **kw)


def replay(specs):
    """
    Build the history a real register would produce: applied ops take effect in
    order of their linearization point and results are read off that replay.
    Each spec: client, key, op, invoke, complete, info, applied, point, value, old, new
    """
    state = {}
    results = {}
    for i, s in sorted(((i, s) for i, s in enumerate(specs) if s["applied"]), key=lambda p: (p[1]["point"], p[0])):
        current = state.get(s["key"])
        if s["op"] == "get":
            results[i] = current
        elif s["op"] == "put":
            state[s["key"]] = s["value"]
        else:
            results[i] = current == s["old"]
            if current == s["old"]:
                state[s["key"]] = s["new"]

    b = Builder()
    for i, s in enumerate(specs):
        kw = dict(key=s["key"], client=s["client"])
        if s["op"] == "get":
            b.get(results[i], s["invoke"], s["complete"], **kw)
        elif s["op"] == "put":
            if s["info"]:
                b.info_put(s["value"], s["invoke"], **kw)
            else:
                b.put(s["value"], s["invoke"], s["complete"], **kw)
        elif s["info"]:
            b.info_cas(s["old"], s["new"], s["invoke"], **kw)
        elif results[i]:
            b.cas(s["old"], s["new"], s["invoke"], s["complete"], **kw)
        else:
            b.cas_mismatch(s["old"], s["new"], s["invoke"], s["complete"], **kw)
    return b.ops


@st.composite
def linearizable_histories(draw, max_ops=7, values=3, key="k"):
    """Small single-key histories from a real register; info writes land anywhere after invoke, or never"""
    specs = []
    for i in range(draw(st.integers(1, max_ops))):
        op = draw(st.sampled_from(["get", "put", "cas"]))
        invoke = draw(st.integers(0, 40))
        length = draw(st.integers(0, 20))
        info = op != "get" and draw(st.integers(0, 4)) == 0
        horizon = 60 if info else length
        specs.append(dict(
            client=i, key=key, op=op, invoke=invoke, complete=invoke + length, info=info,
            applied=not info or draw(st.booleans()), point=invoke + draw(st.integers(0, 2 * horizon)) / 2,
            value=draw(st.integers(0, values - 1)), old=draw(st.integers(0, values - 1)),
            new=draw(st.integers(0, values - 1)),
        ))
    return replay(specs)


def simulated_history(clients=6, ops_per_client=100, keys=5, values=5, info_rate=0.0, seed=0):
    """Workload-shaped history: sequential clients, overlapping across clients, many keys"""
    rng = random.Random(seed)
    specs = []
    for c in range(clients):
        t = rng.randint(0, 20)
        for _ in range(ops_per_client):
            op = rng.choices(["get", "put", "cas"], weights=[2, 1, 1])[0]
            complete = t + rng.randint(1, 40)
            info = op != "get" and rng.random() < info_rate
            specs.append(dict(
                client=c, key=f"k{rng.randrange(keys)}", op=op, invoke=t, complete=complete, info=info,
                applied=not info or rng.random() < 0.5,
                point=rng.uniform(t, complete + 400 if info else complete),
                value=rng.randrange(values), old=rng.randrange(values), new=rng.randrange(values),
            ))
            t = complete + rng.randint(0, 20)
    return replay(specs)
