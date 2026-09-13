from hypothesis import strategies as st

from harness.history import Op


class Builder:
    """Hand-written histories: b.put(1, 0, 10) is client-less shorthand for an op on [0, 10]"""

    def __init__(self, key="k"):
        self.key = key
        self.ops = []

    def _add(self, op, invoke, complete, type="ok", err=None, value=None, old=None, new=None, key=None):
        if type == "info":
            complete = None
        self.ops.append(Op(
            id=len(self.ops), client=len(self.ops), node="n", op=op, key=key or self.key, value=value,
            old=old, new=new, invoke_ns=invoke, complete_ns=complete, type=type, err=err,
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


@st.composite
def linearizable_histories(draw, max_ops=7, values=3, key="k"):
    """
    Histories produced by a real register: every applied op gets a linearization
    point inside its interval (info writes: anywhere after invoke, or never),
    and results come from replaying the points in order
    """
    n = draw(st.integers(1, max_ops))
    specs = []
    for i in range(n):
        op = draw(st.sampled_from(["get", "put", "cas"]))
        invoke = draw(st.integers(0, 40))
        length = draw(st.integers(0, 20))
        info = op != "get" and draw(st.integers(0, 4)) == 0
        applied = not info or draw(st.booleans())
        horizon = 60 if info else length
        point = invoke + draw(st.integers(0, 2 * horizon)) / 2
        specs.append(dict(
            i=i, op=op, invoke=invoke, complete=invoke + length, info=info, applied=applied, point=point,
            value=draw(st.integers(0, values - 1)), old=draw(st.integers(0, values - 1)),
            new=draw(st.integers(0, values - 1)),
        ))

    state = None
    results = {}
    for s in sorted((s for s in specs if s["applied"]), key=lambda s: (s["point"], s["i"])):
        if s["op"] == "get":
            results[s["i"]] = state
        elif s["op"] == "put":
            state = s["value"]
        else:
            results[s["i"]] = state == s["old"]
            if state == s["old"]:
                state = s["new"]

    b = Builder(key)
    for s in specs:
        if s["op"] == "get":
            b.get(results[s["i"]], s["invoke"], s["complete"])
        elif s["op"] == "put":
            if s["info"]:
                b.info_put(s["value"], s["invoke"])
            else:
                b.put(s["value"], s["invoke"], s["complete"])
        elif s["info"]:
            b.info_cas(s["old"], s["new"], s["invoke"])
        elif results[s["i"]]:
            b.cas(s["old"], s["new"], s["invoke"], s["complete"])
        else:
            b.cas_mismatch(s["old"], s["new"], s["invoke"], s["complete"])
    return b.ops
