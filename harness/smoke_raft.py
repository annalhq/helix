import sys
import time

from harness import k8s


def step(msg):
    print(f"==> {msg}", flush=True)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    for pod in k8s.KV_PODS:
        logs = k8s.kubectl("logs", pod, "--tail", "20", check=False).stdout
        print(f"--- {pod} (last 20 lines)\n{logs}", file=sys.stderr)
    sys.exit(1)


def wait_until(predicate, what, timeout=30.0):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            fail(f"timed out after {timeout}s waiting for {what}")
        time.sleep(0.5)


def status(pod):
    code, reply, _ = k8s.client(pod, "status", timeout=1.0)
    return reply["status"] if code == 0 and reply else None


def leader(after_term=0):
    live = {p: s for p in k8s.KV_PODS if (s := status(p))}
    leaders = [p for p, s in live.items() if s["role"] == "leader" and s["term"] > after_term]
    if not leaders:
        return None
    best = max(leaders, key=lambda p: live[p]["term"])
    if any(s["term"] > live[best]["term"] for s in live.values()):
        return None
    return best, live[best]["term"]


def expect_applied(pod, *args):
    code, reply, err = k8s.client(pod, *args)
    if code != 0:
        fail(f"{pod}: {' '.join(args)} -> exit {code}, reply {reply}, stderr {err!r}")
    return reply


def local_value(pod, key):
    code, reply, _ = k8s.client(pod, "get", key, timeout=1.0)
    return reply.get("value") if code == 0 and reply else None


def main():
    step("waiting for a leader")
    old, old_term = wait_until(leader, "an initial leader")
    follower = next(p for p in k8s.KV_PODS if p != old)
    print(f"    leader {old} (term {old_term}), writing through follower {follower}")

    expect_applied(follower, "put", "smoke", "1")
    if expect_applied(old, "get", "smoke")["value"] != 1:
        fail("leader does not see the write forwarded by a follower")

    restarts = k8s.restart_count(old)
    step(f"SIGKILL kvnode on leader {old}")
    k8s.kubectl("exec", old, "--", "pkill", "-9", "kvnode")

    new, new_term = wait_until(lambda: leader(after_term=old_term), "re-election")
    print(f"    new leader {new} (term {new_term})")
    writer = next(p for p in k8s.KV_PODS if p not in (old, new))
    expect_applied(writer, "put", "smoke", "2")
    if not expect_applied(writer, "cas", "smoke", "2", "3")["ok"]:
        fail("cas 2 -> 3 through a follower did not apply")

    step(f"waiting for {old} to restart under the supervisor and catch up")
    wait_until(lambda: local_value(old, "smoke") == 3, f"{old} to apply smoke=3")
    st = status(old)
    if st["role"] != "follower" or st["term"] < new_term:
        fail(f"restarted node status {st}")
    if k8s.restart_count(old) != restarts:
        fail("container restarted; the supervisor loop should have restarted kvnode in place")

    print(f"PASS: re-election {old}@{old_term} -> {new}@{new_term}, forwarding, and catch-up after SIGKILL")


if __name__ == "__main__":
    main()
