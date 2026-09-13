import json
import subprocess
import time

NS = "helix"
KV_PODS = ["kv-0", "kv-1", "kv-2"]


def kubectl(*args, check=True, timeout=60, capture=True):
    return subprocess.run(
        ["kubectl", "-n", NS, *args], capture_output=capture, text=True, check=check, timeout=timeout
    )


def kv_addr(pod):
    return f"{pod}.kv.{NS}.svc.cluster.local:8000"


def client(pod, *args, timeout=2.0):
    """Run the JSON client CLI inside the control pod; returns (exit code, reply or None, stderr)"""
    p = kubectl(
        "exec", "control", "--",
        "python", "-m", "harness.client", "--addr", kv_addr(pod), "--timeout", str(timeout), *args,
        check=False,
    )
    reply = json.loads(p.stdout) if p.stdout.strip() else None
    return p.returncode, reply, p.stderr.strip()


def status(pod):
    code, reply, _ = client(pod, "status", timeout=1.0)
    return reply["status"] if code == 0 and reply else None


def leader(after_term=0):
    """(pod, term) of the leader with the highest term, if no live node has already moved past it"""
    live = {p: s for p in KV_PODS if (s := status(p))}
    leaders = [p for p, s in live.items() if s["role"] == "leader" and s["term"] > after_term]
    if not leaders:
        return None
    best = max(leaders, key=lambda p: live[p]["term"])
    if any(s["term"] > live[best]["term"] for s in live.values()):
        return None
    return best, live[best]["term"]


def wait_until(predicate, what, timeout=30.0, interval=0.5):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for {what}")
        time.sleep(interval)


def restart_count(pod):
    out = kubectl("get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].restartCount}").stdout
    return int(out or 0)
