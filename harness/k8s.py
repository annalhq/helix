import json
import subprocess

NS = "helix"
KV_PODS = ["kv-0", "kv-1", "kv-2"]


def kubectl(*args, check=True, timeout=60):
    return subprocess.run(
        ["kubectl", "-n", NS, *args], capture_output=True, text=True, check=check, timeout=timeout
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


def restart_count(pod):
    out = kubectl("get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].restartCount}").stdout
    return int(out or 0)
