import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from harness import k8s
from harness.history import read_history
from harness.workload import Config, format_summary, summarize

ROOT = Path(__file__).resolve().parents[1]
REMOTE_HISTORY = "/tmp/history.jsonl"


def step(msg):
    print(f"==> {msg}", flush=True)


def prepare_out_dir(path, force):
    path = Path(path)
    if path.exists() and any(path.iterdir()) and not force:
        raise FileExistsError(f"{path} already has results; pass --force to overwrite")
    path.mkdir(parents=True, exist_ok=True)
    return path


def git_describe(cwd=ROOT):
    def git(*args):
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=False)

    head = git("rev-parse", "--short", "HEAD")
    if head.returncode != 0:
        return "unknown"
    dirty = git("status", "--porcelain", "--untracked-files=no").stdout.strip()
    return head.stdout.strip() + ("-dirty" if dirty else "")


def sync_harness():
    # The control image may predate local harness changes; a stale workload would silently record the wrong thing
    k8s.kubectl("exec", "control", "--", "rm", "-rf", "/app/harness")
    k8s.kubectl("cp", str(ROOT / "harness"), "control:/app/harness")


def reset_cluster(read_mode):
    """Recreate the kv pods, and with them their tmpfs data: the checker assumes every register starts absent"""
    k8s.kubectl("scale", "statefulset/kv", "--replicas=0")
    k8s.wait_until(
        lambda: not k8s.kubectl("get", "pods", "-l", "app=kv", "-o", "name").stdout.strip(),
        "kv pods to terminate", timeout=120,
    )
    k8s.kubectl("set", "env", "statefulset/kv", f"READ_MODE={read_mode}")
    k8s.kubectl("scale", "statefulset/kv", "--replicas=3")
    k8s.kubectl("rollout", "status", "statefulset/kv", "--timeout=180s", timeout=200)


def check_fresh(read_mode):
    for pod in k8s.KV_PODS:
        st = k8s.wait_until(lambda: k8s.status(pod), f"{pod} to answer status", timeout=60)
        if st["read_mode"] != read_mode:
            raise RuntimeError(f"{pod} runs read-mode={st['read_mode']}, expected {read_mode}")
        # A wiped node's log holds at most one leader no-op per term
        if st["last_log_index"] > st["term"]:
            raise RuntimeError(f"{pod} is not fresh: {st}")


def run_workload(cfg):
    nodes = ",".join(f"{pod}={addr}" for pod, addr in cfg.nodes.items())
    k8s.kubectl("exec", "control", "--", "rm", "-f", REMOTE_HISTORY)
    p = k8s.kubectl(
        "exec", "control", "--",
        "python", "-m", "harness.workload", "--nodes", nodes, "--out", REMOTE_HISTORY,
        "--duration", str(cfg.duration), "--clients", str(cfg.clients), "--seed", str(cfg.seed),
        check=False, capture=False, timeout=cfg.duration + 120,
    )
    if p.returncode != 0:
        raise RuntimeError(f"workload exited with status {p.returncode}")


def collect(out):
    history = k8s.kubectl("exec", "control", "--", "cat", REMOTE_HISTORY, timeout=120).stdout
    (out / "history.jsonl").write_text(history)
    for pod in k8s.KV_PODS:
        (out / f"{pod}.log").write_text(k8s.kubectl("logs", pod, timeout=60).stdout)
    return read_history(out / "history.jsonl")


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m harness.run")
    p.add_argument("--name", required=True, help="results/<name> receives history, node logs, run.json")
    p.add_argument("--results-dir", default=str(ROOT / "results"))
    p.add_argument("--read-mode", choices=("local", "log"), default="local")
    p.add_argument("--duration", type=float, default=30.0)
    p.add_argument("--clients", type=int, default=6)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--force", action="store_true")
    args = p.parse_args(argv)

    out = prepare_out_dir(Path(args.results_dir) / args.name, args.force)
    cfg = Config(
        nodes={pod: k8s.kv_addr(pod) for pod in k8s.KV_PODS},
        clients=args.clients, duration=args.duration, seed=args.seed,
    )
    meta = {
        "name": args.name,
        "read_mode": args.read_mode,
        "nemesis": None,
        "seed": args.seed,
        "duration_s": args.duration,
        "clients": args.clients,
        "git": git_describe(),
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    step("syncing harness code into the control pod")
    sync_harness()
    step(f"resetting the kv cluster to empty state (read-mode={args.read_mode})")
    reset_cluster(args.read_mode)
    pod, term = k8s.wait_until(k8s.leader, "a leader", timeout=60)
    check_fresh(args.read_mode)
    meta["leader_at_start"] = {"pod": pod, "term": term}
    meta["kvnode_image"] = k8s.kubectl(
        "get", "pod", "kv-0", "-o", "jsonpath={.status.containerStatuses[0].imageID}"
    ).stdout.strip()
    step(f"leader {pod} (term {term}); running workload for {args.duration:g}s with {args.clients} clients")

    run_workload(cfg)
    step(f"collecting history and node logs into {out}")
    ops = collect(out)
    meta["summary"] = summarize(ops)
    (out / "run.json").write_text(json.dumps(meta, indent=2) + "\n")

    print(format_summary(meta["summary"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
