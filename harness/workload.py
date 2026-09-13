import argparse
import random
import sys
import threading
from collections import Counter
from dataclasses import dataclass

from harness.client import Client, Indeterminate, NotSent
from harness.history import Clock, HistoryWriter, classify, read_history


@dataclass(frozen=True)
class Config:
    nodes: dict
    clients: int = 6
    duration: float = 30.0
    keys: int = 5
    values: int = 5
    read_ratio: float = 0.5
    write_ratio: float = 0.25
    min_delay: float = 0.05
    max_delay: float = 0.15
    timeout: float = 3.0
    seed: int = 42


def generate(rng, cfg):
    key = f"k{rng.randrange(cfg.keys)}"
    r = rng.random()
    if r < cfg.read_ratio:
        return "get", key, {}
    if r < cfg.read_ratio + cfg.write_ratio:
        return "put", key, {"value": rng.randrange(cfg.values)}
    return "cas", key, {"old": rng.randrange(cfg.values), "new": rng.randrange(cfg.values)}


def run_op(client, clock, op, key, args):
    # Connecting happens inside the timed window: a wider interval can hide a violation, never invent one
    invoke_ns = clock.now()
    try:
        response, error = client.call(op, key, **args), None
    except (NotSent, Indeterminate) as e:
        response, error = None, e
    complete_ns = clock.now()

    type_, read, err = classify(op, response, error)
    return dict(
        op=op, key=key, value=read if op == "get" else args.get("value"), old=args.get("old"), new=args.get("new"),
        invoke_ns=invoke_ns, complete_ns=complete_ns, type=type_, err=err,
    )


def client_loop(client_id, node, addr, cfg, clock, writer, stop):
    rng = random.Random(f"{cfg.seed}:{client_id}")
    with Client(addr, timeout=cfg.timeout) as client:
        while not stop.is_set():
            op, key, args = generate(rng, cfg)
            writer.record(client=client_id, node=node, **run_op(client, clock, op, key, args))
            stop.wait(rng.uniform(cfg.min_delay, cfg.max_delay))


def run(cfg, path):
    node_ids = list(cfg.nodes)
    clock = Clock()
    stop = threading.Event()
    failures = []

    def guarded(*args):
        try:
            client_loop(*args)
        except BaseException as e:
            failures.append(e)
            stop.set()

    with HistoryWriter(path) as writer:
        threads = []
        for c in range(cfg.clients):
            node = node_ids[c % len(node_ids)]
            args = (c, node, cfg.nodes[node], cfg, clock, writer, stop)
            threads.append(threading.Thread(target=guarded, args=args, name=f"client-{c}"))
        for t in threads:
            t.start()
        try:
            stop.wait(cfg.duration)
        except KeyboardInterrupt:
            pass
        finally:
            stop.set()
            for t in threads:
                t.join()

    if failures:
        raise failures[0]
    return summarize(read_history(path))


def summarize(ops):
    return {
        "ops": len(ops),
        "types": dict(Counter(o.type for o in ops)),
        "by_op": {op: dict(Counter(o.type for o in ops if o.op == op)) for op in ("get", "put", "cas")},
        "errors": dict(Counter(o.err for o in ops if o.err)),
        "clients": dict(Counter(f"{o.client}@{o.node}" for o in ops)),
    }


def format_summary(s):
    lines = [f"{s['ops']} ops: " + ", ".join(f"{k}={v}" for k, v in sorted(s["types"].items()))]
    for op, counts in s["by_op"].items():
        lines.append(f"  {op:<4} " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if s["errors"]:
        lines.append("  errors " + ", ".join(f"{k}={v}" for k, v in sorted(s["errors"].items())))
    lines.append("  clients " + ", ".join(f"{k}={v}" for k, v in sorted(s["clients"].items())))
    return "\n".join(lines)


def parse_nodes(s):
    nodes = {}
    for part in s.split(","):
        node, sep, addr = part.strip().partition("=")
        if not sep or not node or not addr:
            raise argparse.ArgumentTypeError(f"invalid node {part!r} (want id=host:port)")
        nodes[node] = addr
    return nodes


def main(argv=None):
    defaults = Config(nodes={})
    p = argparse.ArgumentParser(prog="python -m harness.workload")
    p.add_argument("--nodes", type=parse_nodes, required=True, help="id=host:port,... client addresses")
    p.add_argument("--out", required=True)
    p.add_argument("--clients", type=int, default=defaults.clients)
    p.add_argument("--duration", type=float, default=defaults.duration)
    p.add_argument("--keys", type=int, default=defaults.keys)
    p.add_argument("--values", type=int, default=defaults.values)
    p.add_argument("--read-ratio", type=float, default=defaults.read_ratio)
    p.add_argument("--write-ratio", type=float, default=defaults.write_ratio)
    p.add_argument("--min-delay", type=float, default=defaults.min_delay)
    p.add_argument("--max-delay", type=float, default=defaults.max_delay)
    p.add_argument("--timeout", type=float, default=defaults.timeout)
    p.add_argument("--seed", type=int, default=defaults.seed)
    args = vars(p.parse_args(argv))
    out = args.pop("out")
    cfg = Config(**args)
    if cfg.read_ratio + cfg.write_ratio > 1:
        p.error("--read-ratio + --write-ratio must be <= 1")

    print(format_summary(run(cfg, out)), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
