import socket
import subprocess
import time
from pathlib import Path

from harness.client import Client, Indeterminate, NotSent


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    def __init__(self, binary, workdir, node_id, client_addr, raft_addr, peers, read_mode):
        self.binary = binary
        self.id = node_id
        self.client_addr = client_addr
        self.raft_addr = raft_addr
        self.peers = peers
        self.read_mode = read_mode
        self.data_dir = Path(workdir) / node_id
        self.log_path = Path(workdir) / f"{node_id}.log"
        self.proc = None
        self._log = None

    def start(self):
        self._log = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            [
                str(self.binary),
                "--id", self.id,
                "--client-addr", self.client_addr,
                "--raft-addr", self.raft_addr,
                "--peers", self.peers,
                "--data-dir", str(self.data_dir),
                "--read-mode", self.read_mode,
            ],
            stderr=self._log,
        )

    def kill(self):
        self.proc.kill()
        self.proc.wait()
        self._log.close()

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if self._log is not None:
            self._log.close()

    def status(self):
        try:
            with Client(self.client_addr, timeout=0.5) as c:
                return c.status().get("status")
        except (NotSent, Indeterminate):
            return None

    def get(self, key):
        try:
            with Client(self.client_addr, timeout=0.5) as c:
                resp = c.get(key)
        except (NotSent, Indeterminate):
            return None
        return resp.get("value") if resp.get("outcome") == "applied" else None


class LocalCluster:
    def __init__(self, binary, workdir, size=3, read_mode="local"):
        Path(workdir).mkdir(parents=True, exist_ok=True)
        ids = [f"kv-{i}" for i in range(size)]
        raft_addrs = {i: f"127.0.0.1:{free_port()}" for i in ids}
        peers = ",".join(f"{i}={a}" for i, a in raft_addrs.items())
        self.nodes = [
            Node(binary, workdir, i, f"127.0.0.1:{free_port()}", raft_addrs[i], peers, read_mode) for i in ids
        ]

    def __enter__(self):
        for n in self.nodes:
            n.start()
        return self

    def __exit__(self, *exc):
        for n in self.nodes:
            n.stop()

    def logs(self):
        return "\n".join(
            f"--- {n.id}\n{n.log_path.read_text(errors='replace')}" for n in self.nodes if n.log_path.exists()
        )

    def wait_until(self, predicate, what, timeout=10.0):
        deadline = time.monotonic() + timeout
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() > deadline:
                raise TimeoutError(f"timed out after {timeout}s waiting for {what}\n{self.logs()}")
            time.sleep(0.05)

    def leader(self, after_term=0):
        live = [(n, s) for n in self.nodes if (s := n.status())]
        leaders = [(n, s) for n, s in live if s["role"] == "leader" and s["term"] > after_term]
        if not leaders:
            return None
        node, st = max(leaders, key=lambda ns: ns[1]["term"])
        if any(s["term"] > st["term"] for _, s in live):
            return None
        return node

    def wait_leader(self, after_term=0, timeout=10.0):
        return self.wait_until(lambda: self.leader(after_term), f"a leader with term > {after_term}", timeout)
