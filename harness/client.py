import argparse
import itertools
import json
import socket
import sys


class NotSent(Exception):
    """The request never reached a server, so it definitely had no effect."""


class Indeterminate(Exception):
    """The request may have been received and applied."""


class Client:
    def __init__(self, addr, timeout=3.0):
        host, port = addr.rsplit(":", 1)
        self.addr = (host, int(port))
        self.timeout = timeout
        self._sock = None
        self._rfile = None
        self._ids = itertools.count(1)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        if self._sock is not None:
            self._rfile.close()
            self._sock.close()
            self._sock = self._rfile = None

    def _connect(self):
        try:
            sock = socket.create_connection(self.addr, timeout=self.timeout)
        except OSError as e:
            raise NotSent(f"connect {self.addr[0]}:{self.addr[1]}: {e}") from e
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock, self._rfile = sock, sock.makefile("rb")

    def call(self, op, key, **fields):
        if self._sock is None:
            self._connect()
        req_id = next(self._ids)
        line = json.dumps({"id": req_id, "op": op, "key": key, **fields}).encode() + b"\n"

        # Once any byte may have left, a failure no longer proves the op had no effect.
        try:
            self._sock.sendall(line)
            raw = self._rfile.readline()
        except OSError as e:
            self.close()
            raise Indeterminate(f"{op} {key}: {e!r}") from e
        if not raw:
            self.close()
            raise Indeterminate(f"{op} {key}: connection closed before reply")
        try:
            resp = json.loads(raw)
        except ValueError as e:
            self.close()
            raise Indeterminate(f"{op} {key}: malformed reply {raw!r}") from e
        if resp.get("id") != req_id:
            self.close()
            raise Indeterminate(f"{op} {key}: reply id {resp.get('id')!r} != {req_id}")
        return resp

    def get(self, key):
        return self.call("get", key)

    def put(self, key, value):
        return self.call("put", key, value=value)

    def cas(self, key, old, new):
        return self.call("cas", key, old=old, new=new)


EXIT_CODES = {"applied": 0, "definite": 1, "unknown": 2}


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m harness.client")
    p.add_argument("--addr", default="127.0.0.1:8000")
    p.add_argument("--timeout", type=float, default=3.0)
    sub = p.add_subparsers(dest="op", required=True)
    sub.add_parser("get").add_argument("key")
    put = sub.add_parser("put")
    put.add_argument("key")
    put.add_argument("value", type=int)
    cas = sub.add_parser("cas")
    cas.add_argument("key")
    cas.add_argument("old", type=int)
    cas.add_argument("new", type=int)
    args = p.parse_args(argv)

    with Client(args.addr, args.timeout) as client:
        try:
            if args.op == "get":
                resp = client.get(args.key)
            elif args.op == "put":
                resp = client.put(args.key, args.value)
            else:
                resp = client.cas(args.key, args.old, args.new)
        except NotSent as e:
            print(f"fail (not sent): {e}", file=sys.stderr)
            return EXIT_CODES["definite"]
        except Indeterminate as e:
            print(f"info (indeterminate): {e}", file=sys.stderr)
            return EXIT_CODES["unknown"]

    print(json.dumps(resp))
    return EXIT_CODES.get(resp.get("outcome"), EXIT_CODES["unknown"])


if __name__ == "__main__":
    sys.exit(main())
