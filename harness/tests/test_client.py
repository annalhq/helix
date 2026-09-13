import json
import socket
import threading

import pytest

from harness.client import Client, Indeterminate, NotSent, main
from harness.localcluster import LocalCluster, free_port


@pytest.fixture(scope="module")
def node(kvnode_binary, tmp_path_factory):
    with LocalCluster(kvnode_binary, tmp_path_factory.mktemp("single"), size=1) as cluster:
        yield cluster.wait_leader().client_addr


def test_put_get_cas_roundtrip(node):
    with Client(node) as c:
        assert c.put("k0", 3) == {"id": 1, "ok": True, "value": None, "outcome": "applied"}
        assert c.get("k0")["value"] == 3
        mismatch = c.cas("k0", 1, 2)
        assert (mismatch["ok"], mismatch["err"], mismatch["outcome"]) == (False, "cas_mismatch", "applied")
        assert c.cas("k0", 3, 4)["ok"] is True
        assert c.get("k0")["value"] == 4


def test_missing_key_reads_null_and_never_cas_matches(node):
    with Client(node) as c:
        assert c.get("absent") == {"id": 1, "ok": True, "value": None, "outcome": "applied"}
        assert c.cas("absent", 0, 1)["ok"] is False


def test_bad_request_is_definite(node):
    with Client(node) as c:
        resp = c.call("put", "k0")
        assert (resp["err"], resp["outcome"]) == ("bad_request", "definite")
        assert c.get("k0")["outcome"] == "applied"


def test_status(node):
    with Client(node) as c:
        st = c.status()["status"]
    assert (st["id"], st["role"], st["leader"], st["read_mode"]) == ("kv-0", "leader", "kv-0", "local")
    assert st["term"] >= 1 and st["last_applied"] <= st["commit_index"] <= st["last_log_index"]


def test_separate_connections_see_same_state(node):
    with Client(node) as a, Client(node) as b:
        a.put("shared", 2)
        assert b.get("shared")["value"] == 2


def test_cli(node, capsys):
    assert main(["--addr", node, "put", "cli", "2"]) == 0
    assert main(["--addr", node, "cas", "cli", "2", "3"]) == 0
    capsys.readouterr()
    assert main(["--addr", node, "get", "cli"]) == 0
    assert json.loads(capsys.readouterr().out)["value"] == 3
    assert main(["--addr", node, "status"]) == 0
    assert json.loads(capsys.readouterr().out)["status"]["role"] == "leader"


def test_connection_refused_is_not_sent():
    with Client(f"127.0.0.1:{free_port()}", timeout=0.5) as c:
        with pytest.raises(NotSent):
            c.get("k0")


def test_silent_server_is_indeterminate():
    with socket.socket() as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        with Client(f"127.0.0.1:{srv.getsockname()[1]}", timeout=0.2) as c:
            with pytest.raises(Indeterminate):
                c.put("k0", 1)


def test_server_closing_after_request_is_indeterminate():
    with socket.socket() as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)

        def accept_and_hang_up():
            conn, _ = srv.accept()
            conn.recv(1024)
            conn.close()

        t = threading.Thread(target=accept_and_hang_up)
        t.start()
        with Client(f"127.0.0.1:{srv.getsockname()[1]}", timeout=2) as c:
            with pytest.raises(Indeterminate):
                c.cas("k0", 1, 2)
        t.join()
