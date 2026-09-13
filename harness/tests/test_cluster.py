from harness.client import Client
from harness.localcluster import LocalCluster


def test_follower_forwards_writes_and_replicas_converge(kvnode_binary, tmp_path):
    with LocalCluster(kvnode_binary, tmp_path) as cluster:
        leader = cluster.wait_leader()
        follower = next(n for n in cluster.nodes if n is not leader)

        with Client(follower.client_addr) as c:
            assert c.put("k0", 1)["outcome"] == "applied"
            assert c.cas("k0", 1, 2)["ok"] is True
        assert leader.get("k0") == 2
        cluster.wait_until(lambda: all(n.get("k0") == 2 for n in cluster.nodes), "all replicas to apply k0=2")


def test_leader_crash_reelects_and_restarted_node_catches_up(kvnode_binary, tmp_path):
    with LocalCluster(kvnode_binary, tmp_path) as cluster:
        old = cluster.wait_leader()
        old_term = old.status()["term"]
        with Client(old.client_addr) as c:
            assert c.put("k0", 1)["outcome"] == "applied"

        old.kill()
        new = cluster.wait_leader(after_term=old_term)
        assert new is not old
        with Client(new.client_addr) as c:
            assert c.put("k0", 7)["outcome"] == "applied"
            assert c.cas("k0", 7, 8)["ok"] is True

        old.start()
        cluster.wait_until(lambda: old.get("k0") == 8, "restarted node to catch up")
        st = old.status()
        assert st["role"] == "follower" and st["term"] > old_term


def test_committed_state_survives_full_cluster_restart(kvnode_binary, tmp_path):
    with LocalCluster(kvnode_binary, tmp_path) as cluster:
        leader = cluster.wait_leader()
        term = leader.status()["term"]
        with Client(leader.client_addr) as c:
            assert c.put("durable", 5)["outcome"] == "applied"

        for n in cluster.nodes:
            n.kill()
        for n in cluster.nodes:
            n.start()

        leader = cluster.wait_leader(after_term=term)
        cluster.wait_until(lambda: leader.get("durable") == 5, "replayed state after full restart")


def test_log_read_mode_routes_reads_through_the_leader(kvnode_binary, tmp_path):
    with LocalCluster(kvnode_binary, tmp_path, read_mode="log") as cluster:
        leader = cluster.wait_leader()
        follower = next(n for n in cluster.nodes if n is not leader)
        with Client(leader.client_addr) as c:
            assert c.put("k0", 3)["outcome"] == "applied"

        before = leader.status()["last_log_index"]
        with Client(follower.client_addr) as c:
            resp = c.get("k0")
        assert (resp["outcome"], resp["value"]) == ("applied", 3)
        assert leader.status()["last_log_index"] == before + 1
        assert follower.status()["read_mode"] == "log"
