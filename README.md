# Helix

**A small Raft key-value store, a Jepsen-style fault injector, and a linearizability checker to prove whether the store stays correct under real failures.**

Helix runs a 3-node Raft cluster on Kubernetes (kind), attacks it with network partitions, process crashes, and pauses while concurrent clients record every operation, and then checks the recorded history for linearizability. When the history isn't linearizable, it names the operations involved and explains why no valid ordering exists, then renders a timeline of the run.

It is modeled on [Jepsen](https://jepsen.io) and [Knossos](https://github.com/jepsen-io/knossos).

## Demo

Three runs with the same workload and seed:

| | Baseline | Naive reads + partition | Fixed reads + partition |
|---|---|---|---|
| Read mode | `leader` | `leader` | `log` |
| Faults | none | isolate the leader | isolate the leader |
| Verdict | linearizable | **not linearizable** | linearizable |
| Report | [verdict](results/01-baseline/verdict.txt) | [verdict](results/02-naive-partition/verdict.txt) | [verdict](results/03-fixed-partition/verdict.txt) |
| Timeline | ![](results/01-baseline/timeline.png) | ![](results/02-naive-partition/timeline.png) | ![](results/03-fixed-partition/timeline.png) |

**What breaks in the naive mode:** the partition leaves the old leader in the minority. It still believes it is leader and keeps answering reads from its own state, while the majority elects a new leader and accepts newer writes. Clients connected to the old leader read stale values, and the checker reports exactly which read contradicted which acknowledged write. In `log` mode, reads are committed through Raft, so a deposed leader can't answer them.

## Quick start

**Requirements:** Go, Python 3.10+, Docker, `kubectl`, [`kind`](https://kind.sigs.k8s.io)

```bash
python -m venv .venv && .venv/bin/pip install matplotlib pytest hypothesis

make kind-up images deploy     # cluster, images, 3-node StatefulSet + control pod
make run-baseline              # leader reads, no faults
make run-naive                 # leader reads, partition the leader
make run-fixed                 # log reads, partition the leader
```

Each run writes to `results/<name>/`:

| File | Contents |
|---|---|
| `history.jsonl` | one line per operation: client, node, op, key, values, invoke and complete times, outcome |
| `nemesis.jsonl` | fault start and stop events |
| `kv-N.log` | node logs: elections, term changes, leadership |
| `run.json` | seed, read mode, nemesis, git revision, image digest, summary |
| `verdict.json`, `verdict.txt` | checker result and violation explanation |
| `timeline.png` | client timelines with fault windows and violating ops highlighted |


## Usage

```bash
make run NAME=my-run READ_MODE=leader NEMESIS=partition-leader DURATION=60 SEED=7
make check NAME=my-run         # re-check an existing history
make viz NAME=my-run           # re-render the timeline

make test                      # go vet, go test -race, pytest
make smoke-raft                # SIGKILL the leader: re-election, forwarding, catch-up
make smoke-iptables            # confirm pod-level partitions work
make run-node                  # single local node on 127.0.0.1:8000
make undeploy kind-down        # clean up
```

**Nemesis schedules**

| Name | Fault |
|---|---|
| `none` | no faults |
| `partition-leader` | cut the current leader off from both peers, heal, repeat |
| `pause-leader` | `SIGSTOP` the leader, then `SIGCONT` it |
| `kill-random` | `SIGKILL` a random node; the supervisor restarts it in place |

**Client CLI**

```bash
python -m harness.client --addr 127.0.0.1:8000 put k0 3
python -m harness.client --addr 127.0.0.1:8000 cas k0 3 4
python -m harness.client --addr 127.0.0.1:8000 get k0
python -m harness.client --addr 127.0.0.1:8000 status
```

## Design

### Store

- **Raft**, following Figure 2 of the paper:
  - Election with randomized timeouts; each new leader appends a no-op entry.
  - Log replication with fast backtracking on conflicts.
  - Leaders only commit entries from their own term.
  - An fsync'd, CRC-framed write-ahead log.
  - No snapshots.
- **Protocol bridge:** clients speak newline-delimited JSON, nodes speak Go `net/rpc`, and followers forward commands to the leader.
- **Data model:** independent registers (`k0`–`k4`) supporting `put`, `get`, and compare-and-swap.

### Read modes

| Mode | Read path | Under partition |
|---|---|---|
| `leader` | the node that believes it is leader answers from its applied state | stale reads from a deposed leader |
| `log` | reads are committed through the Raft log like writes | linearizable |

### Honest outcomes

Every reply says whether a command took effect, so the history never guesses:

| Reply | Recorded as | Checker treats it as |
|---|---|---|
| applied | `ok` | happened, with this result |
| rejected before reaching the log | `fail` | never happened |
| timed out or lost after sending | `info` | may have happened, at any time after invoke |

Clients never retry, and a CAS that ran but found a different value is recorded as an observation of the register.

### Checker

- A Wing & Gong search over each key's invoke and complete events, memoizing (set of ops already placed, register state), as in [Porcupine](https://github.com/anishathalye/porcupine).
- Validated against a brute-force reference on thousands of generated and mutated histories.
- A 60-second run of ~3,500 operations checks in well under a second.
- On failure it reports how far the search got, the register state at that point, and the operation that couldn't be ordered.

### Faults on Kubernetes

- **Partitions:** `iptables -j DROP` rules against peer pod IPs, inside each pod's own network namespace. kind's default network plugin doesn't enforce NetworkPolicy, so policies can't be used.
- **Crashes and pauses** target `kvnode` under a supervisor loop (not PID 1), so the pod, its IP, and its Raft state survive.
- **Storage:** Raft state lives on a memory-backed volume. The fault model covers process crashes, pauses, and partitions; it doesn't cover OS or power loss.


## Scope

**In scope:** one Raft implementation; register + CAS; partitions, crashes, and pauses; 3 nodes on a local kind cluster; CLI output and one PNG per run.

**Out of scope:** other data types, snapshots and membership changes, clock-skew faults, durability across power loss, dashboards, and pluggable consensus.

## References

- Ongaro & Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf), USENIX ATC 2014
- Wing & Gong, *Testing and Verifying Concurrent Objects*, JPDC 1993
- Lowe, *Testing for Linearizability*, CCPE 2017
- Kingsbury, [Jepsen analyses](https://jepsen.io/analyses) and [Knossos](https://github.com/jepsen-io/knossos)
- Athalye, [Porcupine](https://github.com/anishathalye/porcupine)
