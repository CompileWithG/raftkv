# raftkv

**A distributed, replicated key/value store built on a from-scratch implementation of the Raft consensus algorithm — no external consensus libraries.**

`raftkv` is a small, readable, production-shaped teaching/portfolio project. Every part of Raft that makes replication *safe* — leader election, log replication, the election restriction, log-matching with conflict truncation, the commit-index rule, and crash-safe persistence — is implemented here by hand, following the [extended Raft paper](https://raft.github.io/raft.pdf) (Ongaro & Ousterhout). On top of the Raft core sits a linearizable-write key/value state machine exposed over a small REST API.

---

## Features

- **From-scratch Raft core** (`internal/raft`) — no `hashicorp/raft`, no external consensus code.
- **Leader election** with randomized election timeouts, terms, split-vote recovery, and step-down on a higher term.
- **Log replication** via AppendEntries (doubling as heartbeat), per-follower `nextIndex`/`matchIndex`, and majority-based commit advancement.
- **Safety rules**: the election restriction (a candidate's log must be up-to-date to win), the log-matching property with **conflict truncation**, and the current-term commit restriction (§5.4.2).
- **Crash-safe persistence**: `currentTerm`, `votedFor`, and the log are written to disk (atomic rename) and reloaded on boot, so a restarted node rejoins and catches up.
- **Replicated KV state machine**: `set`/`delete` go through the Raft log; committed entries are applied in order on every node.
- **REST API** per node: `PUT/GET/DELETE /kv/{key}` and `GET /status`, with **leader redirects** (307 + JSON hint) for writes and reads sent to a follower.
- **Two transports behind one interface**: an in-memory network for fast, deterministic, race-tested tests, and an HTTP/JSON transport for the real multi-process cluster.
- **Deterministic in-process cluster tests** covering election, replication, failover, log repair, and persistence — all passing under `-race`.

---

## Architecture

```
                    ┌─────────────┐   PUT /kv/k  (or redirect)
     client ───────▶│   raftctl   │───────────────────────────┐
                    └─────────────┘                            │
                                                               ▼
                       ┌───────────────────────── node (LEADER) ─────────────────────────┐
                       │  REST API            Raft core                 State machine     │
                       │  /kv, /status  ──▶  append to log  ──┐                           │
                       │                                      │  apply on commit          │
                       │                     ┌── replicated log ──┐   ┌───────────────┐   │
                       │                     │ 1:noop 2:set k=v ..│──▶│  KV map       │   │
                       │                     └────────────────────┘   └───────────────┘   │
                       └───────────┬───────────────────────────────────────┬─────────────┘
                                   │ AppendEntries (entries + heartbeat)    │
                        RequestVote│ / commitIndex                          │
                                   ▼                                        ▼
                  ┌──────── node (FOLLOWER) ────────┐      ┌──────── node (FOLLOWER) ────────┐
                  │ replicated log ──▶ KV map       │      │ replicated log ──▶ KV map       │
                  └─────────────────────────────────┘      └─────────────────────────────────┘

     A write is committed once it is stored on a MAJORITY of nodes, then applied
     to every node's state machine in identical log order.
```

### Package layout

```
cmd/raftkv     server binary: one node (flags --id --addr --peers --data-dir)
cmd/raftctl    tiny CLI client (put/get/del/status); follows leader redirects
internal/raft  the Raft consensus core + persistence (the heart of the project)
internal/kv    the replicated key/value state machine
internal/transport  in-memory network (tests) + HTTP/JSON transport (real cluster)
internal/server     HTTP wiring: REST API, Raft RPC endpoints, apply loop
scripts/demo.sh     live 3-node failover demo
```

---

## How it works in *this* implementation

### Leader election
Each node runs a **ticker** (`raft.go: ticker`). If it is not the leader and no valid heartbeat/vote has reset its timer within a randomized timeout (`[250ms, 400ms)`), it becomes a **Candidate**, bumps `currentTerm`, votes for itself, persists, and sends `RequestVote` to all peers. A candidate that collects a **quorum** (`⌊n/2⌋+1`, including its own vote) becomes **Leader**. Randomized timeouts make simultaneous candidacies (split votes) rare, and a fresh random timeout is drawn each round so a split resolves quickly. Any RPC carrying a higher term immediately forces the node back to **Follower** (`becomeFollower`).

### Log replication
On becoming leader, a node initializes `nextIndex[peer] = lastIndex()+1` and `matchIndex[peer] = 0`, and starts a **leader loop** that broadcasts `AppendEntries` every 60 ms (heartbeats when there is nothing new). A client write is appended to the leader's log via `Submit` and pushed out immediately. `replicateTo` sends the entries after `nextIndex`; on success it advances `matchIndex`/`nextIndex`, on rejection it **backs `nextIndex` up using the follower's conflict hint** (see below). `advanceCommit` then finds the highest index replicated on a majority and commits it. Committed entries are handed to the state machine, in order, by the **applier** goroutine.

### Safety
- **Election restriction** (`HandleRequestVote`): a vote is granted only if the candidate's `(lastLogTerm, lastLogIndex)` is at least as up-to-date as the voter's. This guarantees a new leader already holds every committed entry.
- **Log matching + conflict truncation** (`HandleAppendEntries`): the follower rejects unless it has an entry at `PrevLogIndex` with `PrevLogTerm`. On a term conflict it truncates the divergent tail and appends the leader's entries — but it **never truncates on a matching entry**, so a delayed/duplicate AppendEntries can't chop off already-committed data.
- **Fast conflict backup**: instead of decrementing `nextIndex` one entry per round trip, a rejecting follower returns `ConflictTerm` + `ConflictIndex`, letting the leader skip a whole conflicting term at once.
- **Commit restriction (§5.4.2)**: `advanceCommit` only commits an index whose entry is from the **leader's current term**. To make prior-term entries committable promptly, a new leader appends a **no-op entry** in its own term on election — this is why an index of `1` is always a no-op and client data starts at index `2`.
- **Persistence**: `currentTerm`, `votedFor`, and the log are flushed (atomically) before an RPC that depends on them returns, so guarantees hold across a crash. On boot the node reloads them and rejoins.

---

## Quickstart

```bash
make build            # builds ./bin/raftkv and ./bin/raftctl
```

Run a 3-node cluster (three terminals, or use the demo script below):

```bash
./bin/raftkv --id n1 --addr 127.0.0.1:9001 --peers n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir data/n1
./bin/raftkv --id n2 --addr 127.0.0.1:9002 --peers n1=127.0.0.1:9001,n3=127.0.0.1:9003 --data-dir data/n2
./bin/raftkv --id n3 --addr 127.0.0.1:9003 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 --data-dir data/n3
```

Drive it with the CLI (auto-redirects to the leader, so any node works):

```bash
./bin/raftctl -addr 127.0.0.1:9002 put city paris
./bin/raftctl -addr 127.0.0.1:9003 get city        # -> {"key":"city","value":"paris"}
./bin/raftctl -addr 127.0.0.1:9001 status
```

…or with plain `curl`:

```bash
curl -L -X PUT  --data 'paris' 127.0.0.1:9002/kv/city    # -L follows the leader redirect
curl -L         127.0.0.1:9003/kv/city
curl            127.0.0.1:9001/status
```

### Example: leader failover (automated)

`scripts/demo.sh` launches a real 3-node cluster, writes a key, **kills the leader process**, shows a new leader being elected, proves the key survived, then **restarts the dead node** and shows it rejoin and catch up — finally tearing everything down cleanly.

```bash
make demo
```

Observed output (abridged):

```
=== 2. waiting for leader election ===
  leader elected: n1
=== 3. PUT city=paris ===              {"committed":2,"key":"city","op":"set"}
=== 4. GET city from n3 ===            {"key":"city","value":"paris"}
=== 5. killing the leader (n1) ===
  NEW leader elected: n2 (was n1)
=== 6. GET city again after failover ==={"key":"city","value":"paris"}   # survived
=== 8. restarting the dead node (n1) === {"commitIndex":4,"lastApplied":4,"state":"Follower"}  # rejoined & caught up
```

---

## Testing

```bash
make test        # go test ./... -race -count=1
```

All Raft behaviour is covered by an **in-process simulated cluster** (`internal/raft/raft_test.go`): N real Raft nodes wired through an in-memory transport that can drop/partition traffic. Tests poll with timeouts (never fixed sleeps), so they are fast and non-flaky under the race detector.

| Test | Proves |
|------|--------|
| `TestLeaderElection` | exactly one leader is elected within an election timeout |
| `TestReplicationCommitApply` | a command replicated to a majority is committed and applied on all nodes |
| `TestLeaderFailoverAndRecovery` | killing the leader triggers re-election, the cluster keeps serving, and a crashed node rejoins and catches up |
| `TestLogRepairAfterPartition` | a lagging/divergent follower's log is repaired by AppendEntries after reconnecting |
| `TestPersistenceAcrossRestart` | committed entries survive a full node restart (state reloaded from disk) |

---

## Design decisions & trade-offs

- **Leader-only reads.** `GET` is served from the leader's applied state; a follower redirects. This avoids returning stale values from a node that has fallen behind. It is *not* strictly linearizable (a leader that was just partitioned could serve a stale read before it steps down); a lease or a read-index/ReadIndex barrier would close that gap. Chosen for simplicity and honesty over a false claim of linearizability.
- **Synchronous writes.** A `PUT`/`DELETE` returns only after the entry is committed *and* applied on the leader, so a `200` means the write is durable on a majority. This makes the API easy to reason about at the cost of write latency ≈ one replication round trip.
- **No-op on election.** A new leader commits a no-op in its term so prior-term entries become committable immediately (§5.4.2). Simpler and more responsive than making clients retry until the next write.
- **HTTP/JSON transport.** Readable and dependency-free (`net/http` + `encoding/json`) rather than the fastest option (gRPC/protobuf). The `Transport` interface keeps the wire format swappable and lets tests use an in-memory network.
- **Whole-log persistence as JSON.** Simple and inspectable. Fine for a demo; a real system would use a segmented, appended, checksummed log.

## Known limitations / possible improvements

Honest about what is **not** implemented:

- **No log compaction / snapshotting.** The log grows unbounded; a restarted node replays its entire log. Adding InstallSnapshot + state-machine snapshots (paper §7) is the natural next step.
- **No dynamic membership changes.** The peer set is fixed at start-up; there is no joint-consensus reconfiguration (paper §6) to add/remove nodes online.
- **Reads are not linearizable** (see leader-only reads above) — no lease/ReadIndex.
- **No client de-duplication / exactly-once.** A client that retries a `PUT` after a timeout could apply it twice. Real systems attach client IDs + sequence numbers and dedupe in the state machine.
- **No pre-vote.** A partitioned node rejoining can disrupt the cluster by bumping the term; the Pre-Vote extension avoids that.
- **No batching/pipelining or persistence fsync tuning** — correctness-first, not throughput-tuned.

---

## License

MIT — for learning and portfolio use.
