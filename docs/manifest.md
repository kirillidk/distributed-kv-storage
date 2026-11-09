# Cluster manifest

`cluster.yaml` describes the static cluster topology and the processes running
on each node. The manifest is shared by components, but each component uses
only the fields it needs.

## Example

```yaml
nodes:
  - id: node-1
    repl:
      listen_addr: 0.0.0.0:7001
      connect_addr: node-1:7001
      log_dir: /var/log/distributed-kv/node-1
      wal_path: /var/lib/repl/node-1.wal

    engine:
      connect_addr: node-1:8001
      command:
        - /usr/local/bin/kv-engine
        - --port
        - "8001"
      restart_delay: 3s
      shutdown_timeout: 5s
```

## Fields

### `nodes[].id`

Unique node identifier. A local `repl` selects its configuration using
`--node-id`.

### `nodes[].repl.listen_addr`

Address on which the local `repl` binds its gRPC server.

### `nodes[].repl.connect_addr`

Address other cluster components use to connect to this `repl`.

### `nodes[].repl.log_dir`

Optional directory for `repl.log` (supervisor events) and `kv-engine.log`
(engine stdout/stderr). Files use JSON Lines and are appended to across
restarts. Relative paths use the `repl` working directory.

Omit the field for console-only logging. Files are available through
[`ReplAdminService.ReadLogs`](../api/proto/repl/v1/admin.proto).

### `nodes[].repl.wal_path`

Optional path to the local binary write-ahead log. When set, `repl` exposes
`repl.v1.ReplService.Propose` on its listen address. Clients send Set/Get to
kv-engine; it encodes writing events and proposes their bytes to repl. The
elected leader persists each payload with fsync and waits for durable replication
on a strict majority before calling back into `EngineService.Apply`.
Repl does not expose KV methods or decode engine events.

The supervisor sets `REPL_CONNECT_ADDR` to `repl.connect_addr` in the engine
process environment. kv-engine uses it by default; `--repl-addr HOST:PORT`
overrides it. Without `wal_path`, repl serves health and supervises an in-memory
engine.

At every engine start, repl replays the missing committed log prefix using
`GetOffset` and `Apply`. An uncommitted suffix remains in the WAL without being
applied. During recovery, engine Set/Get and repl Propose return Unavailable.
The engine and the named `repl.v1.ReplService` Health service report NOT_SERVING;
the default repl Health service stays SERVING while its supervisor runs.
Repl calls `EngineService.SetReady` to enable client requests after replay finishes.
Every engine RPC made by repl has
a one-second timeout; the whole log has no fixed time limit. Replay runs in the background,
independently of client requests.

Payloads are limited to 4 MiB. The internal gRPC limit also reserves space for
the protobuf fields, including the offset. Oversized payloads are rejected
before they reach the WAL.

The parent directory must already exist. Relative paths are resolved against
the `repl` working directory. Each file must be owned by one `repl` process.
Keep the file on persistent storage to recover across process restarts.
On opening the WAL, repl syncs the file and its parent directory before
accepting writes, so creating the log also persists its directory entry.

### Raft elections and replication

When `wal_path` is configured, every node in the shared manifest is a voter and
must have its own WAL. The supervisor connects to all other `repl.connect_addr`
addresses and serves [`RaftService`](../internal/repl/proto/repl/raft/v1/raft.proto) alongside
health and logs, independently of engine recovery. Membership is static.

Each election needs a strict majority of all configured nodes, including those
currently offline. RequestVote compares the last record's term first, then its
Raft index (record count, separate from the engine's byte offset). The election
timeout is sampled independently from `[500ms, 1s)` on every reset. Leaders send
AppendEntries immediately after election, on new proposals and while catching
up followers; idle heartbeats run every 100ms. Peer RPCs have a 200ms deadline.
A higher term in a request or response demotes the node.

Term, votedFor and the committed record index are stored in
`<wal_path>.raft-state` with a checksum, atomic replacement, file fsync and
directory fsync before votes are acknowledged, election requests are sent or
new committed commands reach Apply. Keep this file with the WAL across restarts. Corrupt
state or persistence failures stop the node. WAL records now include their term;
legacy records are still readable with term zero and retain their engine offsets.

Each follower checks the preceding record's index and term, durably truncates
only a conflicting uncommitted suffix, and fsyncs incoming records before
acknowledging them. Repeated batches preserve matching entries and extra tails.
The leader tracks each follower's next and matched record indexes and retries
from an earlier prefix after a mismatch. Batches contain up to 128 records and
fit a 4 MiB + 64 KiB gRPC limit, including protobuf framing.

The leader advances commit only when a strict majority, including its own
durable WAL, confirms an entry in its current term. This also commits earlier
entries in the prefix. Previous-term entries alone cannot advance commit;
an uncommitted inherited suffix waits for a new current-term proposal.
AppendEntries carries the committed record index. Followers cap it at the last
record matched by that request, then translate it to the engine's byte
`commitOffset`. Apply runs in order on every node without holding Raft or WAL
locks during engine RPCs. Failed Apply calls are retried through GetOffset and
recovery, without replaying uncommitted entries.

Propose returns the opaque engine result only after commit and Apply. A canceled
request can still commit later; cancellation does not remove its WAL entry.
Losing leadership or stopping the node releases pending proposals with
`Unavailable`. Without quorum, proposals wait until their context expires or
leadership changes. Followers and candidates return `Unavailable` for proposals.
The named `repl.v1.ReplService` health status becomes SERVING after both election
and engine recovery, in single-node and multi-node deployments.
`repl.raft.v1.RaftService` stays available independently of engine recovery.
Role, term and leader ID changes appear in the repl diagnostic log.

The previous RFT1 state format is readable, but contains no commit boundary.
Its WAL entries remain unapplied until Raft establishes their commit through a
leader's AppendEntries or a current-term quorum commit.

### `nodes[].engine.connect_addr`

Address `repl`, `rpc-proxy`, and `clusterstat` use to connect to the engine.

### `nodes[].engine.command`

Executable and arguments used by `repl` to start the engine.

### Engine process settings

- `restart_delay` is the delay before restarting an exited engine.
- `shutdown_timeout` is how long `repl` waits after `SIGTERM` before killing
  the engine.
