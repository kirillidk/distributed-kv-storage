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
kv-engine; it encodes writing events and proposes their bytes to repl. In a
single-node cluster, the elected leader persists each payload with fsync before
calling back into `EngineService.Apply`.
Repl does not expose KV methods or decode engine events.

The supervisor sets `REPL_CONNECT_ADDR` to `repl.connect_addr` in the engine
process environment. kv-engine uses it by default; `--repl-addr HOST:PORT`
overrides it. Without `wal_path`, repl serves health and supervises an in-memory
engine.

At every engine start, repl replays the missing log suffix using `GetOffset` and
`Apply`. During recovery, engine Set/Get and repl Propose return Unavailable.
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

### Raft elections

When `wal_path` is configured, every node in the shared manifest is a voter and
must have its own WAL. The supervisor connects to all other `repl.connect_addr`
addresses and serves [`RaftService`](../internal/repl/proto/repl/raft/v1/raft.proto) alongside
health and logs, independently of engine recovery. Membership is static.

Each election needs a strict majority of all configured nodes, including those
currently offline. RequestVote compares the last record's term first, then its
Raft index (record count, separate from the engine's byte offset). The election
timeout is sampled independently from `[500ms, 1s)` on every reset. Leaders send
empty AppendEntries immediately after election and then every 100ms; peer RPCs
have a 200ms deadline. A higher term in a request or response demotes the node.

Term and votedFor are stored in `<wal_path>.raft-state` with a checksum, atomic
replacement, file fsync and directory fsync before votes are acknowledged or
election requests are sent. Keep this file with the WAL across restarts. Corrupt
state or persistence failures stop the node. WAL records now include their term;
legacy records are still readable with term zero and retain their engine offsets.

This stage implements elections and heartbeats. Nonempty AppendEntries and
multi-node proposals return `Unimplemented` until log replication is implemented.
A follower or candidate returns `Unavailable` for proposals. Single-node writes
become available after both election and engine recovery; the named
`repl.v1.ReplService` health status reflects this. In multi-node clusters that
health status stays NOT_SERVING, while `repl.raft.v1.RaftService` reports SERVING.
Role, term and leader ID changes appear in the repl diagnostic log.

### `nodes[].engine.connect_addr`

Address `repl`, `rpc-proxy`, and `clusterstat` use to connect to the engine.

### `nodes[].engine.command`

Executable and arguments used by `repl` to start the engine.

### Engine process settings

- `restart_delay` is the delay before restarting an exited engine.
- `shutdown_timeout` is how long `repl` waits after `SIGTERM` before killing
  the engine.
