# Cluster manifest

All nodes and `clusterstat` read the same version 1 YAML file. `cluster_id`
names the cluster, `nodes` lists its repl endpoints, and `shards` lists the
replicas of each logical dataset. Start one repl process per node ID.

```sh
repl --manifest cluster.yaml --node-id node-1
```

The two-node example runs both shard-a and shard-b on node-1. Node-2 runs the
second replica of shard-a. Each node exposes a single repl gRPC address.
[Local example](../examples/two-shards/cluster.yaml) · [Single-node Docker example](../examples/single-node/cluster.yaml)

## Manifest fields

| Field | Meaning |
| --- | --- |
| `version` | Required integer, currently `1`. |
| `cluster_id` | Required cluster identifier carried by replication RPCs and stored with the WAL. |
| `nodes[].id` | Unique node ID selected by `--node-id`. |
| `nodes[].repl.listen_addr` | Local gRPC bind address for Raft, proposals, health and logs. |
| `nodes[].repl.connect_addr` | Advertised address used by engines, peer nodes and clusterstat. |
| `nodes[].repl.log_dir` | Optional directory for JSON Lines diagnostic logs. |
| `shards[].id` | Unique logical shard name. |
| `shards[].replicas[].node_id` | Existing node that supervises this replica. Each node may occur once per shard. |
| `shards[].replicas[].data_dir` | Persistent local directory for this replica, created on startup. |
| `shards[].replicas[].engine.connect_addr` | Unique engine address for KV requests, health and Apply. |
| `shards[].replicas[].engine.command` | Executable and its arguments. Set distinct listen and metrics ports for engines on the same host. |
| `shards[].replicas[].engine.restart_delay` | Delay before restarting this engine after exit, such as `1s`. |
| `shards[].replicas[].engine.shutdown_timeout` | Time allowed for this engine to exit after SIGTERM before it is killed. |

Each shard must have at least one replica. A node may host any number of
different shards, including zero. Local replicas need distinct data directories.
Separate hosts can use the same path. Relative paths resolve against the
working directory of the supervising repl.

## Key placement and request routing

Sort the shard IDs in bytewise ascending order. The unsigned IEEE CRC32 of the
raw key modulo the number of shards selects its zero-based index in that list.
Clients connect directly to an engine of that shard. Writes require its elected
leader. Reads use the selected engine's applied state.

For each local replica, repl starts a separate supervisor, engine connection,
WAL, Raft state machine and recovery loop. Every engine receives its
`KV_CLUSTER_ID`, named `KV_SHARD_ID`, numeric `KV_SHARD_INDEX`,
`KV_SHARD_COUNT` and local `REPL_CONNECT_ADDR`. The engine encodes each mutation
and sends its cluster and shard IDs with `Propose`. Raft peers send the same
pair in `RequestVote` and `AppendEntries`. The node selects the addressed local
replica before accessing its state. Missing IDs return `InvalidArgument`, a
foreign cluster returns `FailedPrecondition`, and an unhosted shard returns
`NotFound`. Replication payloads are opaque and limited to 4 MiB.

## Data and recovery

Each `data_dir` contains `wal` (records), `wal.raft-state` (term, vote and
commit boundary), and `wal.shard` (cluster, node, shard and hash assignment).
Keep all three together on persistent storage. Each engine restart replays its
own committed prefix through `GetOffset` and `Apply`, then enables KV requests
through `SetReady`. During replay that engine returns `Unavailable`. Sibling
replicas continue to serve and retain their offsets.

To upgrade a previous node-based manifest, stop the old cluster and keep a
backup of its files. Declare each old engine as a replica with the same node ID
and voter set. Choose shard names whose sorted indexes match the old numeric
IDs. For each replica, copy its old `wal_path`, `<wal_path>.raft-state` and
`<wal_path>.shard` into its new `data_dir` under the names above. On first
startup repl upgrades a matching numeric binding to the named identity. Logs
from before the introduction of sharding can be adopted by a single-shard
cluster. To retain engine diagnostic history, rename the old `kv-engine.log`
to the corresponding per-shard log filename. Deploy the new repl and engine
binaries together. Changing key placement requires migrating the affected data.

## Health and logs

The default repl health service reports node availability.
`repl.v1.ReplService/<shard-id>` reports SERVING only for a ready local leader.
`repl.raft.v1.RaftService/<shard-id>` reports the local Raft group's availability.
A node with exactly one local replica also publishes the unqualified service names.
Each engine has its own default health endpoint.

With `log_dir` configured, `repl.log` contains node and supervisor events and
`kv-engine.<escaped-shard-id>.log` contains stdout/stderr for one engine.
The filenames use URL path escaping for the shard ID. The logs append across
restarts. Use `ReadLogs.shard_id` or the clusterstat engine log URL parameter
`?shard_id=shard-a` to select the replica. [ReplAdminService](../api/proto/repl/v1/admin.proto)

## Raft elections and replication

Each election needs a strict majority of the shard's configured nodes, including those
currently offline. RequestVote compares the last record's term first, then its
Raft index (record count, separate from the engine's byte offset). The election
timeout is sampled independently from `[500ms, 1s)` on every reset. Leaders send
AppendEntries immediately after election, on new proposals and while catching
up followers; idle heartbeats run every 100ms. Peer RPCs have a 200ms deadline.
A higher term in a request or response demotes the node.

Term, votedFor and the committed record index are stored in
`<data_dir>/wal.raft-state` with a checksum, atomic replacement, file fsync and
directory fsync before votes are acknowledged, election requests are sent or
new committed commands reach Apply. Keep this file with the WAL across restarts.
Corrupt state prevents startup. A runtime Raft persistence failure stops the
affected replica. Other replicas continue serving. If every local replica fails,
repl exits with an error. WAL records include their term. Legacy records remain
readable with term zero and retain their engine offsets.

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
`commitOffset`. Apply runs in order on every replica without holding Raft or WAL
locks during engine RPCs. Failed Apply calls are retried through GetOffset and
recovery, without replaying uncommitted entries.

Propose returns the opaque engine result only after commit and Apply. A canceled
request can still commit later; cancellation does not remove its WAL entry.
Losing leadership or stopping the replica releases pending proposals with
`Unavailable`. Without quorum, proposals wait until their context expires or
leadership changes. Followers and candidates return `Unavailable` for proposals.
Role, term and leader ID changes appear in the repl diagnostic log.

The previous RFT1 state format is readable, but contains no commit boundary.
Its WAL entries remain unapplied until Raft establishes their commit through a
leader's AppendEntries or a current-term quorum commit.
