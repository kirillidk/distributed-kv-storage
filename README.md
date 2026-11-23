# Distributed KV Storage

Distributed key-value storage written in Go. The project consists of separate
`kv-engine`, `repl`, `kvctl`, and `clusterstat` binaries.

## API contracts

- `api/proto` contains source contracts
- `api/gen/go` contains generated messages and gRPC stubs
- `internal/repl/proto` and `internal/repl/gen/go` contain the repl-owned Raft protocol

## KV API v1

[`kv.proto`](api/proto/kv/v1/kv.proto) defines `Set`, `Get`, `Add`, `Replace`,
`Delete`, `Incr`, `Decr`, and `GetExpireTime`.
Keys and values are byte sequences; keys must not be empty.

`Add` creates an absent key, `Replace` updates an existing key, and `Delete`
removes an existing key. Each returns whether it changed the key. Expired keys
count as absent. `Incr` and `Decr` operate on decimal `int64` values with signed
`int64` deltas, preserve expiration, and reject overflow without changing the value.

TTL is expressed in seconds:

- `0` means no expiration
- values up to 30 days are relative
- larger values are Unix timestamps

Expired keys are returned as `NotFound`. Other expected gRPC codes are
`InvalidArgument`, `FailedPrecondition`, `Unavailable`, and `Internal`.
Health checks use the standard `grpc.health.v1.Health` service.

`Get` and `GetExpireTime` return an absolute expiration timestamp in seconds,
or `0` for no expiration. During recovery, client methods return `Unavailable`.
With a local `repl`, every mutation goes through commit and `Apply` before its
result is returned. Conditional checks use the timestamp recorded in the command,
so replay has the same result even after a key's expiration time has passed.

Go import path:

```go
import kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
```

## kvctl

Start the interactive client with a KVService address:

```bash
go run ./cmd/kvctl --address 127.0.0.1:8001
```

```text
kv> set name distributed-kv 0
OK
kv> get name
value: "distributed-kv"
expires_at: 0
```

Use `help` to list command syntax, the corresponding `kv.v1.KVService` RPC,
and the result format:

| Command | RPC | Result |
| --- | --- | --- |
| `set <key> <value> <ttl>` | `Set` | `OK` |
| `get <key>` | `Get` | `value`, `expires_at` |
| `add <key> <value> <ttl>` | `Add` | `success: true\|false` |
| `replace <key> <value> <ttl>` | `Replace` | `success: true\|false` |
| `delete <key>` | `Delete` | `success: true\|false` |
| `get-expire-time <key>` | `GetExpireTime` | `expires_at` |
| `incr <key> <delta>` | `Incr` | `value` |
| `decr <key> <delta>` | `Decr` | `value` |

Values can contain spaces; double quotes support escapes and empty values
(for example, `add empty "" 0`). `delta` is a signed decimal `int64`, including
zero and negative values. Each RPC uses the `--timeout` setting (default `3s`);
errors show the gRPC status code and the session remains open.

## Cluster manifest and sharding

The version 1 manifest lists each `repl` process in `nodes` and each logical
Raft group in `shards`. Each replica assigns one engine and a private
`data_dir` to a node. Starting a node starts all of its assigned engines.
[Manifest fields and upgrade instructions](docs/manifest.md)

The local example places the replicas as follows. [Two-shard manifest](examples/two-shards/cluster.yaml)

| Shard | node-1 engine | node-2 engine | Votes needed to commit |
| --- | --- | --- | --- |
| shard-a | 127.0.0.1:8001 | 127.0.0.1:8011 | 2 |
| shard-b | 127.0.0.1:8002 | | 1 |

With `repl`, `kv-engine` and `kvctl` on your `PATH`, run one command per
terminal from the repository root.

```sh
repl --manifest examples/two-shards/cluster.yaml --node-id node-1
repl --manifest examples/two-shards/cluster.yaml --node-id node-2
```

Connect `kvctl --address 127.0.0.1:8002` and run `set beta hello 0` followed
by `get beta`. This also works when only node-1 is running. The key `alpha`
belongs to shard-a. Send its writes to the elected leader's engine and read
from either ready replica. Followers apply committed entries in the background.

A key selects index `CRC32-IEEE(key bytes) % len(shards)` in the lexicographically
sorted shard IDs. Reordering the manifest preserves placement. Each engine
rejects keys outside its shard with `FailedPrecondition`. [Go key placement function](api/sharding/sharding.go)

## Development

Go and [Buf](https://buf.build/docs/cli/) are required. Generator versions are
pinned in `buf.gen.yaml` and `internal/repl/buf.gen.yaml`.

```bash
make format
make generate
make ci
make breaking
```

Generated code is committed to the repository and must not be edited manually.
