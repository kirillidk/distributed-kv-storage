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

Optional directory for the local component logs. `repl` creates it and appends
its own structured events to `repl.log` and the engine's stdout/stderr to
`kv-engine.log`, in JSON Lines format. Existing files are preserved across
engine and `repl` restarts. Relative paths use the `repl` working directory;
each node needs its own directory owned by a single `repl` process.

Omit the field to keep console-only logging. `ReplAdminService.ReadLogs` on
the `repl` gRPC address provides paginated read access when file logging is
enabled. See [component logs](component-logs.md) for the protocol and examples.

### `nodes[].engine.connect_addr`

Address `repl`, `rpc-proxy`, and `clusterstat` use to connect to the engine.

### `nodes[].engine.command`

Executable and arguments used by `repl` to start the engine.

### Engine process settings

- `restart_delay` is the delay before restarting an exited engine.
- `shutdown_timeout` is how long `repl` waits after `SIGTERM` before killing
  the engine.
