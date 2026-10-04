# Cluster manifest

`cluster.yaml` describes the static cluster topology and the processes running
on each node. The manifest is shared by components, but each component uses
only the fields it needs.

The current format has no `version` field. Unknown fields are rejected so that
configuration typos fail during startup.

## Example

```yaml
nodes:
  - id: node-1
    repl:
      listen_addr: 0.0.0.0:7001
      connect_addr: node-1:7001

    engine:
      local_addr: 127.0.0.1:8001
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

Address on which the local `repl` binds its gRPC server. A wildcard address
such as `0.0.0.0:7001` is valid here.

### `nodes[].repl.connect_addr`

Address other cluster components use to connect to this `repl`. Its host must
be reachable from those components; a wildcard address is not a valid
destination.

### `nodes[].engine.local_addr`

Address the local `repl` uses for engine Health and internal RPC calls. It can
use loopback because both processes run on the same node.

### `nodes[].engine.connect_addr`

Address remote components such as `rpc-proxy` and `clusterstat` use to connect
to the engine.

### `nodes[].engine.command`

Executable and arguments used by `repl` to start the engine. The command is
executed directly, without a shell. Network arguments in this command must
make the engine reachable through both configured engine addresses; `repl`
does not parse or rewrite them.

### Engine process settings

- `restart_delay` is the delay before restarting an exited engine.
- `shutdown_timeout` is how long `repl` waits after `SIGTERM` before killing
  the engine.

Both values use Go duration syntax and must be positive.
