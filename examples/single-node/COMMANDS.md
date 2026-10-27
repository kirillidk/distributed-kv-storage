# Single-node commands

## Build and start

```bash
docker compose -f examples/single-node/compose.yaml \
  up --build --force-recreate -d
```

## Rebuild node

```bash
docker compose -f examples/single-node/compose.yaml \
  up --build -d node-1
```

## Reset all data (deletes logs and WAL), rebuild and start

```bash
docker compose -f examples/single-node/compose.yaml down --volumes
docker compose -f examples/single-node/compose.yaml up --build -d
```

## Services

```bash
docker compose -f examples/single-node/compose.yaml ps
```

## All logs

```bash
docker compose -f examples/single-node/compose.yaml logs -f
```

## Node logs

```bash
docker compose -f examples/single-node/compose.yaml logs -f node-1
```

## Clusterstat logs

```bash
docker compose -f examples/single-node/compose.yaml logs -f clusterstat
```

## Repl log file

```bash
docker compose -f examples/single-node/compose.yaml \
  exec node-1 tail -f /var/log/distributed-kv/node-1/repl.log
```

## Engine log file

```bash
docker compose -f examples/single-node/compose.yaml \
  exec node-1 tail -f /var/log/distributed-kv/node-1/kv-engine.log
```

## Clear component log files

```bash
docker compose -f examples/single-node/compose.yaml \
  exec node-1 truncate -s 0 \
  /var/log/distributed-kv/node-1/repl.log \
  /var/log/distributed-kv/node-1/kv-engine.log
```

## Restart engine with SIGTERM

```bash
docker compose -f examples/single-node/compose.yaml \
  exec node-1 killall -TERM kv-engine
```

## Dashboard

```text
http://127.0.0.1:8080
```

## Repl logs in browser (refresh manually)

```text
http://127.0.0.1:8080/components/node-1/repl/logs
```

## Engine logs in browser (refresh manually)

```text
http://127.0.0.1:8080/components/node-1/kv-engine/logs
```

## API

```bash
curl http://127.0.0.1:8080/api/v1/components
```

```bash
curl http://127.0.0.1:8080/healthz
```

## Repl logs API

```bash
curl http://127.0.0.1:8080/api/v1/components/node-1/repl/logs
```

## Engine logs API

```bash
curl http://127.0.0.1:8080/api/v1/components/node-1/kv-engine/logs
```

## Container shell

```bash
docker compose -f examples/single-node/compose.yaml exec node-1 sh
```

```sh
cat /etc/distributed-kv/cluster.yaml
```

## Processes

```sh
ps
```

## kvctl

```bash
docker compose -f examples/single-node/compose.yaml \
  exec node-1 kvctl --address 127.0.0.1:8001
```

## Set

```text
set name distributed-kv 0
```

## Get

```text
get name
```

## Stop

```bash
docker compose -f examples/single-node/compose.yaml down
```
