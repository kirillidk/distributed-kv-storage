# Single-node commands

## Build and start

```bash
docker compose -f examples/single-node/compose.yaml \
  up --build --force-recreate -d
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

## Dashboard

```text
http://127.0.0.1:8080
```

## API

```bash
curl http://127.0.0.1:8080/api/v1/components
```

```bash
curl http://127.0.0.1:8080/healthz
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

## Stop

```bash
docker compose -f examples/single-node/compose.yaml down
```
