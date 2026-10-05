# syntax=docker/dockerfile:1

FROM golang:1.25.0-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY api ./api
COPY clusterstat ./clusterstat
COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build -trimpath -o /out/repl ./cmd/repl \
    && CGO_ENABLED=0 go build -trimpath -o /out/kv-engine ./cmd/kv-engine \
    && CGO_ENABLED=0 go build -trimpath -o /out/clusterstat ./clusterstat/cmd/clusterstat

FROM alpine:3.22 AS runtime-base

RUN addgroup -S app && adduser -S -G app app

COPY --from=build /out/repl /usr/local/bin/repl
COPY --from=build /out/kv-engine /usr/local/bin/kv-engine
COPY --from=build /out/clusterstat /usr/local/bin/clusterstat

USER app

EXPOSE 7001 8001 8080

STOPSIGNAL SIGTERM

FROM runtime-base AS node

ENTRYPOINT ["/usr/local/bin/repl"]

FROM runtime-base AS clusterstat

ENTRYPOINT ["/usr/local/bin/clusterstat"]
