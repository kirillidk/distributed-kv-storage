package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	kvengine "github.com/kirillidk/distributed-kv-storage/internal/kv-engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	port        = flag.Int("port", 50052, "port number")
	replAddr    = flag.String("repl-addr", os.Getenv("REPL_CONNECT_ADDR"), "local repl address for durable proposals")
	metricsAddr = flag.String("metrics-listen", ":2112", "metrics server address")
)

func main() {
	flag.Parse()
	var proposer replv1.ReplServiceClient
	if *replAddr != "" {
		conn, err := grpc.NewClient(*replAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		proposer = replv1.NewReplServiceClient(conn)
	}
	server, err := kvengine.CreateShardServer(kvengine.CreateMemoryStorage(), proposer,
		shardSetting("KV_SHARD_ID", 0), shardSetting("KV_SHARD_COUNT", 1))
	if err != nil {
		log.Fatal(err)
	}
	server.RunServer(*port, *metricsAddr)
}

func shardSetting(name string, fallback uint32) uint32 {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		log.Fatalf("invalid %s: %v", name, err)
	}
	return uint32(n)
}
