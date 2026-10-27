package main

import (
	"flag"
	"log"
	"os"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	kvengine "github.com/kirillidk/distributed-kv-storage/internal/kv-engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	port     = flag.Int("port", 50052, "port number")
	replAddr = flag.String("repl-addr", os.Getenv("REPL_CONNECT_ADDR"), "local repl address for durable proposals")
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
	server := kvengine.CreateServerWithProposer(kvengine.CreateMemoryStorage(), proposer)
	server.RunServer(*port)
}
