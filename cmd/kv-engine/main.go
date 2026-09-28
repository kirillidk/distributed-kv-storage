package main

import (
	"flag"

	kvengine "github.com/kirillidk/distributed-kv-storage/internal/kv-engine"
)

var (
	port = flag.Int("port", 50052, "port number")
)

func main() {
	flag.Parse()
	server := kvengine.CreateServer(kvengine.CreateMemoryStorage())
	server.RunServer(*port)
}
