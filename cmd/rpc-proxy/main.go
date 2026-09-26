package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/rpcproxy"
	"google.golang.org/grpc"
)

func main() {
	manifestPath := flag.String("manifest", "manifest.yaml", "path to the cluster manifest")
	listenAddress := flag.String("listen", ":50050", "gRPC listen address")
	flag.Parse()

	cfg, err := loadConfig(*manifestPath)
	if err != nil {
		log.Fatal(err)
	}

	proxy, err := rpcproxy.New(context.Background(), cfg)
	if err != nil {
		log.Fatalf("create RPC proxy: %v", err)
	}
	defer func() {
		if err := proxy.Close(); err != nil {
			log.Printf("close engine connections: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen on %q: %v", *listenAddress, err)
	}

	server := grpc.NewServer()
	kvv1.RegisterKVServiceServer(server, proxy)

	go func() {
		log.Printf("RPC proxy listening on %s; %d engine(s) configured", listener.Addr(), len(cfg.Engines))
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve gRPC: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	signal.Stop(stop)
	server.GracefulStop()
}

func loadConfig(path string) (rpcproxy.Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return rpcproxy.Config{}, fmt.Errorf("open manifest %q: %w", path, err)
	}
	defer file.Close()

	cfg, err := rpcproxy.LoadConfig(file)
	if err != nil {
		return rpcproxy.Config{}, fmt.Errorf("load manifest %q: %w", path, err)
	}
	return cfg, nil
}
