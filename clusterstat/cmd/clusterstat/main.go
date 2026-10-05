package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kirillidk/distributed-kv-storage/clusterstat/internal/collector"
	"github.com/kirillidk/distributed-kv-storage/clusterstat/internal/config"
	"github.com/kirillidk/distributed-kv-storage/clusterstat/internal/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("clusterstat ")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("monitoring %d component(s), poll=%s timeout=%s",
		len(cfg.Components), cfg.PollInterval, cfg.CheckTimeout)

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	col := collector.New(cfg.Components, cfg.PollInterval, cfg.CheckTimeout)

	collDone := make(chan struct{})
	go func() {
		defer close(collDone)
		col.Run(ctx)
	}()

	srv := server.New(cfg.Listen, col)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	failed := false
	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			log.Printf("http server: %v", err)
			failed = true
			stop()
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	cancel()

	<-collDone

	log.Printf("bye-bye")
	if failed {
		os.Exit(1)
	}
}
