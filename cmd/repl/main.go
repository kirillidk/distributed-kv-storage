package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kirillidk/distributed-kv-storage/internal/repl"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], logger); err != nil {
		logger.Error("repl failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("repl", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	manifestPath := flags.String("manifest", "", "path to the cluster YAML manifest")
	nodeID := flags.String("node-id", "", "ID of the local node")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" || *nodeID == "" || flags.NArg() != 0 {
		return errors.New("usage: repl --manifest PATH --node-id ID")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	manifest, err := repl.LoadManifest(*manifestPath)
	if err != nil {
		return err
	}
	node, err := manifest.FindNode(*nodeID)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	logger.Info("repl starting", "node_id", node.ID, "repl_address", node.ReplAddress)
	if err := repl.Run(ctx, *node, logger); err != nil {
		return fmt.Errorf("supervise node %q: %w", node.ID, err)
	}
	logger.Info("repl stopped", "node_id", node.ID)
	return nil
}
