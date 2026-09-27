package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRejectsInvalidArguments(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(nil, logger); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("expected usage error, got %v", err)
	}
	if err := run([]string{"--manifest", "missing.yaml", "--shard-id", "shard_0"}, logger); err == nil || !strings.Contains(err.Error(), "open manifest") {
		t.Fatalf("expected missing manifest error, got %v", err)
	}
}

func TestRunRejectsUnknownShard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	data := `version: 1
shards:
  - id: shard_0
    repl_address: 127.0.0.1:7001
    engine:
      address: 127.0.0.1:8001
      command: [kv-engine]
      restart_delay: 1s
      shutdown_timeout: 5s
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run([]string{"--manifest", path, "--shard-id", "missing"}, logger); err == nil || !strings.Contains(err.Error(), `shard "missing" not found`) {
		t.Fatalf("expected missing shard error, got %v", err)
	}
}
