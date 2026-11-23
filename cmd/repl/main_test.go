package main

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRejectsInvalidArguments(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(nil, logger); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("expected usage error, got %v", err)
	}
	if err := run([]string{"--manifest", "missing.yaml", "--node-id", "node_0"}, logger); err == nil || !strings.Contains(err.Error(), "open manifest") {
		t.Fatalf("expected missing manifest error, got %v", err)
	}
}

func TestRunRejectsUnknownNode(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "single-node", "cluster.yaml")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run([]string{"--manifest", path, "--node-id", "missing"}, logger); err == nil || !strings.Contains(err.Error(), `node "missing" not found`) {
		t.Fatalf("expected missing node error, got %v", err)
	}
}
