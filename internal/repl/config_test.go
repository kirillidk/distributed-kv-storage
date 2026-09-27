package repl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validManifest = `version: 1
shards:
  - id: shard_0
    repl_address: 127.0.0.1:7001
    engine:
      address: 127.0.0.1:8001
      command: [./bin/kv-engine, --listen, 127.0.0.1:8001]
      restart_delay: 1s
      shutdown_timeout: 5s
`

func writeManifest(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadManifestAndFindShard(t *testing.T) {
	manifest, err := LoadManifest(writeManifest(t, validManifest))
	if err != nil {
		t.Fatal(err)
	}
	shard, err := manifest.FindShard("shard_0")
	if err != nil {
		t.Fatal(err)
	}
	if shard.Engine.RestartDelay.Duration != time.Second || shard.Engine.ShutdownTimeout.Duration != 5*time.Second {
		t.Fatalf("unexpected engine durations: %+v", shard.Engine)
	}
	if len(shard.Engine.Command) != 3 || shard.Engine.Command[1] != "--listen" {
		t.Fatalf("unexpected command: %v", shard.Engine.Command)
	}
	if _, err := manifest.FindShard("missing"); err == nil || !strings.Contains(err.Error(), `shard "missing" not found`) {
		t.Fatalf("expected missing shard error, got %v", err)
	}
}

func TestLoadManifestErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"invalid YAML", "version: [", "parse manifest"},
		{"duplicate ID", validManifest + `  - id: shard_0
    repl_address: 127.0.0.1:7002
    engine:
      address: 127.0.0.1:8002
      command: [kv-engine]
      restart_delay: 1s
      shutdown_timeout: 5s
`, `duplicate shard ID "shard_0"`},
		{"missing command", strings.Replace(validManifest, "command: [./bin/kv-engine, --listen, 127.0.0.1:8001]", "command: []", 1), "engine.command"},
		{"invalid duration", strings.Replace(validManifest, "restart_delay: 1s", "restart_delay: soon", 1), "invalid duration"},
		{"missing address", strings.Replace(validManifest, "address: 127.0.0.1:8001", "address: ''", 1), "engine.address"},
		{"unknown field", validManifest + "extra: true\n", "field extra not found"},
		{"extra document", validManifest + "---\nversion: 1\n", "multiple YAML documents"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadManifest(writeManifest(t, test.content))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected error containing %q, got %v", test.want, err)
			}
		})
	}
	if _, err := LoadManifest(filepath.Join(t.TempDir(), "missing.yaml")); err == nil || !strings.Contains(err.Error(), "open manifest") {
		t.Fatalf("expected file error, got %v", err)
	}
}
