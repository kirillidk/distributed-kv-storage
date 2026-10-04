package repl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validManifest = `nodes:
  - id: node_0
    repl:
      listen_addr: 0.0.0.0:7001
      connect_addr: node-0:7001
    engine:
      local_addr: 127.0.0.1:8001
      connect_addr: node-0:8001
      command: [./bin/kv-engine, --port, "8001"]
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

func TestLoadManifestAndFindNode(t *testing.T) {
	manifest, err := LoadManifest(writeManifest(t, validManifest))
	if err != nil {
		t.Fatal(err)
	}
	node, err := manifest.FindNode("node_0")
	if err != nil {
		t.Fatal(err)
	}
	if node.Engine.RestartDelay.Duration != time.Second || node.Engine.ShutdownTimeout.Duration != 5*time.Second {
		t.Fatalf("unexpected engine durations: %+v", node.Engine)
	}
	if node.Repl.ListenAddr != "0.0.0.0:7001" || node.Repl.ConnectAddr != "node-0:7001" {
		t.Fatalf("unexpected repl addresses: %+v", node.Repl)
	}
	if node.Engine.LocalAddr != "127.0.0.1:8001" || node.Engine.ConnectAddr != "node-0:8001" {
		t.Fatalf("unexpected engine addresses: %+v", node.Engine)
	}
	if len(node.Engine.Command) != 3 || node.Engine.Command[1] != "--port" || node.Engine.Command[2] != "8001" {
		t.Fatalf("unexpected command: %v", node.Engine.Command)
	}
	if _, err := manifest.FindNode("missing"); err == nil || !strings.Contains(err.Error(), `node "missing" not found`) {
		t.Fatalf("expected missing node error, got %v", err)
	}
}

func TestCanonicalExampleManifest(t *testing.T) {
	manifest, err := LoadManifest(filepath.Join("..", "..", "examples", "single-node", "cluster.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Nodes) != 1 || manifest.Nodes[0].ID != "node-1" {
		t.Fatalf("unexpected canonical manifest: %+v", manifest)
	}
}

func TestLoadManifestErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"invalid YAML", "nodes: [", "parse manifest"},
		{"duplicate ID", validManifest + `  - id: node_0
    repl:
      listen_addr: 0.0.0.0:7002
      connect_addr: node-2:7002
    engine:
      local_addr: 127.0.0.1:8002
      connect_addr: node-2:8002
      command: [kv-engine]
      restart_delay: 1s
      shutdown_timeout: 5s
`, `duplicate node ID "node_0"`},
		{"missing command", strings.Replace(validManifest, "command: [./bin/kv-engine, --port, \"8001\"]", "command: []", 1), "engine.command"},
		{"invalid duration", strings.Replace(validManifest, "restart_delay: 1s", "restart_delay: soon", 1), "invalid duration"},
		{"missing local address", strings.Replace(validManifest, "local_addr: 127.0.0.1:8001", "local_addr: ''", 1), "engine.local_addr"},
		{"wildcard repl connect address", strings.Replace(validManifest, "connect_addr: node-0:7001", "connect_addr: 0.0.0.0:7001", 1), "cannot be used as a connection address"},
		{"wildcard engine connect address", strings.Replace(validManifest, "connect_addr: node-0:8001", "connect_addr: 0.0.0.0:8001", 1), "cannot be used as a connection address"},
		{"version field", "version: 1\n" + validManifest, "field version not found"},
		{"unknown field", validManifest + "extra: true\n", "field extra not found"},
		{"extra document", validManifest + "---\nnodes: []\n", "multiple YAML documents"},
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
