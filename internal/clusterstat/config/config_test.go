package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadManifest(t *testing.T) {
	manifestPath := filepath.Join("..", "..", "..", "examples", "single-node", "cluster.yaml")
	cfg, err := LoadArgs([]string{
		"--manifest", manifestPath,
		"--listen", "0.0.0.0:8080",
		"--poll-interval", "500ms",
		"--check-timeout", "250ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "0.0.0.0:8080" || cfg.PollInterval != 500*time.Millisecond || cfg.CheckTimeout != 250*time.Millisecond {
		t.Fatalf("unexpected runtime config: %+v", cfg)
	}
	if len(cfg.Components) != 2 {
		t.Fatalf("components count = %d, want 2", len(cfg.Components))
	}
	want := []Component{
		{NodeID: "node-1", Type: TypeRepl, Address: "node-1:7001"},
		{NodeID: "node-1", Type: TypeKVEngine, Address: "node-1:8001"},
	}
	for i := range want {
		if cfg.Components[i] != want[i] {
			t.Fatalf("components[%d] = %+v, want %+v", i, cfg.Components[i], want[i])
		}
	}
}

func TestLoadErrors(t *testing.T) {
	writeManifest := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "cluster.yaml")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "invalid poll interval", args: []string{"--poll-interval", "0s"}, want: "poll-interval must be positive"},
		{name: "unexpected argument", args: []string{"extra"}, want: "unexpected arguments"},
		{name: "unknown manifest field", args: []string{"--manifest", writeManifest(t, "nodes: []\nunknown: true\n")}, want: "field unknown not found"},
		{name: "empty nodes", args: []string{"--manifest", writeManifest(t, "nodes: []\n")}, want: "manifest contains no nodes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadArgs(test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}
