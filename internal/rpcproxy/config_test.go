package rpcproxy

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig(strings.NewReader(`
engines:
  - name: primary
    address: 127.0.0.1:50051
  - name: replica
    address: 127.0.0.1:50052
`))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if len(cfg.Engines) != 2 {
		t.Fatalf("len(cfg.Engines) = %d, want 2", len(cfg.Engines))
	}
	if got := cfg.Engines[0]; got.Name != "primary" || got.Address != "127.0.0.1:50051" {
		t.Errorf("cfg.Engines[0] = %#v, want primary at 127.0.0.1:50051", got)
	}
}

func TestLoadConfigRejectsInvalidManifest(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		manifest string
	}{
		{
			name:     "no engines",
			manifest: "engines: []",
		},
		{
			name: "missing name",
			manifest: `
engines:
  - address: 127.0.0.1:50051
`,
		},
		{
			name: "unknown field",
			manifest: `
engines:
  - name: primary
    address: 127.0.0.1:50051
    weight: 1
`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if _, err := LoadConfig(strings.NewReader(testCase.manifest)); err == nil {
				t.Fatal("LoadConfig() error = nil, want an error")
			}
		})
	}
}
