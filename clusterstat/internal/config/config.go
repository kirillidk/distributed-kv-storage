package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Node struct {
	ID       string `yaml:"id"`
	KVEngine string `yaml:"kv_engine"`
	Repl     string `yaml:"repl"`
}

type Manifest struct {
	Nodes    []Node `yaml:"nodes"`
	RPCProxy string `yaml:"rpc_proxy"`
}

type ComponentType string

const (
	TypeKVEngine ComponentType = "kv-engine"
	TypeRepl     ComponentType = "repl"
	TypeRPCProxy ComponentType = "rpc-proxy"
)

type Component struct {
	NodeID  string
	Type    ComponentType
	Address string
}

type Config struct {
	ManifestPath string
	Listen       string
	PollInterval time.Duration
	CheckTimeout time.Duration
	Components   []Component
}

func Load() (*Config, error) {
	cfg := &Config{}

	flag.StringVar(&cfg.ManifestPath, "manifest", "./cluster.yaml", "path to cluster yaml manifest")
	flag.StringVar(&cfg.Listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", 2*time.Second, "how often to poll health")
	flag.DurationVar(&cfg.CheckTimeout, "check-timeout", 1*time.Second, "timeout for one health check")
	flag.Parse()

	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("poll-interval must be positive, got %s", cfg.PollInterval)
	}
	if cfg.CheckTimeout <= 0 {
		return nil, fmt.Errorf("check-timeout must be positive, got %s", cfg.CheckTimeout)
	}

	data, err := os.ReadFile((cfg.ManifestPath))
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", cfg.ManifestPath, err)
	}

	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}

	for _, n := range m.Nodes {
		if n.KVEngine != "" {
			cfg.Components = append(cfg.Components, Component{
				NodeID:  n.ID,
				Type:    TypeKVEngine,
				Address: n.KVEngine,
			})
		}

		if n.Repl != "" {
			cfg.Components = append(cfg.Components, Component{
				NodeID:  n.ID,
				Type:    TypeRepl,
				Address: n.Repl,
			})
		}
	}

	if m.RPCProxy != "" {
		cfg.Components = append(cfg.Components, Component{
			NodeID:  "",
			Type:    TypeRPCProxy,
			Address: m.RPCProxy,
		})
	}

	if len(cfg.Components) == 0 {
		return nil, fmt.Errorf("manifest contains no components to monitor")
	}

	return cfg, nil
}
