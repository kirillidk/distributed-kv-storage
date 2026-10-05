package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type manifest struct {
	Nodes []nodeConfig `yaml:"nodes"`
}

type nodeConfig struct {
	ID     string       `yaml:"id"`
	Repl   replConfig   `yaml:"repl"`
	Engine engineConfig `yaml:"engine"`
}

type replConfig struct {
	ListenAddr  string `yaml:"listen_addr"`
	ConnectAddr string `yaml:"connect_addr"`
}

type engineConfig struct {
	ConnectAddr     string   `yaml:"connect_addr"`
	Command         []string `yaml:"command"`
	RestartDelay    string   `yaml:"restart_delay"`
	ShutdownTimeout string   `yaml:"shutdown_timeout"`
}

type ComponentType string

const (
	TypeKVEngine ComponentType = "kv-engine"
	TypeRepl     ComponentType = "repl"
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
	return LoadArgs(os.Args[1:])
}

func LoadArgs(args []string) (*Config, error) {
	cfg := &Config{}

	flags := flag.NewFlagSet("clusterstat", flag.ContinueOnError)
	flags.StringVar(&cfg.ManifestPath, "manifest", "./cluster.yaml", "path to cluster yaml manifest")
	flags.StringVar(&cfg.Listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flags.DurationVar(&cfg.PollInterval, "poll-interval", 2*time.Second, "how often to poll health")
	flags.DurationVar(&cfg.CheckTimeout, "check-timeout", 1*time.Second, "timeout for one health check")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}

	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("poll-interval must be positive, got %s", cfg.PollInterval)
	}
	if cfg.CheckTimeout <= 0 {
		return nil, fmt.Errorf("check-timeout must be positive, got %s", cfg.CheckTimeout)
	}

	file, err := os.Open(cfg.ManifestPath)
	if err != nil {
		return nil, fmt.Errorf("open manifest %q: %w", cfg.ManifestPath, err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var m manifest
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("parse manifest %q: %w", cfg.ManifestPath, err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("parse manifest %q: multiple YAML documents are not supported", cfg.ManifestPath)
		}
		return nil, fmt.Errorf("parse manifest %q: %w", cfg.ManifestPath, err)
	}
	if len(m.Nodes) == 0 {
		return nil, fmt.Errorf("manifest contains no nodes")
	}

	seenIDs := make(map[string]struct{}, len(m.Nodes))
	for i, n := range m.Nodes {
		if strings.TrimSpace(n.ID) == "" {
			return nil, fmt.Errorf("nodes[%d].id is required", i)
		}
		if _, exists := seenIDs[n.ID]; exists {
			return nil, fmt.Errorf("duplicate node ID %q", n.ID)
		}
		seenIDs[n.ID] = struct{}{}
		if strings.TrimSpace(n.Repl.ConnectAddr) == "" {
			return nil, fmt.Errorf("nodes[%d].repl.connect_addr is required", i)
		}
		if strings.TrimSpace(n.Engine.ConnectAddr) == "" {
			return nil, fmt.Errorf("nodes[%d].engine.connect_addr is required", i)
		}
		cfg.Components = append(cfg.Components,
			Component{NodeID: n.ID, Type: TypeRepl, Address: n.Repl.ConnectAddr},
			Component{NodeID: n.ID, Type: TypeKVEngine, Address: n.Engine.ConnectAddr},
		)
	}

	return cfg, nil
}
