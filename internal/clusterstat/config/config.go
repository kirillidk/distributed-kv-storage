package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kirillidk/distributed-kv-storage/internal/manifest"
)

type ComponentType string

const (
	TypeKVEngine ComponentType = "kv-engine"
	TypeRepl     ComponentType = "repl"
)

type Component struct {
	ShardID string
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

	topology, err := manifest.Load(cfg.ManifestPath)
	if err != nil {
		return nil, err
	}
	for _, node := range topology.Nodes {
		cfg.Components = append(cfg.Components, Component{NodeID: node.ID, Type: TypeRepl, Address: node.Repl.ConnectAddr})
	}
	for _, shard := range topology.Shards {
		for _, replica := range shard.Replicas {
			cfg.Components = append(cfg.Components, Component{NodeID: replica.NodeID, ShardID: shard.ID, Type: TypeKVEngine, Address: replica.Engine.ConnectAddr})
		}
	}

	return cfg, nil
}
