package repl

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Manifest describes every node in the cluster. The whole manifest is
// validated before a local node is selected.
type Manifest struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID     string       `yaml:"id"`
	Repl   ReplConfig   `yaml:"repl"`
	Engine EngineConfig `yaml:"engine"`
}

type ReplConfig struct {
	ListenAddr  string `yaml:"listen_addr"`
	ConnectAddr string `yaml:"connect_addr"`
}

type EngineConfig struct {
	LocalAddr       string   `yaml:"local_addr"`
	ConnectAddr     string   `yaml:"connect_addr"`
	Command         []string `yaml:"command"`
	RestartDelay    Duration `yaml:"restart_delay"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

// Duration accepts Go duration strings such as "1s" and "500ms" in YAML.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var value string
	if err := node.Decode(&value); err != nil {
		return fmt.Errorf("expected duration string: %w", err)
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value, err)
	}
	d.Duration = parsed
	return nil
}

func LoadManifest(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open manifest %q: %w", path, err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("parse manifest %q: %w", path, err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("parse manifest %q: multiple YAML documents are not supported", path)
		}
		return nil, fmt.Errorf("parse manifest %q: %w", path, err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("validate manifest %q: %w", path, err)
	}
	return &manifest, nil
}

func (m *Manifest) Validate() error {
	if len(m.Nodes) == 0 {
		return errors.New("nodes must contain at least one node")
	}

	seenIDs := make(map[string]int, len(m.Nodes))
	seenReplAddrs := make(map[string]int, len(m.Nodes))
	seenEngineAddrs := make(map[string]int, len(m.Nodes))
	for i, node := range m.Nodes {
		label := fmt.Sprintf("nodes[%d]", i)
		if strings.TrimSpace(node.ID) == "" {
			return fmt.Errorf("%s.id is required", label)
		}
		if previous, exists := seenIDs[node.ID]; exists {
			return fmt.Errorf("duplicate node ID %q at nodes[%d] and %s", node.ID, previous, label)
		}
		seenIDs[node.ID] = i
		if err := validateAddress(node.Repl.ListenAddr, true); err != nil {
			return fmt.Errorf("%s.repl.listen_addr: %w", label, err)
		}
		if err := validateAddress(node.Repl.ConnectAddr, false); err != nil {
			return fmt.Errorf("%s.repl.connect_addr: %w", label, err)
		}
		if previous, exists := seenReplAddrs[node.Repl.ConnectAddr]; exists {
			return fmt.Errorf("duplicate repl connect address %q at nodes[%d] and %s", node.Repl.ConnectAddr, previous, label)
		}
		seenReplAddrs[node.Repl.ConnectAddr] = i
		if err := validateAddress(node.Engine.LocalAddr, false); err != nil {
			return fmt.Errorf("%s.engine.local_addr: %w", label, err)
		}
		if err := validateAddress(node.Engine.ConnectAddr, false); err != nil {
			return fmt.Errorf("%s.engine.connect_addr: %w", label, err)
		}
		if previous, exists := seenEngineAddrs[node.Engine.ConnectAddr]; exists {
			return fmt.Errorf("duplicate engine connect address %q at nodes[%d] and %s", node.Engine.ConnectAddr, previous, label)
		}
		seenEngineAddrs[node.Engine.ConnectAddr] = i
		if len(node.Engine.Command) == 0 || strings.TrimSpace(node.Engine.Command[0]) == "" {
			return fmt.Errorf("%s.engine.command must start with an executable", label)
		}
		if node.Engine.RestartDelay.Duration <= 0 {
			return fmt.Errorf("%s.engine.restart_delay must be positive", label)
		}
		if node.Engine.ShutdownTimeout.Duration <= 0 {
			return fmt.Errorf("%s.engine.shutdown_timeout must be positive", label)
		}
	}
	return nil
}

func validateAddress(address string, allowUnspecified bool) error {
	if strings.TrimSpace(address) == "" {
		return errors.New("address is required")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("expected host:port, got %q", address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("expected port between 1 and 65535, got %q", portText)
	}
	if ip := net.ParseIP(host); !allowUnspecified && ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("unspecified host %q cannot be used as a connection address", host)
	}
	return nil
}

func (m *Manifest) FindNode(id string) (*NodeConfig, error) {
	for i := range m.Nodes {
		if m.Nodes[i].ID == id {
			return &m.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in manifest", id)
}
