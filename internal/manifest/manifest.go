// Package manifest describes the static placement of shard replicas on repl nodes.
package manifest

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kirillidk/distributed-kv-storage/api/sharding"
	"gopkg.in/yaml.v3"
)

type Manifest struct {
	Version   Version `yaml:"version"`
	ClusterID string  `yaml:"cluster_id"`
	Nodes     []Node  `yaml:"nodes"`
	Shards    []Shard `yaml:"shards"`
}

type Version uint32

func (v *Version) UnmarshalYAML(node *yaml.Node) error {
	if node.ShortTag() != "!!int" {
		return fmt.Errorf("line %d: version must be an integer", node.Line)
	}
	return node.Decode((*uint32)(v))
}

type Node struct {
	ID   string `yaml:"id"`
	Repl Repl   `yaml:"repl"`
}

type Repl struct {
	ListenAddr  string `yaml:"listen_addr"`
	ConnectAddr string `yaml:"connect_addr"`
	LogDir      string `yaml:"log_dir"`
}

type Shard struct {
	ID       string    `yaml:"id"`
	Replicas []Replica `yaml:"replicas"`
}

type Replica struct {
	NodeID  string `yaml:"node_id"`
	DataDir string `yaml:"data_dir"`
	Engine  Engine `yaml:"engine"`
}

type Engine struct {
	ConnectAddr     string   `yaml:"connect_addr"`
	Command         []string `yaml:"command"`
	RestartDelay    Duration `yaml:"restart_delay"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

// Duration accepts Go duration strings such as "1s" and "500ms" in YAML.
type Duration struct {
	time.Duration
}

func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
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

func Load(path string) (*Manifest, error) {
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
	if m.Version != 1 {
		return fmt.Errorf("expected manifest version 1, got %d", m.Version)
	}
	if strings.TrimSpace(m.ClusterID) == "" {
		return errors.New("cluster_id is required")
	}
	if len(m.Nodes) == 0 {
		return errors.New("manifest contains no nodes")
	}
	if len(m.Shards) == 0 || uint64(len(m.Shards)) > math.MaxUint32 {
		return errors.New("manifest requires between 1 and 2^32-1 shards")
	}
	// Data directories are local to a physical node. Advertised addresses are cluster-wide.
	dirs := make(map[string]map[string]string, len(m.Nodes))
	addresses := make(map[string]string)
	for i, n := range m.Nodes {
		label := fmt.Sprintf("nodes[%d]", i)
		if strings.TrimSpace(n.ID) == "" {
			return fmt.Errorf("%s.id is required", label)
		}
		if _, exists := dirs[n.ID]; exists {
			return fmt.Errorf("duplicate node ID %q", n.ID)
		}
		dirs[n.ID] = make(map[string]string)
		if err := validateAddress(n.Repl.ListenAddr, true); err != nil {
			return fmt.Errorf("%s.repl.listen_addr: %w", label, err)
		}
		if err := claimAddress(addresses, label+".repl.connect_addr", n.Repl.ConnectAddr); err != nil {
			return err
		}
	}
	shards := make(map[string]bool, len(m.Shards))
	for i, shard := range m.Shards {
		if strings.TrimSpace(shard.ID) == "" {
			return fmt.Errorf("shards[%d].id is required", i)
		}
		if shards[shard.ID] {
			return fmt.Errorf("duplicate shard ID %q", shard.ID)
		}
		shards[shard.ID] = true
		if len(shard.Replicas) == 0 {
			return fmt.Errorf("shard %q has no replicas", shard.ID)
		}
		members := make(map[string]bool, len(shard.Replicas))
		for j, replica := range shard.Replicas {
			label := fmt.Sprintf("shards[%d].replicas[%d]", i, j)
			localDirs, ok := dirs[replica.NodeID]
			if !ok {
				return fmt.Errorf("%s.node_id references unknown node %q", label, replica.NodeID)
			}
			if members[replica.NodeID] {
				return fmt.Errorf("duplicate node_id %q in shard %q", replica.NodeID, shard.ID)
			}
			members[replica.NodeID] = true
			if strings.TrimSpace(replica.DataDir) == "" {
				return fmt.Errorf("%s.data_dir is required", label)
			}
			dir, err := filepath.Abs(replica.DataDir)
			if err != nil {
				return fmt.Errorf("%s.data_dir: %w", label, err)
			}
			if other, exists := localDirs[dir]; exists {
				return fmt.Errorf("shards %q and %q share data_dir %q on node %q", other, shard.ID, dir, replica.NodeID)
			}
			localDirs[dir] = shard.ID
			if err := claimAddress(addresses, label+".engine.connect_addr", replica.Engine.ConnectAddr); err != nil {
				return err
			}
			if len(replica.Engine.Command) == 0 || strings.TrimSpace(replica.Engine.Command[0]) == "" {
				return fmt.Errorf("%s.engine.command must start with an executable", label)
			}
			if replica.Engine.RestartDelay.Duration <= 0 || replica.Engine.ShutdownTimeout.Duration <= 0 {
				return fmt.Errorf("%s.engine restart_delay and shutdown_timeout must be positive", label)
			}
		}
	}
	return nil
}

func claimAddress(seen map[string]string, label, addr string) error {
	if err := validateAddress(addr, false); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if previous, ok := seen[addr]; ok {
		return fmt.Errorf("%s and %s share connect address %q", previous, label, addr)
	}
	seen[addr] = label
	return nil
}

// SortedShards maps a numeric hash slot to its stable logical shard name.
func (m *Manifest) SortedShards() []Shard {
	shards := slices.Clone(m.Shards)
	slices.SortFunc(shards, func(a, b Shard) int { return cmp.Compare(a.ID, b.ID) })
	return shards
}

type LocalReplica struct {
	Replica
	Shard sharding.Assignment
	Peers []Node
}

// LocalReplicas selects the engines managed by one repl and their remote voters.
func (m *Manifest) LocalReplicas(nodeID string) []LocalReplica {
	nodes := make(map[string]Node, len(m.Nodes))
	for _, node := range m.Nodes {
		nodes[node.ID] = node
	}
	var local []LocalReplica
	for index, shard := range m.SortedShards() {
		i := slices.IndexFunc(shard.Replicas, func(r Replica) bool { return r.NodeID == nodeID })
		if i < 0 {
			continue
		}
		replica := LocalReplica{
			Replica: shard.Replicas[i],
			Shard: sharding.Assignment{
				ClusterID: m.ClusterID,
				ShardID:   shard.ID,
				Index:     uint32(index),
				Count:     uint32(len(m.Shards)),
			},
		}
		for _, member := range shard.Replicas {
			if member.NodeID != nodeID {
				replica.Peers = append(replica.Peers, nodes[member.NodeID])
			}
		}
		local = append(local, replica)
	}
	return local
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

func (m *Manifest) FindNode(id string) (*Node, error) {
	for i := range m.Nodes {
		if m.Nodes[i].ID == id {
			return &m.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in manifest", id)
}
