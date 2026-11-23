package manifest

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalReplicas(t *testing.T) {
	m, err := Load(filepath.Join("..", "..", "examples", "two-shards", "cluster.yaml"))
	require.NoError(t, err)
	first := m.LocalReplicas("node-1")
	require.Len(t, first, 2)
	require.Equal(t, "shard-a", first[0].Shard.ShardID)
	require.Equal(t, uint32(2), first[0].Shard.Count)
	require.Equal(t, uint32(0), first[0].Shard.Index)
	require.Equal(t, "node-2", first[0].Peers[0].ID)
	require.Equal(t, "127.0.0.1:8001", first[0].Engine.ConnectAddr)
	require.Equal(t, time.Second, first[0].Engine.RestartDelay.Duration)
	require.Equal(t, 5*time.Second, first[0].Engine.ShutdownTimeout.Duration)
	require.Equal(t, "shard-b", first[1].Shard.ShardID)
	require.Empty(t, first[1].Peers)
	second := m.LocalReplicas("node-2")
	require.Len(t, second, 1)
	require.Equal(t, first[0].Shard, second[0].Shard)
	require.Equal(t, "node-1", second[0].Peers[0].ID)
	slices.Reverse(m.Nodes)
	slices.Reverse(m.Shards)
	slices.Reverse(m.Shards[1].Replicas)
	require.Equal(t, first, m.LocalReplicas("node-1"))
	require.Equal(t, second, m.LocalReplicas("node-2"))
	// Different machines may use exactly the same local path.
	m.Shards[1].Replicas[0].DataDir = m.Shards[1].Replicas[1].DataDir
	require.NoError(t, m.Validate())
	_, err = m.FindNode("missing")
	require.ErrorContains(t, err, `node "missing" not found`)
}

func TestInvalidPlacement(t *testing.T) {
	for _, tc := range []struct {
		want   string
		change func(*Manifest)
	}{
		{"version", func(m *Manifest) { m.Version = 2 }},
		{"cluster_id", func(m *Manifest) { m.ClusterID = " " }},
		{"contains no nodes", func(m *Manifest) { m.Nodes = nil }},
		{"shards", func(m *Manifest) { m.Shards = nil }},
		{"duplicate node ID", func(m *Manifest) { m.Nodes[1].ID = m.Nodes[0].ID }},
		{"duplicate shard ID", func(m *Manifest) { m.Shards[1].ID = m.Shards[0].ID }},
		{"has no replicas", func(m *Manifest) { m.Shards[0].Replicas = nil }},
		{"duplicate node_id", func(m *Manifest) { m.Shards[0].Replicas[1].NodeID = "node-1" }},
		{"unknown node", func(m *Manifest) { m.Shards[1].Replicas[0].NodeID = "missing" }},
		{"data_dir is required", func(m *Manifest) { m.Shards[0].Replicas[0].DataDir = "" }},
		{"share data_dir", func(m *Manifest) { m.Shards[1].Replicas[0].DataDir = m.Shards[0].Replicas[0].DataDir + "/." }},
		{"share connect address", func(m *Manifest) {
			m.Shards[1].Replicas[0].Engine.ConnectAddr = m.Shards[0].Replicas[0].Engine.ConnectAddr
		}},
		{"cannot be used", func(m *Manifest) { m.Nodes[0].Repl.ConnectAddr = "0.0.0.0:7001" }},
		{"cannot be used", func(m *Manifest) { m.Shards[0].Replicas[0].Engine.ConnectAddr = "0.0.0.0:8001" }},
		{"engine.command", func(m *Manifest) { m.Shards[0].Replicas[0].Engine.Command = nil }},
		{"must be positive", func(m *Manifest) { m.Shards[0].Replicas[0].Engine.ShutdownTimeout.Duration = 0 }},
	} {
		t.Run(tc.want, func(t *testing.T) {
			m, err := Load(filepath.Join("..", "..", "examples", "two-shards", "cluster.yaml"))
			require.NoError(t, err)
			tc.change(m)
			require.ErrorContains(t, m.Validate(), tc.want)
		})
	}
}

func TestManifestDecoding(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "single-node", "cluster.yaml"))
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	_, err = Load(path)
	require.ErrorContains(t, err, "open manifest")
	for _, tc := range []struct{ content, want string }{
		{"nodes: [", "parse manifest"},
		{strings.Replace(string(data), "version: 1", "version: 1.5", 1), "must be an integer"},
		{strings.Replace(string(data), "restart_delay: 3s", "restart_delay: soon", 1), "invalid duration"},
		{string(data) + "extra: true\n", "field extra not found"},
		{string(data) + "---\nnodes: []\n", "multiple YAML documents"},
		{strings.Replace(string(data), "    repl:", "    engine: {}\n    repl:", 1), "field engine not found"},
	} {
		require.NoError(t, os.WriteFile(path, []byte(tc.content), 0600))
		_, err := Load(path)
		require.ErrorContains(t, err, tc.want)
	}
}
