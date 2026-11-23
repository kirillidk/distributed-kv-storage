package repl

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/manifest"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// The same peer can vote in one group and replicate a different log in another.
func TestRaftRoutesOverlappingMembershipByShard(t *testing.T) {
	a, b := testRaft(t), testRaft(t)
	s := replicaServices{clusterID: "demo", replicas: map[string]*replicaRuntime{
		"shard-a": {raft: a},
		"shard-b": {raft: b},
	}}
	vote, err := s.RequestVote(t.Context(), &raftv1.RequestVoteRequest{
		ClusterId: "demo", ShardId: "shard-a", Term: 8, CandidateId: "n2",
	})
	require.NoError(t, err)
	require.True(t, vote.VoteGranted)
	assertRaftStateOnDisk(t, b, 0, "")

	payload := []byte("shard-b command")
	response, err := s.AppendEntries(t.Context(), &raftv1.AppendEntriesRequest{
		ClusterId: "demo", ShardId: "shard-b", Term: 3, LeaderId: "n2",
		Entries: []*raftv1.LogEntry{{Term: 3, Payload: payload}}, LeaderCommit: 1,
	})
	require.NoError(t, err)
	require.True(t, response.Success)
	require.Equal(t, uint64(len(payload)), b.status().CommitOffset)
	assertRaftStateOnDisk(t, a, 8, "n2")
	require.Zero(t, a.wal.LastOffset())
}

// Run the CLI with the example placement: node-1 hosts a and b, node-2 hosts a.
func TestNodeSupervisesIndependentShardReplicas(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real repl and kv-engine processes")
	}
	dir := t.TempDir()
	replBin := buildTestBinary(t, dir, "repl")
	engineBin := buildTestBinary(t, dir, "kv-engine")
	m, err := manifest.Load(filepath.Join("..", "..", "examples", "two-shards", "cluster.yaml"))
	require.NoError(t, err)
	ports := make(map[string][]net.Listener)
	reserve := func(node string) string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { l.Close() })
		ports[node] = append(ports[node], l)
		return l.Addr().String()
	}
	for i := range m.Nodes {
		n := &m.Nodes[i]
		n.Repl.ConnectAddr = reserve(n.ID)
		n.Repl.ListenAddr = n.Repl.ConnectAddr
		n.Repl.LogDir = filepath.Join(dir, n.ID, "logs")
	}
	for _, s := range m.Shards {
		for i := range s.Replicas {
			r := &s.Replicas[i]
			r.DataDir = filepath.Join(dir, r.NodeID, s.ID)
			r.Engine.ConnectAddr = reserve(r.NodeID)
			_, port, err := net.SplitHostPort(r.Engine.ConnectAddr)
			require.NoError(t, err)
			r.Engine.Command = []string{engineBin, "--port", port, "--metrics-listen", "127.0.0.1:0"}
			r.Engine.RestartDelay.Duration = 50 * time.Millisecond
		}
	}
	path := filepath.Join(dir, "cluster.yaml")
	data, err := yaml.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	start := func(n manifest.Node, wantExitCode int) func() {
		t.Helper()
		for _, l := range ports[n.ID] {
			require.NoError(t, l.Close())
		}
		delete(ports, n.ID)
		cmd := exec.Command(replBin, "--manifest", path, "--node-id", n.ID)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		output := new(lockedBuffer)
		cmd.Stdout, cmd.Stderr = output, output
		require.NoError(t, cmd.Start())
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		stop := sync.OnceFunc(func() {
			if wantExitCode == 0 {
				_ = cmd.Process.Signal(syscall.SIGTERM)
			}
			select {
			case err := <-done:
				require.Equal(t, wantExitCode, cmd.ProcessState.ExitCode(), "%v\n%s", err, output.String())
			case <-time.After(8 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
				t.Errorf("%s failed to stop: %s", n.ID, output.String())
			}
		})
		t.Cleanup(stop)
		return stop
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	n1, n2 := m.Nodes[0], m.Nodes[1]
	repl1, repl2 := testConnection(t, n1.Repl.ConnectAddr), testConnection(t, n2.Repl.ConnectAddr)
	a1Conn := testConnection(t, m.Shards[0].Replicas[0].Engine.ConnectAddr)
	a1 := kvv1.NewKVServiceClient(a1Conn)
	a2 := kvv1.NewKVServiceClient(testConnection(t, m.Shards[0].Replicas[1].Engine.ConnectAddr))
	b := kvv1.NewKVServiceClient(testConnection(t, m.Shards[1].Replicas[0].Engine.ConnectAddr))
	health1, health2 := healthpb.NewHealthClient(repl1), healthpb.NewHealthClient(repl2)
	ready := func(client healthpb.HealthClient, shard string) bool {
		check, stop := context.WithTimeout(ctx, 250*time.Millisecond)
		defer stop()
		result, err := client.Check(check, &healthpb.HealthCheckRequest{Service: "repl.v1.ReplService/" + shard})
		return err == nil && result.Status == healthpb.HealthCheckResponse_SERVING
	}
	read := func(kv kvv1.KVServiceClient, key, want string) {
		t.Helper()
		require.Eventually(t, func() bool {
			check, stop := context.WithTimeout(ctx, 250*time.Millisecond)
			defer stop()
			r, err := kv.Get(check, &kvv1.GetRequest{Key: []byte(key)})
			return err == nil && string(r.Value) == want
		}, 8*time.Second, 25*time.Millisecond, "read key %s", key)
	}
	set := func(kv kvv1.KVServiceClient, key, value string) {
		t.Helper()
		write, stop := context.WithTimeout(ctx, 3*time.Second)
		defer stop()
		_, err := kv.Set(write, &kvv1.SetRequest{Key: []byte(key), Value: []byte(value)})
		require.NoError(t, err)
	}
	stop1 := start(n1, 0)
	require.Eventually(t, func() bool { return ready(health1, "shard-b") }, 8*time.Second, 25*time.Millisecond)
	set(b, "beta", "before")
	require.False(t, ready(health1, "shard-a"))
	start(n2, 0)
	var leader kvv1.KVServiceClient
	require.Eventually(t, func() bool {
		if ready(health1, "shard-a") {
			leader = a1
			return true
		}
		if ready(health2, "shard-a") {
			leader = a2
			return true
		}
		return false
	}, 8*time.Second, 25*time.Millisecond)
	set(leader, "alpha", "replicated")
	read(a1, "alpha", "replicated")
	read(a2, "alpha", "replicated")
	_, err = b.Get(ctx, &kvv1.GetRequest{Key: []byte("alpha")})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	logs := replv1.NewReplAdminServiceClient(repl1)
	_, err = logs.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_KV_ENGINE})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	for _, shard := range []string{"shard-a", "shard-b"} {
		page, err := logs.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_KV_ENGINE, ShardId: shard})
		require.NoError(t, err)
		require.Contains(t, string(page.Data), `"shard_id":"`+shard+`"`)
	}
	// A misaddressed RPC must not change another group's term or WAL.
	_, err = replv1.NewReplServiceClient(repl1).Propose(ctx, &replv1.ProposeRequest{Payload: []byte("bad")})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = raftv1.NewRaftServiceClient(repl1).RequestVote(ctx, &raftv1.RequestVoteRequest{ClusterId: "other", ShardId: "shard-a", Term: 100, CandidateId: n2.ID})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = raftv1.NewRaftServiceClient(repl2).AppendEntries(ctx, &raftv1.AppendEntriesRequest{ClusterId: m.ClusterID, ShardId: "shard-b", Term: 100, LeaderId: n1.ID})
	require.Equal(t, codes.NotFound, status.Code(err))

	aPID := shardEnginePID(t, n1.Repl.LogDir, "shard-a")
	bPID := shardEnginePID(t, n1.Repl.LogDir, "shard-b")
	require.NotEqual(t, aPID, bPID)
	before, err := enginev1.NewEngineServiceClient(a1Conn).GetOffset(ctx, &enginev1.GetOffsetRequest{})
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(aPID, syscall.SIGSTOP))
	set(b, "beta", "while-a-stopped")
	require.NoError(t, syscall.Kill(aPID, syscall.SIGKILL))
	read(a1, "alpha", "replicated")
	after, err := enginev1.NewEngineServiceClient(a1Conn).GetOffset(ctx, &enginev1.GetOffsetRequest{})
	require.NoError(t, err)
	require.Equal(t, before.Offset, after.Offset)
	require.NotEqual(t, aPID, shardEnginePID(t, n1.Repl.LogDir, "shard-a"))
	require.Equal(t, bPID, shardEnginePID(t, n1.Repl.LogDir, "shard-b"))

	stop1()
	waitForFailure := start(n1, 1)
	read(a1, "alpha", "replicated")
	read(b, "beta", "while-a-stopped")

	// Losing one data directory stops that replica. The node exits when the
	// last remaining group also loses the ability to persist its Raft state.
	takeStorageOffline := func(dataDir string) {
		t.Helper()
		require.NoError(t, os.Rename(dataDir, dataDir+".offline"))
	}
	takeStorageOffline(m.Shards[0].Replicas[0].DataDir)
	_, err = raftv1.NewRaftServiceClient(repl1).RequestVote(ctx, &raftv1.RequestVoteRequest{
		ClusterId: m.ClusterID, ShardId: "shard-a", Term: 1 << 32, CandidateId: n2.ID,
	})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Eventually(t, func() bool { return ready(health1, "shard-b") }, 8*time.Second, 25*time.Millisecond)
	set(b, "beta", "a-offline")
	takeStorageOffline(m.Shards[1].Replicas[0].DataDir)
	_, err = b.Set(ctx, &kvv1.SetRequest{Key: []byte("beta"), Value: []byte("cannot commit")})
	require.Equal(t, codes.Unavailable, status.Code(err))
	waitForFailure()
}

func shardEnginePID(t *testing.T, logDir, shard string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(logDir, "repl.log"))
	require.NoError(t, err)
	var pid int
	for line := range bytes.Lines(data) {
		var entry struct {
			Msg     string
			ShardID string `json:"shard_id"`
			PID     int
		}
		if json.Unmarshal(line, &entry) == nil && entry.Msg == "engine started" && entry.ShardID == shard {
			pid = entry.PID
		}
	}
	require.Positive(t, pid)
	return pid
}
