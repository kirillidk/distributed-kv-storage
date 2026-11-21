package repl

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestIndependentShardQuorumsAndLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real kv-engine processes")
	}
	dir := t.TempDir()
	engineBin := buildTestBinary(t, dir, "kv-engine")
	manifest := &Manifest{ShardCount: 3}
	type member struct {
		replPort, enginePort net.Listener
		health               healthpb.HealthClient
		kv                   kvv1.KVServiceClient
		engine               enginev1.EngineServiceClient
	}
	var members []member
	reservePort := func() net.Listener {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { l.Close() })
		return l
	}
	for i, shard := range []ShardNumber{0, 1, 1, 2} {
		replPort, enginePort := reservePort(), reservePort()
		replAddr, engineAddr := replPort.Addr().String(), enginePort.Addr().String()
		_, port, err := net.SplitHostPort(engineAddr)
		require.NoError(t, err)
		id := fmt.Sprintf("node-%d", i)
		manifest.Nodes = append(manifest.Nodes, NodeConfig{
			ID: id, ShardID: shard,
			Repl: ReplConfig{ListenAddr: replAddr, ConnectAddr: replAddr, WALPath: filepath.Join(dir, id+".wal")},
			Engine: EngineConfig{
				ConnectAddr: engineAddr, Command: []string{engineBin, "--port", port, "--metrics-listen", "127.0.0.1:0"},
				RestartDelay: Duration{time.Second}, ShutdownTimeout: Duration{2 * time.Second},
			},
		})
		engineConn := testConnection(t, engineAddr)
		members = append(members, member{
			replPort: replPort, enginePort: enginePort, health: healthpb.NewHealthClient(testConnection(t, replAddr)),
			kv: kvv1.NewKVServiceClient(engineConn), engine: enginev1.NewEngineServiceClient(engineConn),
		})
	}
	manifest.Nodes[3].Repl.WALPath = ""
	t.Setenv("REPL_CONNECT_ADDR", manifest.Nodes[0].Repl.ConnectAddr)
	require.NoError(t, manifest.Validate())
	start := func(i int) {
		require.NoError(t, members[i].enginePort.Close())
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- runOnListener(ctx, manifest, manifest.Nodes[i].ID, testLogger(), members[i].replPort) }()
		t.Cleanup(func() {
			cancel()
			require.NoError(t, <-done)
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	waitLeader := func(indices ...int) int {
		t.Helper()
		var leader int
		require.Eventually(t, func() bool {
			for _, i := range indices {
				probe, stop := context.WithTimeout(ctx, 250*time.Millisecond)
				r, err := members[i].health.Check(probe, &healthpb.HealthCheckRequest{Service: replv1.ReplService_ServiceDesc.ServiceName})
				stop()
				if err == nil && r.Status == healthpb.HealthCheckResponse_SERVING {
					leader = i
					return true
				}
			}
			return false
		}, 8*time.Second, 25*time.Millisecond, "shard members %v did not elect a ready leader", indices)
		return leader
	}
	offset := func(i int) uint64 {
		r, err := members[i].engine.GetOffset(ctx, &enginev1.GetOffsetRequest{})
		require.NoError(t, err)
		return r.Offset
	}

	// This node is a majority of shard 0 even while all other nodes are offline.
	start(0)
	waitLeader(0)
	_, err := members[0].kv.Set(ctx, &kvv1.SetRequest{Key: []byte("counter"), Value: []byte("first")})
	require.NoError(t, err)
	firstOffset := offset(0)
	require.Positive(t, firstOffset)

	start(1)
	start(2)
	leader := waitLeader(1, 2)
	require.Zero(t, offset(leader))
	_, err = members[leader].kv.Set(ctx, &kvv1.SetRequest{Key: []byte("counter")})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, offset(leader))
	_, err = members[leader].kv.Set(ctx, &kvv1.SetRequest{Key: []byte("beta"), Value: []byte("second")})
	require.NoError(t, err)
	for _, i := range []int{1, 2} {
		require.Eventually(t, func() bool {
			r, err := members[i].kv.Get(ctx, &kvv1.GetRequest{Key: []byte("beta")})
			return err == nil && string(r.Value) == "second"
		}, 4*time.Second, 25*time.Millisecond)
	}
	require.Equal(t, offset(1), offset(2))
	_, err = members[0].kv.Get(ctx, &kvv1.GetRequest{Key: []byte("beta")})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// The standalone memory shard must serve locally despite the parent repl address.
	start(3)
	_, err = members[3].kv.Set(ctx, &kvv1.SetRequest{Key: []byte("gamma"), Value: []byte("third")}, grpc.WaitForReady(true))
	require.NoError(t, err)
	r, err := members[3].kv.Get(ctx, &kvv1.GetRequest{Key: []byte("gamma")})
	require.NoError(t, err)
	require.Equal(t, "third", string(r.Value))
	require.Zero(t, offset(3))
	require.Equal(t, firstOffset, offset(0))
}
