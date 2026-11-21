package repl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/manifest"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Each replica is an independent Raft state machine with one local engine.
type replicaRuntime struct {
	placement            manifest.LocalReplica
	replAddr             string
	logger, engineLogger *slog.Logger
	engineConn           *grpc.ClientConn
	raft                 *raftNode
	proposals            *proposalServer
	setRaftHealth        func(bool)
}

func (r *replicaRuntime) open(peerConns map[string]*grpc.ClientConn) (err error) {
	local := r.placement
	if err := ensureDirectory(local.DataDir); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	walPath := filepath.Join(local.DataDir, "wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, wal.Close())
		}
	}()
	if err := wal.bindShard(local.NodeID, local.Shard); err != nil {
		return err
	}
	credentials := grpc.WithTransportCredentials(insecure.NewCredentials())
	engineConn, err := grpc.NewClient(local.Engine.ConnectAddr, credentials)
	if err != nil {
		return fmt.Errorf("create engine client: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, engineConn.Close())
		}
	}()
	peers := make(map[string]raftv1.RaftServiceClient, len(local.Peers))
	for _, peer := range local.Peers {
		conn := peerConns[peer.ID]
		if conn == nil {
			conn, err = grpc.NewClient(peer.Repl.ConnectAddr, credentials)
			if err != nil {
				return fmt.Errorf("create Raft peer %q: %w", peer.ID, err)
			}
			peerConns[peer.ID] = conn
		}
		peers[peer.ID] = raftv1.NewRaftServiceClient(conn)
	}
	raft, err := newRaftNode(local.NodeID, peers, wal, walPath+".raft-state", defaultRaftTiming, r.logger)
	if err != nil {
		return err
	}
	raft.clusterID, raft.shardID = local.Shard.ClusterID, local.Shard.ShardID
	r.raft = raft
	r.engineConn = engineConn
	r.proposals = &proposalServer{wal: wal, raft: raft, engine: enginev1.NewEngineServiceClient(engineConn)}
	return nil
}

func (r *replicaRuntime) registerHealth(healthServer *health.Server, onlyReplica bool) {
	setHealth := func(service string, ready bool) {
		state := healthpb.HealthCheckResponse_NOT_SERVING
		if ready {
			state = healthpb.HealthCheckResponse_SERVING
		}
		healthServer.SetServingStatus(service+"/"+r.placement.Shard.ShardID, state)
		if onlyReplica {
			healthServer.SetServingStatus(service, state)
		}
	}
	var mu sync.Mutex
	var engineReady, leaderReady bool
	update := func() {
		setHealth(replv1.ReplService_ServiceDesc.ServiceName, engineReady && leaderReady)
	}
	update()
	r.raft.onLeaderChange = func(ready bool) {
		mu.Lock()
		defer mu.Unlock()
		leaderReady = ready
		update()
	}
	r.proposals.setReady = func(ready bool) {
		mu.Lock()
		defer mu.Unlock()
		engineReady = ready
		update()
	}
	r.setRaftHealth = func(ready bool) {
		setHealth(raftv1.RaftService_ServiceDesc.ServiceName, ready)
	}
}

func (r *replicaRuntime) close() error {
	return errors.Join(r.engineConn.Close(), r.proposals.wal.Close())
}

func (r *replicaRuntime) run(parent context.Context) error {
	ctx, stop := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.supervise(ctx)
	}()
	r.setRaftHealth(true)
	err := r.raft.run(ctx)
	r.setRaftHealth(false)
	stop()
	<-done
	if err != nil {
		r.logger.Error("replica stopped", "error", err)
		return fmt.Errorf("shard %q: %w", r.placement.Shard.ShardID, err)
	}
	return nil
}

// The registry is populated before Serve. Requests select a local replica
// before any vote, log append, or engine call can change its state.
type replicaServices struct {
	replv1.UnimplementedReplServiceServer
	raftv1.UnimplementedRaftServiceServer
	clusterID string
	replicas  map[string]*replicaRuntime
}

func (s *replicaServices) lookup(clusterID, shardID string) (*replicaRuntime, error) {
	if clusterID == "" || shardID == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id and shard_id are required")
	}
	if clusterID != s.clusterID {
		return nil, status.Error(codes.FailedPrecondition, "cluster_id does not match this cluster")
	}
	r := s.replicas[shardID]
	if r == nil {
		return nil, status.Errorf(codes.NotFound, "shard %q has no replica on this node", shardID)
	}
	return r, nil
}

func (s *replicaServices) Propose(ctx context.Context, in *replv1.ProposeRequest) (*replv1.ProposeResponse, error) {
	group, err := s.lookup(in.GetClusterId(), in.GetShardId())
	if err != nil {
		return nil, err
	}
	return group.proposals.Propose(ctx, in)
}

func (s *replicaServices) RequestVote(ctx context.Context, in *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	group, err := s.lookup(in.GetClusterId(), in.GetShardId())
	if err != nil {
		return nil, err
	}
	return group.raft.RequestVote(ctx, in)
}

func (s *replicaServices) AppendEntries(ctx context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	group, err := s.lookup(in.GetClusterId(), in.GetShardId())
	if err != nil {
		return nil, err
	}
	return group.raft.AppendEntries(ctx, in)
}
