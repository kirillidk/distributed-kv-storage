package repl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/manifest"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Run serves one gRPC address for the node and supervises every local replica.
func Run(ctx context.Context, topology *manifest.Manifest, nodeID string, logger *slog.Logger) error {
	return runOnListener(ctx, topology, nodeID, logger, nil)
}

func runOnListener(ctx context.Context, topology *manifest.Manifest, nodeID string, logger *slog.Logger, listener net.Listener) (runErr error) {
	if listener != nil {
		defer listener.Close()
	}
	node, err := topology.FindNode(nodeID)
	if err != nil {
		return err
	}
	local := topology.LocalReplicas(nodeID)
	var logs *componentLogs
	var logErrors <-chan error
	console := logger.Handler()
	if node.Repl.LogDir != "" {
		ids := make([]string, 0, len(local))
		for _, replica := range local {
			ids = append(ids, replica.Shard.ShardID)
		}
		logs, err = openComponentLogs(node.Repl.LogDir, ids)
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, logs.Close()) }()
		logErrors = logs.errors
		logger = slog.New(teeLogHandler{console, slog.NewJSONHandler(logs.repl, nil)})
	}
	logger = logger.With("node_id", nodeID, "cluster_id", topology.ClusterID)
	logger.Info("repl starting", "listen_addr", node.Repl.ListenAddr, "local_replicas", len(local))
	defer func() {
		if runErr != nil {
			logger.Error("repl failed", "error", runErr)
		} else {
			logger.Info("repl stopped")
		}
	}()
	if listener == nil {
		listener, err = net.Listen("tcp", node.Repl.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen on repl address %q: %w", node.Repl.ListenAddr, err)
		}
		defer listener.Close()
	}
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	services := &replicaServices{clusterID: topology.ClusterID, replicas: make(map[string]*replicaRuntime, len(local))}
	// Share the transport to a remote repl across all groups that peer with it.
	peerConns := make(map[string]*grpc.ClientConn)
	defer func() {
		for _, c := range peerConns {
			runErr = errors.Join(runErr, c.Close())
		}
	}()
	for _, placement := range local {
		id := placement.Shard.ShardID
		engineHandler := console
		if logs != nil {
			engineHandler = teeLogHandler{console, slog.NewJSONHandler(logs.engines[id], nil)}
		}
		replica := &replicaRuntime{
			placement:    placement,
			replAddr:     node.Repl.ConnectAddr,
			logger:       logger.With("shard_id", id),
			engineLogger: slog.New(engineHandler).With("node_id", nodeID, "shard_id", id),
		}
		if err := replica.open(peerConns); err != nil {
			return fmt.Errorf("open replica of shard %q: %w", id, err)
		}
		defer func() { runErr = errors.Join(runErr, replica.close()) }()
		replica.registerHealth(healthServer, len(local) == 1)
		services.replicas[id] = replica
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxRaftMessageSize))
	replv1.RegisterReplAdminServiceServer(server, &replAdminServer{logs: logs})
	replv1.RegisterReplServiceServer(server, services)
	raftv1.RegisterRaftServiceServer(server, services)
	healthpb.RegisterHealthServer(server, healthServer)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	nodeCtx, stop := context.WithCancel(ctx)
	replicaErrors := make(chan error, len(local))
	var workers sync.WaitGroup
	for _, replica := range services.replicas {
		workers.Go(func() {
			if err := replica.run(nodeCtx); err != nil {
				replicaErrors <- err
			}
		})
	}
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	logger.Info("repl health serving", "address", listener.Addr().String())
	active := len(local)
	var failedReplicas error
	for {
		select {
		case <-ctx.Done():
		case runErr = <-logErrors:
		case err := <-serveDone:
			if ctx.Err() == nil {
				runErr = fmt.Errorf("repl gRPC server stopped: %v", err)
			}
		case err := <-replicaErrors:
			failedReplicas = errors.Join(failedReplicas, err)
			active--
			if active > 0 {
				continue
			}
			runErr = failedReplicas
		}
		break
	}
	healthServer.Shutdown()
	stop()
	workers.Wait()
	// Active streams may outlive the node. Bound the drain before closing WALs.
	timer := time.AfterFunc(250*time.Millisecond, server.Stop)
	server.GracefulStop()
	timer.Stop()
	return runErr
}

func (r *replicaRuntime) supervise(ctx context.Context) {
	engine, proposals := r.placement.Engine, r.proposals
	logger, engineLogger := r.logger, r.engineLogger
	for ctx.Err() == nil {
		proposals.unavailable()
		cmd := exec.Command(engine.Command[0], engine.Command[1:]...)
		shard := r.placement.Shard
		cmd.Env = append(os.Environ(),
			"REPL_CONNECT_ADDR="+r.replAddr,
			"KV_CLUSTER_ID="+shard.ClusterID,
			"KV_SHARD_ID="+shard.ShardID,
			fmt.Sprintf("KV_SHARD_INDEX=%d", shard.Index),
			fmt.Sprintf("KV_SHARD_COUNT=%d", shard.Count),
		)
		stdout := &engineLogWriter{logger: engineLogger, stream: "stdout"}
		stderr := &engineLogWriter{logger: engineLogger, stream: "stderr"}
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		cmd.WaitDelay = time.Second

		if err := cmd.Start(); err != nil {
			logger.Error("engine failed to start", "command", engine.Command, "error", err)
		} else {
			logger.Info("engine started", "pid", cmd.Process.Pid, "command", engine.Command)
			waitCh := make(chan error, 1)
			go func() { waitCh <- cmd.Wait() }()

			healthCtx, stopHealth := context.WithCancel(ctx)
			healthDone := make(chan struct{})
			go func() {
				monitorHealth(healthCtx, engine.ConnectAddr, logger)
				close(healthDone)
			}()
			recoveryDone := make(chan struct{})
			go func() {
				defer close(recoveryDone)
				proposals.recoverEngine(healthCtx, logger)
			}()

			select {
			case err := <-waitCh:
				stopHealth()
				<-healthDone
				<-recoveryDone
				proposals.unavailable()
				stdout.close()
				stderr.close()
				logEngineExit(logger, cmd.Process.Pid, err)
			case <-ctx.Done():
				stopHealth()
				shutdownEngine(cmd, waitCh, engine.ShutdownTimeout.Duration, logger)
				<-healthDone
				<-recoveryDone
				proposals.unavailable()
				stdout.close()
				stderr.close()
				return
			}
		}

		if ctx.Err() != nil {
			return
		}
		logger.Info("engine restart scheduled", "delay", engine.RestartDelay.Duration)
		timer := time.NewTimer(engine.RestartDelay.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func shutdownEngine(cmd *exec.Cmd, waitCh <-chan error, timeout time.Duration, logger *slog.Logger) {
	pid := cmd.Process.Pid
	logger.Info("stopping engine", "pid", pid)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		logger.Warn("send SIGTERM to engine", "pid", pid, "error", err)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-waitCh:
		logEngineExit(logger, pid, err)
	case <-timer.C:
		logger.Warn("engine shutdown timed out; killing", "pid", pid, "timeout", timeout)
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			logger.Error("kill engine", "pid", pid, "error", err)
		}
		logEngineExit(logger, pid, <-waitCh)
	}
}

func logEngineExit(logger *slog.Logger, pid int, err error) {
	if err == nil {
		logger.Info("engine exited", "pid", pid, "exit_code", 0, "reason", "normal exit")
		return
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		logger.Warn("engine exited", "pid", pid, "exit_code", exitErr.ExitCode(), "reason", exitErr.Error())
		return
	}
	logger.Warn("engine exited", "pid", pid, "reason", err)
}

// engineLogWriter turns each process output line into a structured log entry.
type engineLogWriter struct {
	mu     sync.Mutex
	logger *slog.Logger
	stream string
	buffer []byte
}

const maxLogLine = 64 * 1024

func (w *engineLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	w.buffer = append(w.buffer, p...)
	for {
		if index := bytes.IndexByte(w.buffer, '\n'); index >= 0 && index < maxLogLine {
			w.emit(w.buffer[:index])
			w.buffer = w.buffer[index+1:]
		} else if len(w.buffer) >= maxLogLine {
			w.emit(w.buffer[:maxLogLine])
			w.buffer = w.buffer[maxLogLine:]
		} else {
			break
		}
	}
	return n, nil
}

func (w *engineLogWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buffer) > 0 {
		w.emit(w.buffer)
		w.buffer = nil
	}
}

func (w *engineLogWriter) emit(line []byte) {
	w.logger.Info("engine output", "stream", w.stream, "line", string(bytes.TrimSuffix(line, []byte{'\r'})))
}
