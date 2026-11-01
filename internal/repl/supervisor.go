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

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/api/replication"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Run serves node health, logs and Raft, and supervises the local engine until
// ctx is canceled. cluster must include all voters, including the local node.
// Omitting cluster starts a single-node deployment.
func Run(ctx context.Context, node NodeConfig, logger *slog.Logger, cluster ...NodeConfig) error {
	return runOnListener(ctx, node, logger, nil, cluster...)
}

func runOnListener(ctx context.Context, node NodeConfig, logger *slog.Logger, listener net.Listener, cluster ...NodeConfig) (runErr error) {
	if listener != nil {
		defer listener.Close()
	}
	var logs *componentLogs
	var logErrors <-chan error
	engineLogger := logger
	if node.Repl.LogDir != "" {
		var err error
		logs, err = openComponentLogs(node.Repl.LogDir)
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, logs.Close()) }()
		logErrors = logs.errors
		engineLogger = slog.New(teeLogHandler{logger.Handler(), slog.NewJSONHandler(logs.engine, nil)})
		logger = slog.New(teeLogHandler{logger.Handler(), slog.NewJSONHandler(logs.repl, nil)})
	}
	logger.Info("repl starting", "node_id", node.ID, "listen_addr", node.Repl.ListenAddr,
		"connect_addr", node.Repl.ConnectAddr, "engine_connect_addr", node.Engine.ConnectAddr)
	defer func() {
		if runErr != nil {
			logger.Error("repl failed", "node_id", node.ID, "error", runErr)
		} else {
			logger.Info("repl stopped", "node_id", node.ID)
		}
	}()
	if listener == nil {
		var err error
		listener, err = net.Listen("tcp", node.Repl.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen on repl address %q: %w", node.Repl.ListenAddr, err)
		}
		defer listener.Close()
	}
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(replication.MaxMessageSize))
	replv1.RegisterReplAdminServiceServer(server, &replAdminServer{logs: logs})
	var proposals *proposalServer
	var raft *raftNode
	if node.Repl.WALPath != "" {
		wal, err := OpenWAL(node.Repl.WALPath)
		if err != nil {
			return err
		}
		defer wal.Close()
		conn, err := grpc.NewClient(node.Engine.ConnectAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("create engine client: %w", err)
		}
		defer conn.Close()
		if len(cluster) == 0 {
			cluster = []NodeConfig{node}
		}
		peers := make(map[string]raftv1.RaftServiceClient, len(cluster)-1)
		seen := make(map[string]bool, len(cluster))
		for _, peer := range cluster {
			if peer.ID == "" || seen[peer.ID] || peer.Repl.WALPath == "" {
				return errors.New("Raft requires unique node IDs and a WAL on every node")
			}
			seen[peer.ID] = true
			if peer.ID == node.ID {
				continue
			}
			peerConn, err := grpc.NewClient(peer.Repl.ConnectAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("create Raft peer %q: %w", peer.ID, err)
			}
			defer peerConn.Close()
			peers[peer.ID] = raftv1.NewRaftServiceClient(peerConn)
		}
		if !seen[node.ID] {
			return errors.New("local Raft node is missing from the cluster")
		}
		raft, err = newRaftNode(node.ID, peers, wal, node.Repl.WALPath+".raft-state", defaultRaftTiming, logger)
		if err != nil {
			return err
		}
		raftv1.RegisterRaftServiceServer(server, raft)
		healthServer.SetServingStatus(raftv1.RaftService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
		healthServer.SetServingStatus(replv1.ReplService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
		var readyMu sync.Mutex
		var engineReady, leaderReady bool
		updateReady := func() {
			state := healthpb.HealthCheckResponse_NOT_SERVING
			if engineReady && leaderReady && len(peers) == 0 {
				state = healthpb.HealthCheckResponse_SERVING
			}
			healthServer.SetServingStatus(replv1.ReplService_ServiceDesc.ServiceName, state)
		}
		raft.onLeaderChange = func(ready bool) {
			readyMu.Lock()
			defer readyMu.Unlock()
			leaderReady = ready
			updateReady()
		}
		proposals = &proposalServer{
			wal: wal, raft: raft, engine: enginev1.NewEngineServiceClient(conn),
			setReady: func(ready bool) {
				readyMu.Lock()
				defer readyMu.Unlock()
				engineReady = ready
				updateReady()
			},
		}
		replv1.RegisterReplServiceServer(server, proposals)
	}
	healthpb.RegisterHealthServer(server, healthServer)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	raftCtx, stopRaft := context.WithCancel(ctx)
	defer stopRaft()
	var raftDone chan error
	if raft != nil {
		raftDone = make(chan error, 1)
		go func() { raftDone <- raft.run(raftCtx) }()
	}

	supervisorCtx, stopSupervisor := context.WithCancel(ctx)
	supervisorDone := make(chan error, 1)
	go func() { supervisorDone <- supervise(supervisorCtx, node, logger, engineLogger, proposals) }()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	logger.Info("repl health serving", "address", listener.Addr().String())

	supervisorFinished := false
	serverFinished := false
	raftFinished := false
	select {
	case <-ctx.Done():
	case runErr = <-supervisorDone:
		supervisorFinished = true
	case runErr = <-logErrors:
	case runErr = <-raftDone:
		raftFinished = true
	case err := <-serveDone:
		serverFinished = true
		if ctx.Err() == nil {
			if err == nil {
				runErr = errors.New("repl health server stopped unexpectedly")
			} else {
				runErr = fmt.Errorf("serve repl health: %w", err)
			}
		}
	}

	healthServer.Shutdown()
	stopRaft()
	stopSupervisor()
	if raftDone != nil && !raftFinished {
		if err := <-raftDone; runErr == nil {
			runErr = err
		}
	}
	if !supervisorFinished {
		if err := <-supervisorDone; runErr == nil {
			runErr = err
		}
	}
	gracefulDone := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(gracefulDone)
	}()
	select {
	case <-gracefulDone:
	case <-time.After(250 * time.Millisecond):
		server.Stop()
		<-gracefulDone
	}
	if !serverFinished {
		if err := <-serveDone; err != nil && runErr == nil && ctx.Err() == nil {
			runErr = fmt.Errorf("serve repl health: %w", err)
		}
	}
	return runErr
}

func supervise(ctx context.Context, node NodeConfig, logger, engineLogger *slog.Logger, proposals *proposalServer) error {
	engine := node.Engine
	for ctx.Err() == nil {
		if proposals != nil {
			proposals.unavailable()
		}
		cmd := exec.Command(engine.Command[0], engine.Command[1:]...)
		if proposals != nil {
			cmd.Env = append(os.Environ(), "REPL_CONNECT_ADDR="+node.Repl.ConnectAddr)
		}
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
				if proposals != nil {
					proposals.recoverEngine(healthCtx, logger)
				}
			}()

			select {
			case err := <-waitCh:
				stopHealth()
				<-healthDone
				<-recoveryDone
				if proposals != nil {
					proposals.unavailable()
				}
				stdout.close()
				stderr.close()
				logEngineExit(logger, cmd.Process.Pid, err)
			case <-ctx.Done():
				stopHealth()
				shutdownEngine(cmd, waitCh, engine.ShutdownTimeout.Duration, logger)
				<-healthDone
				<-recoveryDone
				if proposals != nil {
					proposals.unavailable()
				}
				stdout.close()
				stderr.close()
				return nil
			}
		}

		if ctx.Err() != nil {
			return nil
		}
		logger.Info("engine restart scheduled", "delay", engine.RestartDelay.Duration)
		timer := time.NewTimer(engine.RestartDelay.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
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
