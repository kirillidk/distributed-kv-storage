package repl

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestEngineHelperProcess(t *testing.T) {
	if os.Getenv("REPL_TEST_ENGINE") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	var signals chan os.Signal
	if mode == "ignore-term" {
		signal.Ignore(syscall.SIGTERM)
	} else if mode == "wait" {
		signals = make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
	}
	file, err := os.OpenFile(os.Getenv("REPL_TEST_STARTS"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(2)
	}
	fmt.Fprintln(file, os.Getpid())
	file.Close()
	fmt.Fprintln(os.Stdout, "engine ready")
	fmt.Fprintln(os.Stderr, "engine warning")
	if mode == "exit" {
		os.Exit(7)
	}
	if mode == "exit-zero" {
		os.Exit(0)
	}
	if mode == "ignore-term" {
		for {
			time.Sleep(time.Hour)
		}
	}
	<-signals
	os.Exit(0)
}

func testNode(mode string) NodeConfig {
	return NodeConfig{
		ID: "node_0",
		Repl: ReplConfig{
			ListenAddr:  "127.0.0.1:0",
			ConnectAddr: "node-0:7001",
		},
		Engine: EngineConfig{
			LocalAddr:       "127.0.0.1:1",
			ConnectAddr:     "node-0:8001",
			Command:         []string{os.Args[0], "-test.run=^TestEngineHelperProcess$", "--", mode},
			RestartDelay:    Duration{50 * time.Millisecond},
			ShutdownTimeout: Duration{100 * time.Millisecond},
		},
	}
}

func TestRunServesHealthThroughEngineRestartAndShutdown(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := testNode("exit")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runOnListener(ctx, node, testLogger(), listener) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)
	watchCtx, stopWatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWatch()
	watch, err := client.Watch(watchCtx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	status, err := watch.Recv()
	if err == nil && status.Status == healthpb.HealthCheckResponse_NOT_SERVING {
		status, err = watch.Recv()
	}
	if err != nil || status.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("initial health status = %v, error = %v", status, err)
	}
	waitForStarts(t, starts, 2)
	status, err = client.Check(watchCtx, &healthpb.HealthCheckRequest{})
	if err != nil || status.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health after engine restart = %v, error = %v", status, err)
	}
	cancel()
	status, err = watch.Recv()
	if err != nil || status.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("shutdown health status = %v, error = %v", status, err)
	}
}

func waitForStarts(t *testing.T, path string, count int) []int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			lines := strings.Fields(string(data))
			if len(lines) >= count {
				pids := make([]int, len(lines))
				for i, line := range lines {
					pids[i], err = strconv.Atoi(line)
					if err != nil {
						t.Fatal(err)
					}
				}
				return pids
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d engine starts", count)
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRunRestartsAfterNonzeroExit(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testNode("exit"), testLogger()) }()
	pids := waitForStarts(t, starts, 2)
	cancel()
	if pids[0] == pids[1] {
		t.Fatalf("engine was not restarted: %v", pids)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func TestRunRestartsAfterZeroExit(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testNode("exit-zero"), logger) }()
	pids := waitForStarts(t, starts, 2)
	cancel()
	if pids[0] == pids[1] {
		t.Fatalf("engine was not restarted: %v", pids)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop")
	}
	output := logs.String()
	for _, want := range []string{"exit_code=0", "reason=\"normal exit\""} {
		if !strings.Contains(output, want) {
			t.Errorf("missing %q in logs:\n%s", want, output)
		}
	}
}

func TestRunKillsEngineAfterShutdownTimeout(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testNode("ignore-term"), testLogger()) }()
	pid := waitForStarts(t, starts, 1)[0]
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop after shutdown timeout")
	}
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("engine PID %d still exists: %v", pid, err)
	}
	if pids := waitForStarts(t, starts, 1); len(pids) != 1 {
		t.Fatalf("engine restarted during shutdown: %v", pids)
	}
}

func TestRunStopsEngineGracefullyAndLogsOutput(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	node := testNode("wait")
	// A race-instrumented subprocess waits before exiting to flush race reports.
	node.Engine.ShutdownTimeout = Duration{3 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, node, logger) }()
	pid := waitForStarts(t, starts, 1)[0]
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop")
	}
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("engine PID %d still exists: %v", pid, err)
	}
	output := logs.String()
	for _, want := range []string{"stream=stdout", "line=\"engine ready\"", "stream=stderr", "line=\"engine warning\"", "exit_code=0"} {
		if !strings.Contains(output, want) {
			t.Errorf("missing %q in logs:\n%s", want, output)
		}
	}
}
