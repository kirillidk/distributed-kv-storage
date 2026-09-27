package repl

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
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
	if mode == "ignore-term" {
		for {
			time.Sleep(time.Hour)
		}
	}
	<-signals
	os.Exit(0)
}

func testShard(mode string) ShardConfig {
	return ShardConfig{
		ID: "shard_0",
		Engine: EngineConfig{
			Address:         "127.0.0.1:1",
			Command:         []string{os.Args[0], "-test.run=^TestEngineHelperProcess$", "--", mode},
			RestartDelay:    Duration{50 * time.Millisecond},
			ShutdownTimeout: Duration{100 * time.Millisecond},
		},
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
	go func() { done <- Run(ctx, testShard("exit"), testLogger()) }()
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

func TestRunKillsEngineAfterShutdownTimeout(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, testShard("ignore-term"), testLogger()) }()
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
	shard := testShard("wait")
	shard.Engine.ShutdownTimeout = Duration{time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, shard, logger) }()
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
