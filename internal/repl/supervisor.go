package repl

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Run supervises local engine process until ctx is canceled.
func Run(ctx context.Context, shard ShardConfig, logger *slog.Logger) error {
	engine := shard.Engine
	for ctx.Err() == nil {
		cmd := exec.Command(engine.Command[0], engine.Command[1:]...)
		stdout := &engineLogWriter{logger: logger, stream: "stdout"}
		stderr := &engineLogWriter{logger: logger, stream: "stderr"}
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
				monitorHealth(healthCtx, engine.Address, logger)
				close(healthDone)
			}()

			select {
			case err := <-waitCh:
				stopHealth()
				<-healthDone
				stdout.close()
				stderr.close()
				logEngineExit(logger, cmd.Process.Pid, err)
			case <-ctx.Done():
				stopHealth()
				shutdownEngine(cmd, waitCh, engine.ShutdownTimeout.Duration, logger)
				<-healthDone
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
