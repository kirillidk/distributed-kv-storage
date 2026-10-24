package repl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultLogPageBytes = 64 * 1024
	maxLogPageBytes     = 1024 * 1024
)

var errLogOffset = errors.New("log offset is past the end of the file")

// componentLogs owns diagnostic logs, independently of the replication WAL.
type componentLogs struct {
	repl   *logFile
	engine *logFile
	errors chan error
}

func openComponentLogs(dir string) (*componentLogs, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	logs := &componentLogs{errors: make(chan error, 1)}
	var err error
	logs.repl, err = openLogFile(filepath.Join(dir, "repl.log"), logs.errors)
	if err != nil {
		return nil, err
	}
	logs.engine, err = openLogFile(filepath.Join(dir, "kv-engine.log"), logs.errors)
	if err != nil {
		logs.repl.Close()
		return nil, err
	}
	return logs, nil
}

func (l *componentLogs) Close() error {
	err := errors.Join(l.repl.Close(), l.engine.Close())
	// Include failures from the final shutdown messages as well: the main
	// supervisor select may already have finished when those are written.
	select {
	case writeErr := <-l.errors:
		err = errors.Join(err, writeErr)
	default:
	}
	return err
}

// Each file has its own lock. Reads are bounded and cannot observe half a write.
type logFile struct {
	mu       sync.Mutex
	file     *os.File
	failures chan<- error
}

func openLogFile(path string, failures chan<- error) (*logFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open log %q: %w", path, err)
	}
	return &logFile{file: file, failures: failures}, nil
}

func (f *logFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.file.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("write log %q: %w", f.file.Name(), err)
		// slog.Logger does not return Handler errors. Notify the supervisor so
		// a full disk or another write error cannot silently disable logging.
		select {
		case f.failures <- err:
		default:
		}
	}
	return n, err
}

func (f *logFile) read(offset uint64, maxBytes uint32) ([]byte, uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, err := f.file.Stat()
	if err != nil {
		return nil, 0, false, err
	}
	size := uint64(info.Size())
	if offset > size {
		return nil, 0, false, errLogOffset
	}
	n := min(uint64(maxBytes), size-offset)
	data := make([]byte, int(n))
	if n > 0 {
		if _, err := f.file.ReadAt(data, int64(offset)); err != nil {
			return nil, 0, false, err
		}
	}
	next := offset + n
	return data, next, next == size, nil
}

func (f *logFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return errors.Join(f.file.Sync(), f.file.Close())
}

// teeLogHandler keeps the existing console output and adds a component file.
// Each handler retains its own level filter and structured attributes.
type teeLogHandler []slog.Handler

func (h teeLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h teeLogHandler) Handle(ctx context.Context, record slog.Record) error {
	var err error
	for _, handler := range h {
		if handler.Enabled(ctx, record.Level) {
			err = errors.Join(err, handler.Handle(ctx, record.Clone()))
		}
	}
	return err
}

func (h teeLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	result := make(teeLogHandler, len(h))
	for i, handler := range h {
		result[i] = handler.WithAttrs(attrs)
	}
	return result
}

func (h teeLogHandler) WithGroup(name string) slog.Handler {
	result := make(teeLogHandler, len(h))
	for i, handler := range h {
		result[i] = handler.WithGroup(name)
	}
	return result
}

type replAdminServer struct {
	replv1.UnimplementedReplAdminServiceServer
	logs *componentLogs
}

func (s *replAdminServer) ReadLogs(ctx context.Context, in *replv1.ReadLogsRequest) (*replv1.ReadLogsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if in.Component != replv1.LogComponent_LOG_COMPONENT_REPL && in.Component != replv1.LogComponent_LOG_COMPONENT_KV_ENGINE {
		return nil, status.Error(codes.InvalidArgument, "component must be REPL or KV_ENGINE")
	}
	if in.MaxBytes > maxLogPageBytes {
		return nil, status.Errorf(codes.InvalidArgument, "max_bytes must not exceed %d", maxLogPageBytes)
	}
	if s.logs == nil {
		return nil, status.Error(codes.FailedPrecondition, "file logging is disabled: configure repl.log_dir")
	}
	maxBytes := in.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultLogPageBytes
	}
	file := s.logs.repl
	if in.Component == replv1.LogComponent_LOG_COMPONENT_KV_ENGINE {
		file = s.logs.engine
	}
	data, next, eof, err := file.read(in.Offset, maxBytes)
	if errors.Is(err, errLogOffset) {
		return nil, status.Error(codes.OutOfRange, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "read component log: "+err.Error())
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &replv1.ReadLogsResponse{Data: data, NextOffset: next, Eof: eof}, nil
}
