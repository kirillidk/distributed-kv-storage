package repl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func newLogsClient(t *testing.T, logs *componentLogs) replv1.ReplAdminServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	replv1.RegisterReplAdminServiceServer(server, &replAdminServer{logs: logs})
	go server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///logs",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		server.Stop()
		listener.Close()
	})
	return replv1.NewReplAdminServiceClient(conn)
}

func TestReadLogsPaginationReopenAndTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "logs")
	logs, err := openComponentLogs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logs.Close() })
	client := newLogsClient(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_KV_ENGINE, MaxBytes: 7}
	empty, err := client.ReadLogs(ctx, request)
	if err != nil || len(empty.GetData()) != 0 || !empty.GetEof() || empty.GetNextOffset() != 0 {
		t.Fatalf("empty log: %v, %v", empty, err)
	}
	// Small byte pages deliberately split JSON records and multibyte characters.
	want := []byte("{\"line\":\"привет\",\"stream\":\"stderr\"}\n{\"line\":\"next\"}\n")
	if _, err := logs.engine.Write(want); err != nil {
		t.Fatal(err)
	}
	var got []byte
	for {
		page, err := client.ReadLogs(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) > 7 || page.NextOffset != request.Offset+uint64(len(page.Data)) {
			t.Fatalf("invalid page: %v", page)
		}
		got = append(got, page.Data...)
		request.Offset = page.NextOffset
		if page.Eof {
			break
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("log = %q, want %q", got, want)
	}
	// The two components must never share file contents.
	replPage, err := client.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL})
	if err != nil || len(replPage.GetData()) != 0 {
		t.Fatalf("repl log: %v, %v", replPage, err)
	}
	if err := logs.Close(); err != nil {
		t.Fatal(err)
	}
	logs, err = openComponentLogs(dir)
	if err != nil {
		t.Fatal(err)
	}
	client = newLogsClient(t, logs)
	if _, err := logs.engine.Write([]byte("tail\n")); err != nil {
		t.Fatal(err)
	}
	page, err := client.ReadLogs(ctx, request)
	if err != nil || string(page.GetData()) != "tail\n" || !page.GetEof() {
		t.Fatalf("tail after reopen: %v, %v", page, err)
	}
	request.Offset = 0
	request.MaxBytes = 0
	page, err = client.ReadLogs(ctx, request)
	if err != nil || !bytes.Equal(page.GetData(), append(want, []byte("tail\n")...)) {
		t.Fatalf("history after reopen: %v, %v", page, err)
	}
}

func TestReadLogsLimitsAndErrors(t *testing.T) {
	logs, err := openComponentLogs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	if _, err := logs.repl.Write(bytes.Repeat([]byte("x"), defaultLogPageBytes+1)); err != nil {
		t.Fatal(err)
	}
	client := newLogsClient(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	page, err := client.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL})
	if err != nil || len(page.GetData()) != defaultLogPageBytes || page.GetEof() {
		t.Fatalf("default limit: %v, length=%d", err, len(page.GetData()))
	}
	for _, test := range []struct {
		name string
		in   *replv1.ReadLogsRequest
		code codes.Code
	}{
		{"unspecified component", &replv1.ReadLogsRequest{}, codes.InvalidArgument},
		{"unknown component", &replv1.ReadLogsRequest{Component: 99}, codes.InvalidArgument},
		{"page too large", &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL, MaxBytes: maxLogPageBytes + 1}, codes.InvalidArgument},
		{"offset past EOF", &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL, Offset: ^uint64(0)}, codes.OutOfRange},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := client.ReadLogs(ctx, test.in); status.Code(err) != test.code {
				t.Fatalf("code = %v, want %v: %v", status.Code(err), test.code, err)
			}
		})
	}
	disabled := newLogsClient(t, nil)
	if _, err := disabled.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled logs: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := client.ReadLogs(canceled, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled read: %v", err)
	}
}

func TestComponentLogsConcurrentWritesAndReads(t *testing.T) {
	logs, err := openComponentLogs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	client := newLogsClient(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger := slog.New(teeLogHandler{
		slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}),
		slog.NewJSONHandler(logs.repl, nil),
	})
	const writers, entries = 4, 100
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			withAttrs := logger.With("writer", i).WithGroup("entry")
			for j := range entries {
				withAttrs.Info("record", "id", j)
			}
		}()
	}
	writesDone := make(chan struct{})
	go func() { wg.Wait(); close(writesDone) }()
	var data []byte
	request := &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_REPL, MaxBytes: 123}
	for {
		page, err := client.ReadLogs(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, page.Data...)
		request.Offset = page.NextOffset
		// Check completion after observing that all writes have finished, then
		// make one final read. A previous EOF may predate the last append.
		select {
		case <-writesDone:
			if page.Eof {
				final, err := client.ReadLogs(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				data = append(data, final.Data...)
				if final.Eof {
					goto complete
				}
				request.Offset = final.NextOffset
			}
		default:
		}
	}
complete:
	decoder := json.NewDecoder(bytes.NewReader(data))
	seen := make(map[string]bool)
	for {
		var entry struct {
			Writer int `json:"writer"`
			Entry  struct {
				ID int `json:"id"`
			} `json:"entry"`
		}
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%d/%d", entry.Writer, entry.Entry.ID)
		if seen[key] {
			t.Fatalf("duplicate entry %s", key)
		}
		seen[key] = true
	}
	if len(seen) != writers*entries {
		t.Fatalf("got %d records, want %d", len(seen), writers*entries)
	}
}

func TestComponentLogWriteFailureIsReported(t *testing.T) {
	logs, err := openComponentLogs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	logs.engine.file.Close()
	slog.New(slog.NewJSONHandler(logs.engine, nil)).Info("engine output")
	select {
	case err := <-logs.errors:
		if !strings.Contains(err.Error(), "kv-engine.log") {
			t.Fatal(err)
		}
	default:
		t.Fatal("logging failure was swallowed")
	}
}

func TestRunLogDirectoryFailureDoesNotStartEngine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(path, []byte("existing file"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(dir, "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	node := testNode("wait")
	node.Repl.LogDir = path
	if err := Run(context.Background(), node, testLogger()); err == nil || !strings.Contains(err.Error(), "log directory") {
		t.Fatalf("expected log directory failure, got %v", err)
	}
	if _, err := os.Stat(starts); !os.IsNotExist(err) {
		t.Fatalf("engine started before opening logs: %v", err)
	}
}

func TestEngineLogsFlushPartialAndLongLines(t *testing.T) {
	var data bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&data, nil))
	writer := &engineLogWriter{logger: logger, stream: "stderr"}
	for _, part := range []string{"first\r", "\n", strings.Repeat("x", maxLogLine+1)} {
		if _, err := writer.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	writer.close()
	decoder := json.NewDecoder(&data)
	for _, want := range []string{"first", strings.Repeat("x", maxLogLine), "x"} {
		var entry struct {
			Stream string `json:"stream"`
			Line   string `json:"line"`
		}
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Stream != "stderr" || entry.Line != want {
			t.Fatalf("unexpected log entry, stream=%q length=%d", entry.Stream, len(entry.Line))
		}
	}
}

func TestRunPersistsAndServesLogsAcrossRestarts(t *testing.T) {
	t.Setenv("REPL_TEST_ENGINE", "1")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv("REPL_TEST_STARTS", starts)
	node := testNode("exit-zero")
	node.Repl.LogDir = t.TempDir()
	node.Engine.ShutdownTimeout = Duration{3 * time.Second}
	client, stop := startLogNode(t, node)
	defer stop()
	waitForStarts(t, starts, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		page, err := client.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: replv1.LogComponent_LOG_COMPONENT_KV_ENGINE})
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Count(page.Data, []byte(`"line":"engine ready"`)) >= 2 && bytes.Count(page.Data, []byte(`"line":"engine warning"`)) >= 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("stdout/stderr from both engine processes were not captured")
		case <-time.After(10 * time.Millisecond):
		}
	}
	stop()
	enginePath := filepath.Join(node.Repl.LogDir, "kv-engine.log")
	engineData, err := os.ReadFile(enginePath)
	if err != nil {
		t.Fatal(err)
	}
	replPath := filepath.Join(node.Repl.LogDir, "repl.log")
	replData, err := os.ReadFile(replPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"repl starting", "engine started", "engine exited", "engine restart scheduled", "repl stopped"} {
		if !bytes.Contains(replData, []byte(want)) {
			t.Errorf("repl log is missing %q", want)
		}
	}
	if bytes.Contains(replData, []byte(`"msg":"engine output"`)) {
		t.Error("engine output was duplicated into repl.log")
	}
	for _, want := range []string{`"stream":"stdout"`, `"stream":"stderr"`, `"line":"trailing stdout"`, `"line":"trailing stderr"`} {
		if !bytes.Contains(engineData, []byte(want)) {
			t.Errorf("engine log is missing %s", want)
		}
	}
	// Restart repl itself and ensure both files retain their previous contents.
	client, stopAgain := startLogNode(t, node)
	defer stopAgain()
	for _, test := range []struct {
		component replv1.LogComponent
		before    []byte
	}{
		{replv1.LogComponent_LOG_COMPONENT_REPL, replData},
		{replv1.LogComponent_LOG_COMPONENT_KV_ENGINE, engineData},
	} {
		page, err := client.ReadLogs(ctx, &replv1.ReadLogsRequest{Component: test.component})
		if err != nil || !bytes.HasPrefix(page.GetData(), test.before) {
			t.Fatalf("history lost after repl restart for %v: %v", test.component, err)
		}
	}
}

func startLogNode(t *testing.T, node NodeConfig) (replv1.ReplAdminServiceClient, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runOnListener(ctx, node, testLogger(), listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("supervisor did not stop")
			}
			conn.Close()
		})
	}
	t.Cleanup(stop)
	return replv1.NewReplAdminServiceClient(conn), stop
}
