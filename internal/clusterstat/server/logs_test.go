package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/collector"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/config"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/logclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pagedLogServer struct {
	replv1.UnimplementedReplAdminServiceServer
	mu       sync.Mutex
	engines  map[string][]byte
	failNext bool
	calls    int
}

func (s *pagedLogServer) ReadLogs(ctx context.Context, in *replv1.ReadLogsRequest) (*replv1.ReadLogsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.failNext {
		s.failNext = false
		return nil, status.Error(codes.Unavailable, "temporary failure")
	}
	data := []byte("repl line\n")
	if in.Component == replv1.LogComponent_LOG_COMPONENT_KV_ENGINE {
		var ok bool
		data, ok = s.engines[in.ShardId]
		if !ok {
			return nil, status.Error(codes.NotFound, "unknown engine")
		}
	}
	if in.Offset > uint64(len(data)) {
		return nil, status.Error(codes.OutOfRange, "offset exceeds file size")
	}
	// Short RPC responses exercise aggregation and the remaining HTTP page limit.
	size := min(uint64(in.MaxBytes), uint64(len(data))-in.Offset, 10000)
	end := in.Offset + size
	return &replv1.ReadLogsResponse{Data: bytes.Clone(data[in.Offset:end]), NextOffset: end, Eof: end == uint64(len(data))}, nil
}

func newPagedLogServer(t *testing.T, engines map[string][]byte) (*Server, *pagedLogServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	logs := &pagedLogServer{engines: engines}
	replv1.RegisterReplAdminServiceServer(grpcServer, logs)
	go grpcServer.Serve(listener)
	t.Cleanup(func() { grpcServer.Stop(); listener.Close() })
	components := []config.Component{
		{NodeID: "node-1", Type: config.TypeRepl, Address: listener.Addr().String()},
	}
	for id := range engines {
		components = append(components, config.Component{NodeID: "node-1", Type: config.TypeKVEngine, ShardID: id})
	}
	col := collector.New(components, time.Hour, time.Second)
	return New("127.0.0.1:0", col, components, time.Second, ""), logs
}

type logPage struct {
	ShardID    string   `json:"shard_id"`
	Data       []byte   `json:"data"`
	Lines      []string `json:"lines"`
	NextOffset uint64   `json:"next_offset"`
	EOF        bool     `json:"eof"`
	FetchError string   `json:"fetch_error"`
}

func readLogPage(t *testing.T, s *Server, target string) logPage {
	t.Helper()
	rr := httptest.NewRecorder()
	s.httpServer.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var page logPage
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestLogPagesContinuePastLimitWithoutLosingUTF8(t *testing.T) {
	const endpoint = "/api/v1/components/node-1/kv-engine/logs"
	// The first byte of the emoji falls at the end of the first HTTP page.
	data := []byte(strings.Repeat("x", logclient.PageBytes-1) + "🙂 tail\nlast line")
	s, logs := newPagedLogServer(t, map[string][]byte{"shard-a": data})
	first := readLogPage(t, s, endpoint)
	if first.FetchError != "" || first.EOF || first.NextOffset != logclient.PageBytes ||
		!bytes.Equal(first.Data, data[:logclient.PageBytes]) {
		t.Fatalf("first page: bytes=%d next=%d eof=%v error=%q", len(first.Data), first.NextOffset, first.EOF, first.FetchError)
	}
	nextURL := fmt.Sprintf("%s?offset=%d", endpoint, first.NextOffset)
	logs.mu.Lock()
	logs.failNext = true
	logs.mu.Unlock()
	failed := readLogPage(t, s, nextURL)
	if failed.FetchError == "" || len(failed.Data) != 0 || failed.NextOffset != first.NextOffset {
		t.Fatal("failed page advanced the cursor")
	}
	second := readLogPage(t, s, nextURL)
	if second.FetchError != "" || !second.EOF || second.NextOffset != uint64(len(data)) {
		t.Fatalf("second page: next=%d eof=%v error=%q", second.NextOffset, second.EOF, second.FetchError)
	}
	if got := append(bytes.Clone(first.Data), second.Data...); !bytes.Equal(got, data) {
		t.Fatal("pagination lost or duplicated bytes")
	}
	eofURL := fmt.Sprintf("%s?offset=%d", endpoint, second.NextOffset)
	empty := readLogPage(t, s, eofURL)
	if empty.FetchError != "" || !empty.EOF || len(empty.Data) != 0 || empty.NextOffset != second.NextOffset {
		t.Fatal("EOF page changed the cursor")
	}
	logs.mu.Lock()
	logs.engines["shard-a"] = append(logs.engines["shard-a"], []byte(" completed\nnew line\n")...)
	logs.mu.Unlock()
	appended := readLogPage(t, s, eofURL)
	if appended.FetchError != "" || !appended.EOF || string(appended.Data) != " completed\nnew line\n" {
		t.Fatalf("read after append: %q %v %q", appended.Data, appended.EOF, appended.FetchError)
	}
	repl := readLogPage(t, s, "/api/v1/components/node-1/repl/logs?offset=5")
	if repl.FetchError != "" || string(repl.Data) != "line\n" || !repl.EOF {
		t.Fatal("wrong component or offset")
	}
}

func TestLogPagesValidateOffset(t *testing.T) {
	s, logs := newPagedLogServer(t, map[string][]byte{"shard-a": []byte("line\n")})
	for _, query := range []string{"offset=", "offset=-1", "offset=abc", "offset=18446744073709551616", "offset=1&offset=2"} {
		t.Run(query, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/components/node-1/kv-engine/logs?"+query, nil)
			s.httpServer.Handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		})
	}
	logs.mu.Lock()
	calls := logs.calls
	logs.mu.Unlock()
	if calls != 0 {
		t.Fatal("invalid offset reached repl")
	}
	page := readLogPage(t, s, "/api/v1/components/node-1/kv-engine/logs?offset=18446744073709551615")
	if page.FetchError == "" || page.NextOffset != ^uint64(0) {
		t.Fatal("large offset was lost or truncated")
	}
}

func TestComponentLogsSelectLocalReplica(t *testing.T) {
	const endpoint = "/api/v1/components/node-1/kv-engine/logs"
	engines := map[string][]byte{"shard-a": []byte("first engine"), "shard-b": []byte("second engine")}
	s, _ := newPagedLogServer(t, engines)
	for id, data := range engines {
		page := readLogPage(t, s, endpoint+"?shard_id="+id)
		if page.ShardID != id || !bytes.Equal(page.Data, data) || page.FetchError != "" {
			t.Fatalf("log page for %s: %+v", id, page)
		}
	}
	for _, query := range []string{"", "?shard_id=missing"} {
		rr := httptest.NewRecorder()
		s.httpServer.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, endpoint+query, nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status for %q = %d", query, rr.Code)
		}
	}
}
