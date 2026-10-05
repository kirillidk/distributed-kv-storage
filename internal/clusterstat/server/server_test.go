package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/collector"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/config"
)

func TestHealthz(t *testing.T) {
	col := collector.New(nil, time.Second, time.Second)
	s := New("127.0.0.1:0", col)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	s.httpServer.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}
	if body := rr.Body.String(); body != "ok\n" {
		t.Fatalf("body=%q, want %q", body, "ok\n")
	}
}

func TestAPIComponents(t *testing.T) {
	comps := []config.Component{
		{NodeID: "node-1", Type: config.TypeKVEngine, Address: "127.0.0.1:8001"},
	}
	col := collector.New(comps, time.Hour, time.Second)

	s := New("127.0.0.1:0", col)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/components", nil)
	rr := httptest.NewRecorder()
	s.httpServer.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type=%q, want application/json", ct)
	}

	var resp struct {
		Components []struct {
			NodeID    string `json:"node_id"`
			Type      string `json:"type"`
			Address   string `json:"address"`
			Status    string `json:"status"`
			CheckedAt string `json:"checked_at"`
			Error     string `json:"error"`
		} `json:"components"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if len(resp.Components) != 1 {
		t.Fatalf("len(components)=%d, want 1", len(resp.Components))
	}
	c := resp.Components[0]
	if c.NodeID != "node-1" || c.Type != "kv-engine" || c.Address != "127.0.0.1:8001" {
		t.Fatalf("unexpected component: %+v", c)
	}
	if c.Status != "UNKNOWN" {
		t.Fatalf("status=%s, want UNKNOWN", c.Status)
	}
}

func TestHTTPShutdown(t *testing.T) {
	col := collector.New(nil, time.Second, time.Second)
	s := New("127.0.0.1:0", col)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start()
	}()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	select {
	case err := <-errCh:

		if err != nil {
			t.Fatalf("Start returned error after Shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}
