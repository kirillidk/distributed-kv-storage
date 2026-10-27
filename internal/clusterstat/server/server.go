package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/collector"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/config"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/logclient"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/web"
)

type Server struct {
	httpServer *http.Server
	collector  *collector.Collector
	logs       *logclient.Client
}

func New(addr string, col *collector.Collector, components []config.Component, checkTimeout time.Duration) *Server {
	mux := http.NewServeMux()
	s := &Server{
		collector: col,
		logs:      logclient.New(components, checkTimeout),
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}

	mux.HandleFunc("GET /api/v1/components", s.handleComponents)
	mux.HandleFunc("GET /api/v1/components/{node_id}/{type}/logs", s.handleComponentLogsAPI)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /{$}", s.serveIndex)
	mux.HandleFunc("GET /components/{node_id}/{type}/logs", s.serveLogsPage)

	return s
}

func (s *Server) Start() error {
	log.Printf("HTTP listening on %s", s.httpServer.Addr)
	err := s.httpServer.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) serveIndex(w http.ResponseWriter, r *http.Request) {
	data, err := web.FS.ReadFile("index.html")
	if err != nil {
		http.Error(w, "index.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) serveLogsPage(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("node_id")
	typ := config.ComponentType(r.PathValue("type"))
	if _, ok := s.collector.Lookup(nodeID, typ); !ok {
		http.NotFound(w, r)
		return
	}
	data, err := web.FS.ReadFile("logs.html")
	if err != nil {
		http.Error(w, "logs.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleComponents(w http.ResponseWriter, r *http.Request) {
	states := s.collector.Snapshot()

	type item struct {
		NodeID    string     `json:"node_id"`
		Type      string     `json:"type"`
		Address   string     `json:"address"`
		Status    string     `json:"status"`
		CheckedAt *time.Time `json:"checked_at"`
		Error     string     `json:"error"`
	}
	resp := struct {
		Components []item `json:"components"`
	}{Components: make([]item, 0, len(states))}

	for _, st := range states {
		var checked *time.Time
		if !st.CheckedAt.IsZero() {
			t := st.CheckedAt
			checked = &t
		}
		resp.Components = append(resp.Components, item{
			NodeID:    st.NodeID,
			Type:      string(st.Type),
			Address:   st.Address,
			Status:    string(st.Status),
			CheckedAt: checked,
			Error:     st.Error,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("encode /api/v1/components: %v", err)
	}
}

func (s *Server) handleComponentLogsAPI(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("node_id")
	typ := config.ComponentType(r.PathValue("type"))

	st, ok := s.collector.Lookup(nodeID, typ)
	if !ok {
		http.NotFound(w, r)
		return
	}

	result, err := s.logs.ReadSnapshot(r.Context(), nodeID, typ, 256*1024)

	type respBody struct {
		NodeID     string   `json:"node_id"`
		Type       string   `json:"type"`
		Address    string   `json:"address"`
		Status     string   `json:"status"`
		Error      string   `json:"error"`
		Source     string   `json:"source"`
		ReplAddr   string   `json:"repl_addr,omitempty"`
		NextOffset uint64   `json:"next_offset"`
		EOF        bool     `json:"eof"`
		Lines      []string `json:"lines"`
		FetchError string   `json:"fetch_error,omitempty"`
	}
	out := respBody{
		NodeID:  st.NodeID,
		Type:    string(st.Type),
		Address: st.Address,
		Status:  string(st.Status),
		Error:   st.Error,
		Source:  "repl.v1.ReplAdminService.ReadLogs",
		Lines:   []string{},
	}
	if err != nil {
		out.FetchError = err.Error()
	} else {
		out.ReplAddr = result.ReplAddr
		out.NextOffset = result.NextOffset
		out.EOF = result.EOF
		out.Lines = result.Lines
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("encode component logs: %v", err)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
