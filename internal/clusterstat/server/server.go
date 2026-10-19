package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/collector"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/config"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/web"
)

type Server struct {
	httpServer *http.Server
	collector  *collector.Collector
}

func New(addr string, col *collector.Collector) *Server {
	mux := http.NewServeMux()
	s := &Server{
		collector: col,
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
		NodeID    string    `json:"node_id"`
		Type      string    `json:"type"`
		Address   string    `json:"address"`
		Status    string    `json:"status"`
		CheckedAt time.Time `json:"checked_at"`
		Error     string    `json:"error"`
	}
	resp := struct {
		Components []item `json:"components"`
	}{
		Components: make([]item, 0, len(states)),
	}
	for _, st := range states {
		resp.Components = append(resp.Components, item{
			NodeID:    st.NodeID,
			Type:      string(st.Type),
			Address:   st.Address,
			Status:    string(st.Status),
			CheckedAt: st.CheckedAt,
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

	entries, st, ok := s.collector.Logs(nodeID, typ)
	if !ok {
		http.NotFound(w, r)
		return
	}

	type logItem struct {
		Timestamp string `json:"timestamp"`
		OldStatus string `json:"old_status"`
		NewStatus string `json:"new_status"`
		Error     string `json:"error"`
		Message   string `json:"message"`
	}
	resp := struct {
		NodeID    string    `json:"node_id"`
		Type      string    `json:"type"`
		Address   string    `json:"address"`
		Status    string    `json:"status"`
		CheckedAt time.Time `json:"checked_at"`
		Error     string    `json:"error"`
		Logs      []logItem `json:"logs"`
	}{
		NodeID:    st.NodeID,
		Type:      string(st.Type),
		Address:   st.Address,
		Status:    string(st.Status),
		CheckedAt: st.CheckedAt,
		Error:     st.Error,
		Logs:      make([]logItem, 0, len(entries)),
	}

	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		resp.Logs = append(resp.Logs, logItem{
			Timestamp: e.Timestamp.UTC().Format(time.RFC3339Nano),
			OldStatus: string(e.OldStatus),
			NewStatus: string(e.NewStatus),
			Error:     e.Error,
			Message:   e.Message,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("encode component logs: %v", err)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
