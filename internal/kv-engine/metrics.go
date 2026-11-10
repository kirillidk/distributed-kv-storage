package kvengine

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
)

type metrics struct {
	reg           *prometheus.Registry
	readiness     prometheus.Gauge
	appliedOffset prometheus.Gauge
	entriesCount  prometheus.Gauge
	getHits       *prometheus.CounterVec
	getMisses     *prometheus.CounterVec
	requestCount  prometheus.CounterVec
	requestTime   prometheus.HistogramVec
}

func newMetrics() *metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
	m := &metrics{
		reg: registry,
		readiness: promauto.With(registry).NewGauge(prometheus.GaugeOpts{
			Name: "kvengine_readiness",
			Help: "Indicates if the server is ready to serve requests (1) or not (0)",
		}),
		appliedOffset: promauto.With(registry).NewGauge(prometheus.GaugeOpts{
			Name: "kvengine_applied_offset_bytes",
			Help: "Current applied offset in the replication log",
		}),
		entriesCount: promauto.With(registry).NewGauge(prometheus.GaugeOpts{
			Name: "kvengine_entries_count",
			Help: "Current number of physical entries in storage",
		}),
		getHits: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "kvengine_get_hits_total",
			Help: "Total number of successful key lookups, labeled by RPC method",
		}, []string{"method"}),
		getMisses: promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "kvengine_get_misses_total",
			Help: "Total number of missing or expired key lookups, labeled by RPC method",
		}, []string{"method"}),
		requestCount: *promauto.With(registry).NewCounterVec(prometheus.CounterOpts{
			Name: "kvengine_request_count_total",
			Help: "Total number of requests received by the server, labeled by method and status code",
		}, []string{"method", "status"}),
		requestTime: *promauto.With(registry).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kvengine_request_duration_seconds",
			Help:    "Histogram of request durations in seconds, labeled by method",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		}, []string{"method"}),
	}
	// Publish zero-valued series so hit/miss ratios work before the first miss.
	for _, method := range []string{"Get", "GetExpireTime"} {
		m.getHits.WithLabelValues(method)
		m.getMisses.WithLabelValues(method)
	}
	return m
}

func (m *metrics) startMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux}
	log.Printf("Metrics server running on %s", srv.Addr)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()
	return srv
}

func (m *metrics) setReadiness(ready bool) {
	if ready {
		m.readiness.Set(1.0)
	} else {
		m.readiness.Set(0.0)
	}
}

func (m *metrics) logError(method string, code codes.Code) {
	m.requestCount.WithLabelValues(method, code.String()).Inc()
}

func (m *metrics) logSuccess(method string) {
	m.requestCount.WithLabelValues(method, "OK").Inc()
}

func (m *metrics) requestTimer(method string) *prometheus.Timer {
	return prometheus.NewTimer(m.requestTime.WithLabelValues(method))
}
