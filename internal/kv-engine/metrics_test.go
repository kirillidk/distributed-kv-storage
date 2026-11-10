package kvengine

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc/codes"
)

func gatherMetricFamily(t *testing.T, m *metrics, name string) *dto.MetricFamily {
	t.Helper()

	families, err := m.reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}

	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	t.Fatalf("metric %q not found; gathered: %v", name, names)
	return nil
}

func gaugeValue(t *testing.T, m *metrics, name string) float64 {
	t.Helper()

	family := gatherMetricFamily(t, m, name)
	if len(family.GetMetric()) != 1 {
		t.Fatalf("metric %q has %d samples, want 1", name, len(family.GetMetric()))
	}
	return family.GetMetric()[0].GetGauge().GetValue()
}

func labelsMatch(metric *dto.Metric, labels map[string]string) bool {
	if len(metric.GetLabel()) != len(labels) {
		return false
	}
	for _, pair := range metric.GetLabel() {
		want, ok := labels[pair.GetName()]
		if !ok || pair.GetValue() != want {
			return false
		}
	}
	return true
}

func metricWithLabels(t *testing.T, m *metrics, name string, labels map[string]string) *dto.Metric {
	t.Helper()

	family := gatherMetricFamily(t, m, name)
	for _, metric := range family.GetMetric() {
		if labelsMatch(metric, labels) {
			return metric
		}
	}
	t.Fatalf("metric %q has no sample with labels %v", name, labels)
	return nil
}

func counterVecValue(t *testing.T, m *metrics, name string, labels map[string]string) float64 {
	t.Helper()
	return metricWithLabels(t, m, name, labels).GetCounter().GetValue()
}

func histogramSampleCount(t *testing.T, m *metrics, name string, labels map[string]string) uint64 {
	t.Helper()
	return metricWithLabels(t, m, name, labels).GetHistogram().GetSampleCount()
}

func assertGauge(t *testing.T, m *metrics, name string, want float64) {
	t.Helper()
	if got := gaugeValue(t, m, name); got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertCounterVec(t *testing.T, m *metrics, name string, labels map[string]string, want float64) {
	t.Helper()
	if got := counterVecValue(t, m, name, labels); got != want {
		t.Fatalf("%s%v = %v, want %v", name, labels, got, want)
	}
}

func TestMetricsExposesAllMetrics(t *testing.T) {
	m := newMetrics()
	m.setReadiness(true)
	m.appliedOffset.Set(42)
	m.entriesCount.Set(7)
	m.getHits.WithLabelValues("Get").Inc()
	m.getMisses.WithLabelValues("Get").Inc()
	m.logSuccess("Get")
	m.logError("Set", codes.InvalidArgument)
	m.requestTimer("Get").ObserveDuration()

	for _, name := range []string{
		"kvengine_readiness",
		"kvengine_applied_offset_bytes",
		"kvengine_entries_count",
		"kvengine_get_hits_total",
		"kvengine_get_misses_total",
		"kvengine_request_count_total",
		"kvengine_request_duration_seconds",
		// The registry also exposes the standard Go runtime metrics.
		"go_goroutines",
	} {
		gatherMetricFamily(t, m, name)
	}

	assertGauge(t, m, "kvengine_readiness", 1)
	assertGauge(t, m, "kvengine_applied_offset_bytes", 42)
	assertGauge(t, m, "kvengine_entries_count", 7)
	assertCounterVec(t, m, "kvengine_get_hits_total", map[string]string{"method": "Get"}, 1)
	assertCounterVec(t, m, "kvengine_get_misses_total", map[string]string{"method": "Get"}, 1)
	assertCounterVec(t, m, "kvengine_request_count_total", map[string]string{
		"method": "Get",
		"status": "OK",
	}, 1)
	assertCounterVec(t, m, "kvengine_request_count_total", map[string]string{
		"method": "Set",
		"status": codes.InvalidArgument.String(),
	}, 1)
	if got := histogramSampleCount(t, m, "kvengine_request_duration_seconds", map[string]string{"method": "Get"}); got != 1 {
		t.Fatalf("request duration sample count = %d, want 1", got)
	}
}

func TestMetricsReadinessFollowsServerState(t *testing.T) {
	s := newServer(CreateMemoryStorage(), SystemTimeSource{})
	assertGauge(t, s.metrics, "kvengine_readiness", 1)

	if _, err := s.SetReady(context.Background(), &enginev1.SetReadyRequest{Ready: false}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_readiness", 0)

	if _, err := s.SetReady(context.Background(), &enginev1.SetReadyRequest{Ready: true}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_readiness", 1)
}

func TestMetricsInitialPhysicalEntryCount(t *testing.T) {
	storage := CreateMemoryStorage()
	if err := storage.Set("live", "value", 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Set("expired", "value", 1); err != nil {
		t.Fatal(err)
	}
	s := CreateServer(storage)
	assertGauge(t, s.metrics, "kvengine_entries_count", 2)
	if _, err := s.Get(context.Background(), &kvv1.GetRequest{Key: []byte("expired")}); err == nil {
		t.Fatal("expected expired key to be unavailable")
	}
	// Expiration hides a key but does not remove its physical storage entry.
	assertGauge(t, s.metrics, "kvengine_entries_count", 2)
}

func TestMetricsAppliedOffsetAndEntryCount(t *testing.T) {
	ctx := context.Background()
	clock := &TestTimeSource{t: 100}
	storage := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: clock}
	s := newServer(storage, clock)

	if _, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_entries_count", 1)

	// Overwriting an existing key does not change the number of entries.
	if _, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_entries_count", 1)

	if _, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("other"), Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("added"), Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_entries_count", 3)

	if _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("other")}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_entries_count", 2)

	if _, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("key"), Value: []byte("replaced")}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_entries_count", 2)

	setPayload, err := encodeSetEvent([]byte("applied"), []byte("value"), 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := uint64(len(setPayload))
	if _, err := s.Apply(ctx, &enginev1.ApplyRequest{Offset: offset, Payload: setPayload}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_applied_offset_bytes", float64(offset))
	assertGauge(t, s.metrics, "kvengine_entries_count", 3)

	addPayload, err := encodeMutation(mutation{
		kind:      addEventMagic,
		now:       clock.Now(),
		expiresAt: 0,
		key:       []byte("applied-add"),
		value:     []byte("value"),
	})
	if err != nil {
		t.Fatal(err)
	}
	offset += uint64(len(addPayload))
	if _, err := s.Apply(ctx, &enginev1.ApplyRequest{Offset: offset, Payload: addPayload}); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, s.metrics, "kvengine_applied_offset_bytes", float64(offset))
	assertGauge(t, s.metrics, "kvengine_entries_count", 4)
}

func TestMetricsGetHitsAndMisses(t *testing.T) {
	ctx := context.Background()
	s := newServer(CreateMemoryStorage(), SystemTimeSource{})
	assertLookups := func(method string, hits, misses float64) {
		t.Helper()
		labels := map[string]string{"method": method}
		assertCounterVec(t, s.metrics, "kvengine_get_hits_total", labels, hits)
		assertCounterVec(t, s.metrics, "kvengine_get_misses_total", labels, misses)
	}
	assertLookups("Get", 0, 0)
	assertLookups("GetExpireTime", 0, 0)

	if _, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte("key")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte("missing")}); err == nil {
		t.Fatal("expected GetExpireTime of a missing key to fail")
	}
	assertLookups("GetExpireTime", 1, 1)
	assertLookups("Get", 0, 0)

	if _, err := s.Get(ctx, &kvv1.GetRequest{Key: []byte("key")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, &kvv1.GetRequest{Key: []byte("missing")}); err == nil {
		t.Fatal("expected Get of a missing key to fail")
	}
	assertLookups("Get", 1, 1)
	assertLookups("GetExpireTime", 1, 1)

	// Invalid requests are neither hits nor misses.
	if _, err := s.Get(ctx, &kvv1.GetRequest{}); err == nil {
		t.Fatal("expected Get of an empty key to fail")
	}
	if _, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{}); err == nil {
		t.Fatal("expected GetExpireTime of an empty key to fail")
	}
	for _, method := range []string{"Get", "GetExpireTime"} {
		assertLookups(method, 1, 1)
		for _, code := range []codes.Code{codes.OK, codes.NotFound, codes.InvalidArgument} {
			assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
				"method": method,
				"status": code.String(),
			}, 1)
		}
	}
}

func TestMetricsRequestCountAndDuration(t *testing.T) {
	ctx := context.Background()
	s := newServer(CreateMemoryStorage(), SystemTimeSource{})

	if _, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("counter"), Value: []byte("10")}); err != nil {
		t.Fatal(err)
	}
	successCalls := []struct {
		method string
		call   func() error
	}{
		{"Get", func() error { _, err := s.Get(ctx, &kvv1.GetRequest{Key: []byte("counter")}); return err }},
		{"GetExpireTime", func() error {
			_, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte("counter")})
			return err
		}},
		{"Add", func() error {
			_, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("added"), Value: []byte("1")})
			return err
		}},
		{"Replace", func() error {
			_, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("counter"), Value: []byte("11")})
			return err
		}},
		{"Incr", func() error {
			_, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("counter"), Delta: 1})
			return err
		}},
		{"Decr", func() error {
			_, err := s.Decr(ctx, &kvv1.DecrRequest{Key: []byte("counter"), Delta: 1})
			return err
		}},
		{"Delete", func() error { _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("added")}); return err }},
	}
	// Set succeeded above.
	assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
		"method": "Set",
		"status": "OK",
	}, 1)
	if got := histogramSampleCount(t, s.metrics, "kvengine_request_duration_seconds", map[string]string{"method": "Set"}); got != 1 {
		t.Fatalf("Set duration sample count = %d, want 1", got)
	}

	for _, tc := range successCalls {
		if err := tc.call(); err != nil {
			t.Fatalf("%s: %v", tc.method, err)
		}
		assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
			"method": tc.method,
			"status": "OK",
		}, 1)
		if got := histogramSampleCount(t, s.metrics, "kvengine_request_duration_seconds", map[string]string{"method": tc.method}); got != 1 {
			t.Fatalf("%s duration sample count = %d, want 1", tc.method, got)
		}
	}

	errorCalls := []struct {
		method string
		code   codes.Code
		call   func() error
	}{
		{"Set", codes.InvalidArgument, func() error {
			_, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("")})
			return err
		}},
		{"Get", codes.NotFound, func() error {
			_, err := s.Get(ctx, &kvv1.GetRequest{Key: []byte("missing")})
			return err
		}},
		{"GetExpireTime", codes.InvalidArgument, func() error {
			_, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte("")})
			return err
		}},
		{"Add", codes.InvalidArgument, func() error {
			_, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("")})
			return err
		}},
		{"Replace", codes.InvalidArgument, func() error {
			_, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("")})
			return err
		}},
		{"Delete", codes.InvalidArgument, func() error {
			_, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("")})
			return err
		}},
		{"Incr", codes.NotFound, func() error {
			_, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("missing"), Delta: 1})
			return err
		}},
		{"Decr", codes.InvalidArgument, func() error {
			_, err := s.Decr(ctx, &kvv1.DecrRequest{Key: []byte("")})
			return err
		}},
	}

	for _, tc := range errorCalls {
		if err := tc.call(); err == nil {
			t.Fatalf("%s: expected an error", tc.method)
		}
		assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
			"method": tc.method,
			"status": tc.code.String(),
		}, 1)
	}

	for _, tc := range errorCalls {
		if got := histogramSampleCount(t, s.metrics, "kvengine_request_duration_seconds", map[string]string{"method": tc.method}); got != 2 {
			t.Fatalf("%s duration sample count = %d, want 2", tc.method, got)
		}
	}
}

func TestMetricsRecoveryRejectionsAreLabeledUnavailable(t *testing.T) {
	ctx := context.Background()
	s := newServer(CreateMemoryStorage(), SystemTimeSource{})
	if _, err := s.SetReady(ctx, &enginev1.SetReadyRequest{Ready: false}); err != nil {
		t.Fatal(err)
	}

	calls := []struct {
		method string
		call   func() error
	}{
		{"Set", func() error {
			_, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")})
			return err
		}},
		{"Get", func() error { _, err := s.Get(ctx, &kvv1.GetRequest{Key: []byte("key")}); return err }},
		{"GetExpireTime", func() error {
			_, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte("key")})
			return err
		}},
		{"Add", func() error {
			_, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("key"), Value: []byte("value")})
			return err
		}},
		{"Replace", func() error {
			_, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("key"), Value: []byte("value")})
			return err
		}},
		{"Delete", func() error { _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("key")}); return err }},
		{"Incr", func() error {
			_, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("key"), Delta: 1})
			return err
		}},
		{"Decr", func() error {
			_, err := s.Decr(ctx, &kvv1.DecrRequest{Key: []byte("key"), Delta: 1})
			return err
		}},
	}

	assertGauge(t, s.metrics, "kvengine_readiness", 0)
	for _, tc := range calls {
		if err := tc.call(); err == nil {
			t.Fatalf("%s: expected an error during recovery", tc.method)
		}
		assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
			"method": tc.method,
			"status": codes.Unavailable.String(),
		}, 1)
		if got := histogramSampleCount(t, s.metrics, "kvengine_request_duration_seconds", map[string]string{"method": tc.method}); got != 1 {
			t.Fatalf("%s duration sample count = %d, want 1", tc.method, got)
		}
	}
}

func TestMetricsHTTPExposure(t *testing.T) {
	m := newMetrics()
	m.setReadiness(true)
	m.entriesCount.Set(3)
	m.logSuccess("Get")

	srv := httptest.NewServer(promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# HELP kvengine_readiness",
		"kvengine_readiness 1",
		"kvengine_entries_count 3",
		"kvengine_request_count_total{method=\"Get\",status=\"OK\"} 1",
	} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("metrics response does not contain %q:\n%s", want, body)
		}
	}
}

func TestMetricsServerServesMetrics(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}

	m := newMetrics()
	m.setReadiness(true)
	srv := m.startMetricsServer(addr)
	defer srv.Close()

	deadline := time.Now().Add(3 * time.Second)
	var body []byte
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
	}
	if !strings.Contains(string(body), "kvengine_readiness 1") {
		t.Fatalf("metrics server response does not contain readiness:\n%s", body)
	}
}

func TestMetricsConcurrentCollection(t *testing.T) {
	const (
		workers    = 8
		iterations = 100
	)
	ctx := context.Background()
	s := newServer(CreateMemoryStorage(), SystemTimeSource{})

	var requests sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		requests.Add(1)
		go func(id int) {
			defer requests.Done()
			key := []byte(fmt.Sprintf("key-%d", id))
			for i := 0; i < iterations; i++ {
				_, _ = s.Set(ctx, &kvv1.SetRequest{Key: key, Value: []byte("1"), Ttl: 60})
				_, _ = s.Get(ctx, &kvv1.GetRequest{Key: key})
				_, _ = s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: key})
				_, _ = s.Incr(ctx, &kvv1.IncrRequest{Key: key, Delta: 1})
				_, _ = s.Decr(ctx, &kvv1.DecrRequest{Key: key, Delta: 1})
				_, _ = s.Get(ctx, &kvv1.GetRequest{Key: []byte("missing")})
				_, _ = s.Delete(ctx, &kvv1.DeleteRequest{Key: key})
			}
		}(worker)
	}

	stop := make(chan struct{})
	errCh := make(chan error, 4)
	var gatherers sync.WaitGroup
	for i := 0; i < 4; i++ {
		gatherers.Add(1)
		go func() {
			defer gatherers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := s.metrics.reg.Gather(); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}

	requests.Wait()
	close(stop)
	gatherers.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("gather metrics concurrently: %v", err)
	default:
	}

	for _, method := range []string{"Get", "GetExpireTime"} {
		assertCounterVec(t, s.metrics, "kvengine_get_hits_total", map[string]string{"method": method}, float64(workers*iterations))
	}
	assertCounterVec(t, s.metrics, "kvengine_get_misses_total", map[string]string{"method": "Get"}, float64(workers*iterations))
	assertCounterVec(t, s.metrics, "kvengine_get_misses_total", map[string]string{"method": "GetExpireTime"}, 0)
	assertGauge(t, s.metrics, "kvengine_entries_count", 0)
	assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
		"method": "Get",
		"status": "OK",
	}, float64(workers*iterations))
	assertCounterVec(t, s.metrics, "kvengine_request_count_total", map[string]string{
		"method": "Get",
		"status": codes.NotFound.String(),
	}, float64(workers*iterations))
}
