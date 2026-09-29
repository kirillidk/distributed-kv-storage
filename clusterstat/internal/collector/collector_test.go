package collector_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kirillidk/distributed-kv-storage/clusterstat/internal/collector"
	"github.com/kirillidk/distributed-kv-storage/clusterstat/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func startFakeHealth(t *testing.T, serving bool) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	gs := grpc.NewServer()
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, hs)

	if serving {
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	} else {
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}

	go gs.Serve(lis)

	return lis.Addr().String(), func() {
		gs.Stop()
		_ = lis.Close()
	}
}

func waitFor(t *testing.T, col *collector.Collector, addr string, want collector.Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range col.Snapshot() {
			if s.Address == addr && s.Status == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, s := range col.Snapshot() {
		if s.Address == addr {
			t.Fatalf("component %s status=%s err=%q, want %s",
				addr, s.Status, s.Error, want)
		}
	}
	t.Fatalf("component %s not found", addr)
}

func TestStatusUPAndDOWN(t *testing.T) {
	upAddr, stopUP := startFakeHealth(t, true)
	defer stopUP()
	downAddr, stopDOWN := startFakeHealth(t, false)
	defer stopDOWN()

	comps := []config.Component{
		{NodeID: "n1", Type: config.TypeKVEngine, Address: upAddr},
		{NodeID: "n1", Type: config.TypeRepl, Address: downAddr},
	}
	col := collector.New(comps, 100*time.Millisecond, 500*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go col.Run(ctx)

	waitFor(t, col, upAddr, collector.StatusUP)
	waitFor(t, col, downAddr, collector.StatusDOWN)
}

func TestTimeoutMakesDOWN(t *testing.T) {
	badAddr := "127.0.0.1:1"

	comps := []config.Component{
		{NodeID: "n1", Type: config.TypeKVEngine, Address: badAddr},
	}
	col := collector.New(comps, 50*time.Millisecond, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go col.Run(ctx)

	waitFor(t, col, badAddr, collector.StatusDOWN)

	for _, s := range col.Snapshot() {
		if s.Address == badAddr && s.Error == "" {
			t.Fatal("expected non-empty error for unreachable component")
		}
	}
}

func TestTransitionUPDownUP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, hs)
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	go gs.Serve(lis)
	defer gs.Stop()

	addr := lis.Addr().String()
	comps := []config.Component{
		{NodeID: "n1", Type: config.TypeKVEngine, Address: addr},
	}
	col := collector.New(comps, 80*time.Millisecond, 300*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go col.Run(ctx)

	waitFor(t, col, addr, collector.StatusUP)

	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	waitFor(t, col, addr, collector.StatusDOWN)

	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	waitFor(t, col, addr, collector.StatusUP)
}

func TestGracefulShutdown(t *testing.T) {
	upAddr, stop := startFakeHealth(t, true)
	defer stop()

	comps := []config.Component{
		{NodeID: "n1", Type: config.TypeKVEngine, Address: upAddr},
	}
	col := collector.New(comps, 30*time.Millisecond, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		col.Run(ctx)
		close(done)
	}()

	waitFor(t, col, upAddr, collector.StatusUP)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collector.Run did not return after context cancel")
	}
}

func TestInitialStatusUnknown(t *testing.T) {

	comps := []config.Component{
		{NodeID: "n1", Type: config.TypeKVEngine, Address: "127.0.0.1:1"},
	}
	col := collector.New(comps, time.Hour, 100*time.Millisecond)

	snap := col.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("len(snapshot)=%d, want 1", len(snap))
	}
	if snap[0].Status != collector.StatusUnknown {
		t.Fatalf("status=%s, want UNKNOWN", snap[0].Status)
	}
}

func TestIndependentChecks(t *testing.T) {
	upAddr, stopUP := startFakeHealth(t, true)
	defer stopUP()
	slowAddr := "127.0.0.1:2"

	comps := []config.Component{
		{NodeID: "fast", Type: config.TypeKVEngine, Address: upAddr},
		{NodeID: "slow", Type: config.TypeRepl, Address: slowAddr},
	}

	col := collector.New(comps, 50*time.Millisecond, 800*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go col.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, s := range col.Snapshot() {
			if s.Address == upAddr && s.Status == collector.StatusUP {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("fast component did not become UP independently of slow one")
}
