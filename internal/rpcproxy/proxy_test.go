package rpcproxy

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc"
)

func TestProxyForwardsRequestsToConfiguredEngine(t *testing.T) {
	engineOne := newTestEngine(t, "engine-one")
	engineTwo := newTestEngine(t, "engine-two")

	proxy, err := New(context.Background(), Config{
		Engines: []Engine{
			{Name: "engine-one", Address: engineOne.address},
			{Name: "engine-two", Address: engineTwo.address},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := proxy.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")}); err != nil {
		t.Fatalf("Proxy.Set() error = %v", err)
	}

	response, err := proxy.Get(ctx, &kvv1.GetRequest{Key: []byte("key")})
	if err != nil {
		t.Fatalf("Proxy.Get() error = %v", err)
	}
	if string(response.Value) != "engine-one" && string(response.Value) != "engine-two" {
		t.Errorf("Proxy.Get() value = %q, want a response from a configured engine", response.Value)
	}
	if engineOne.calls()+engineTwo.calls() != 2 {
		t.Errorf("configured engines received %d calls, want 2", engineOne.calls()+engineTwo.calls())
	}
}

func TestNewRejectsEmptyConfiguration(t *testing.T) {
	t.Parallel()

	if _, err := New(context.Background(), Config{}); err == nil {
		t.Fatal("New() error = nil, want an error")
	}
}

type testEngine struct {
	kvv1.UnimplementedKVServiceServer

	name string

	mu        sync.Mutex
	callCount int
}

func newTestEngine(t *testing.T, name string) *runningTestEngine {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	engine := &testEngine{name: name}
	server := grpc.NewServer()
	kvv1.RegisterKVServiceServer(server, engine)
	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	return &runningTestEngine{address: listener.Addr().String(), testEngine: engine}
}

func (e *testEngine) Set(context.Context, *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	e.recordCall()
	return &kvv1.SetResponse{}, nil
}

func (e *testEngine) Get(context.Context, *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	e.recordCall()
	return &kvv1.GetResponse{Value: []byte(e.name)}, nil
}

func (e *testEngine) recordCall() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callCount++
}

func (e *testEngine) calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.callCount
}

type runningTestEngine struct {
	address string
	*testEngine
}
