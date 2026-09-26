package kvengine

import (
	"context"
	"net"
	"testing"
	"time"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// newTestServer spins up the KV service + health service on an
// ephemeral localhost port and returns connected clients.
// It mirrors the wiring done in RunServer without blocking on signals.
func newTestServer(t *testing.T, storage Storage) (kvv1.KVServiceClient, healthgrpc.HealthClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	if err != nil {
		t.FailNow()
	}

	grpcServer := grpc.NewServer()
	srv := CreateServer(storage)
	kvv1.RegisterKVServiceServer(grpcServer, &srv)

	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(grpcServer, healthcheck)
	healthcheck.SetServingStatus("kv-engine", healthgrpc.HealthCheckResponse_SERVING)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	assert.NoError(t, err)
	if err != nil {
		grpcServer.Stop()
		t.FailNow()
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		lis.Close()
	}

	return kvv1.NewKVServiceClient(conn), healthgrpc.NewHealthClient(conn), cleanup
}

func TestSetGetRoundTrip(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte("key1"), Value: []byte("value1")})
	assert.NoError(t, err)
	assert.Equal(t, codes.OK, status.Code(err))

	resp, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("key1")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("value1"), resp.GetValue())
	}
}

func TestSetEmptyKey(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte(""), Value: []byte("value")})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetEmptyKey(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("")})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetNotFound(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("missing")})
	assert.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestHealthCheck(t *testing.T) {
	_, healthClient, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := healthClient.Check(ctx, &healthgrpc.HealthCheckRequest{Service: "kv-engine"})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, healthgrpc.HealthCheckResponse_SERVING, resp.GetStatus())
	}
}
