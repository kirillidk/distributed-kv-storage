package repl

import (
	"context"
	"encoding/binary"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	kvengine "github.com/kirillidk/distributed-kv-storage/internal/kv-engine"
	"github.com/kirillidk/distributed-kv-storage/internal/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Use the actual gRPC chain to exercise wire limits and readiness, rather than
// bypassing either hop with an in-process proposal callback.
func recoveryFixture(t *testing.T, options ...grpc.ServerOption) (*proposalServer, kvv1.KVServiceClient, replv1.ReplServiceClient) {
	t.Helper()
	serve := func(register func(*grpc.Server)) *grpc.ClientConn {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer(append([]grpc.ServerOption{grpc.MaxRecvMsgSize(replication.MaxMessageSize)}, options...)...)
		register(server)
		go server.Serve(listener)
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			server.Stop()
			listener.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close() })
		return conn
	}
	s := &proposalServer{wal: openTestWAL(t, filepath.Join(t.TempDir(), "wal"))}
	replConn := serve(func(server *grpc.Server) { replv1.RegisterReplServiceServer(server, s) })
	proposer := replv1.NewReplServiceClient(replConn)
	engine := kvengine.CreateServerWithProposer(kvengine.CreateMemoryStorage(), proposer)
	engineConn := serve(func(server *grpc.Server) {
		kvv1.RegisterKVServiceServer(server, engine)
		enginev1.RegisterEngineServiceServer(server, engine)
	})
	s.engine = enginev1.NewEngineServiceClient(engineConn)
	return s, kvv1.NewKVServiceClient(engineConn), proposer
}

func TestLargeSetDoesNotPoisonWAL(t *testing.T) {
	s, client, proposer := recoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	// This Set previously fit Propose, but exceeded Apply's receive limit by 4 bytes.
	in := &kvv1.SetRequest{Key: []byte("k"), Value: make([]byte, 4*1024*1024-27)}
	if _, err := client.Set(ctx, in); err != nil {
		t.Fatal(err)
	}
	got, err := client.Get(ctx, &kvv1.GetRequest{Key: in.Key})
	if err != nil || len(got.GetValue()) != len(in.Value) {
		t.Fatalf("Get: %v, size=%d", err, len(got.GetValue()))
	}
	before := s.wal.LastOffset()
	if _, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte("large"), Value: make([]byte, replication.MaxPayloadSize)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized Set: %v", err)
	}
	if _, err := proposer.Propose(ctx, &replv1.ProposeRequest{Payload: make([]byte, replication.MaxPayloadSize+1)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized Propose: %v", err)
	}
	if s.wal.LastOffset() != before {
		t.Fatal("rejected payload was written to WAL")
	}
	if _, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte("small"), Value: []byte("ok")}); err != nil {
		t.Fatalf("small write after rejection: %v", err)
	}
	// Rejections must also leave background recovery usable.
	s.unavailable()
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
}

type blockedApply struct {
	enginev1.EngineServiceClient
	started chan struct{}
	release chan struct{}
}

func (e blockedApply) Apply(ctx context.Context, in *enginev1.ApplyRequest, options ...grpc.CallOption) (*enginev1.ApplyResponse, error) {
	close(e.started)
	select {
	case <-e.release:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return e.EngineServiceClient.Apply(ctx, in, options...)
}

func TestClientRequestsUnavailableUntilReplayCompletes(t *testing.T) {
	s, client, _ := recoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := make([]byte, 20+len("persisted")+len("value"))
	binary.BigEndian.PutUint32(payload, 0x6f825fba)
	binary.BigEndian.PutUint32(payload[12:], uint32(len("persisted")))
	binary.BigEndian.PutUint32(payload[16:], uint32(len("value")))
	copy(payload[20:], "persisted")
	copy(payload[20+len("persisted"):], "value")
	appendTestWAL(t, s.wal, payload)
	if _, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("persisted")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Get before recovery: %v", err)
	}
	blocked := blockedApply{EngineServiceClient: s.engine, started: make(chan struct{}), release: make(chan struct{})}
	s.engine = blocked
	done := make(chan error, 1)
	go func() { done <- s.recover(ctx) }()
	select {
	case <-blocked.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, getErr := client.Get(ctx, &kvv1.GetRequest{Key: []byte("persisted")})
	_, setErr := client.Set(ctx, &kvv1.SetRequest{Key: []byte("new"), Value: []byte("value")})
	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if status.Code(getErr) != codes.Unavailable || status.Code(setErr) != codes.Unavailable {
		t.Fatalf("requests during replay: Get=%v Set=%v", getErr, setErr)
	}
	got, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("persisted")})
	if err != nil || string(got.GetValue()) != "value" {
		t.Fatalf("Get after recovery: %v %v", got, err)
	}
}

type slowApplyEngine struct{ recordingEngine }

func (e *slowApplyEngine) Apply(ctx context.Context, in *enginev1.ApplyRequest, options ...grpc.CallOption) (*enginev1.ApplyResponse, error) {
	timer := time.NewTimer(350 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return e.recordingEngine.Apply(ctx, in, options...)
}

func TestRecoveryMayTakeLongerThanOneRPCDeadline(t *testing.T) {
	w := openTestWAL(t, filepath.Join(t.TempDir(), "wal"))
	for i := 0; i < 4; i++ {
		appendTestWAL(t, w, []byte{1, 2, 3})
	}
	e := &slowApplyEngine{}
	s := &proposalServer{wal: w, engine: e}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); s.recoverEngine(ctx, testLogger()) }()
	defer func() { cancel(); <-done }()
	for {
		s.mu.Lock()
		ready := s.ready
		s.mu.Unlock()
		if ready {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("recovery did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.getOffsetCalls != 1 || e.offset != 12 {
		t.Fatalf("recovery retried a healthy batch: calls=%d offset=%d", e.getOffsetCalls, e.offset)
	}
}

func TestCanceledSetReadyCannotDisableRecoveredEngine(t *testing.T) {
	var delayed atomic.Bool
	release := make(chan struct{})
	finished := make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseRequest()
	interceptor := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == enginev1.EngineService_SetReady_FullMethodName && !req.(*enginev1.SetReadyRequest).Ready && delayed.CompareAndSwap(false, true) {
			// The first request reaches its handler after a subsequent recovery finishes.
			<-ctx.Done()
			<-release
			defer close(finished)
		}
		return handler(ctx, req)
	})
	s, client, _ := recoveryFixture(t, interceptor)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.recover(ctx); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("delayed recovery: %v", err)
	}
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	releaseRequest()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("missing")}); status.Code(err) != codes.NotFound {
		t.Fatalf("late canceled request changed readiness: %v", err)
	}
	if _, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")}); err != nil {
		t.Fatalf("Set after late canceled request: %v", err)
	}
}

func TestProposeTimesOutApplyWithoutClientDeadline(t *testing.T) {
	var stalled atomic.Bool
	interceptor := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == enginev1.EngineService_Apply_FullMethodName && stalled.CompareAndSwap(false, true) {
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return handler(ctx, req)
	})
	s, client, _ := recoveryFixture(t, interceptor)
	ctx, cancel := context.WithCancel(context.Background()) // Deliberately no deadline.
	defer cancel()
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")})
		done <- err
	}()
	select {
	case err := <-done:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("stalled Apply: %v", err)
		}
	case <-time.After(3 * engineRPCTimeout):
		cancel()
		<-done
		t.Fatal("Apply blocked despite the service timeout")
	}
	if s.wal.LastOffset() != 28 {
		t.Fatal("timed out Apply lost its durable event")
	}
	if err := s.recover(ctx); err != nil {
		t.Fatalf("recovery after timeout: %v", err)
	}
	got, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("key")})
	if err != nil || string(got.GetValue()) != "value" {
		t.Fatalf("recovered value: %v, %v", got, err)
	}
}
