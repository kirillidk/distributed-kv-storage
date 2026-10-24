package kvengine

import (
	"context"
	"encoding/binary"
	"testing"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type callbackProposer struct {
	propose func(context.Context, []byte) error
}

func (p callbackProposer) Propose(ctx context.Context, in *replv1.ProposeRequest, _ ...grpc.CallOption) (*replv1.ProposeResponse, error) {
	if err := p.propose(ctx, in.Payload); err != nil {
		return nil, err
	}
	return &replv1.ProposeResponse{}, nil
}

func TestSetChangesStorageOnlyThroughProposeAndApply(t *testing.T) {
	clock := &TestTimeSource{t: 100}
	storage := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: clock}
	s := newServer(storage, clock)
	var calls int
	s.proposer = callbackProposer{propose: func(ctx context.Context, payload []byte) error {
		calls++
		if _, _, err := storage.Get("key"); err != ErrKeyNotFound {
			t.Fatalf("storage changed before Apply: %v", err)
		}
		if binary.BigEndian.Uint32(payload) != setEventMagic || binary.BigEndian.Uint64(payload[4:]) != 160 {
			t.Fatalf("invalid event: %x", payload)
		}
		if _, err := s.Apply(ctx, &enginev1.ApplyRequest{Offset: uint64(len(payload)), Payload: payload}); err != nil {
			return err
		}
		return nil
	}}
	if _, err := s.Set(context.Background(), &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value"), Ttl: 60}); err != nil {
		t.Fatal(err)
	}
	value, expires, err := storage.Get("key")
	if err != nil || value != "value" || expires != 160 || calls != 1 || s.appliedOffset != 28 {
		t.Fatalf("result=%q expiry=%d offset=%d calls=%d err=%v", value, expires, s.appliedOffset, calls, err)
	}
}

func TestFailedProposalDoesNotMutateStorage(t *testing.T) {
	s := CreateServerWithProposer(CreateMemoryStorage(), callbackProposer{propose: func(context.Context, []byte) error { return status.Error(codes.Unavailable, "repl unavailable") }})
	if _, err := s.SetReady(context.Background(), &enginev1.SetReadyRequest{Ready: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Set(context.Background(), &kvv1.SetRequest{Key: []byte("key"), Value: []byte("value")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Set: %v", err)
	}
	if _, _, err := s.storage.Get("key"); err != ErrKeyNotFound {
		t.Fatalf("proposal failure changed storage: %v", err)
	}
	if s.appliedOffset != 0 {
		t.Fatal("proposal failure changed applied offset")
	}
}

func TestSetReadyGatesKVAndHealthButAllowsReplay(t *testing.T) {
	client, engine, healthClient, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()
	ctx := context.Background()
	if _, err := engine.SetReady(ctx, &enginev1.SetReadyRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("key")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Get during recovery: %v", err)
	}
	if _, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte("key"), Value: []byte("new")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Set during recovery: %v", err)
	}
	state, err := healthClient.Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err != nil || state.GetStatus() != healthgrpc.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health during recovery: %v %v", state, err)
	}
	payload, err := encodeSetEvent([]byte("key"), []byte("persisted"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Apply(ctx, &enginev1.ApplyRequest{Offset: uint64(len(payload)), Payload: payload}); err != nil {
		t.Fatal(err)
	}
	offset, err := engine.GetOffset(ctx, &enginev1.GetOffsetRequest{})
	if err != nil || offset.GetOffset() != uint64(len(payload)) {
		t.Fatalf("replay offset: %v %v", offset, err)
	}
	if _, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("key")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Get before recovery completed: %v", err)
	}
	if _, err := engine.SetReady(ctx, &enginev1.SetReadyRequest{Ready: true}); err != nil {
		t.Fatal(err)
	}
	got, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("key")})
	if err != nil || string(got.GetValue()) != "persisted" {
		t.Fatalf("Get after recovery: %v %v", got, err)
	}
	state, err = healthClient.Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err != nil || state.GetStatus() != healthgrpc.HealthCheckResponse_SERVING {
		t.Fatalf("health after recovery: %v %v", state, err)
	}
}
