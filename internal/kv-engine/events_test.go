package kvengine

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSignedArithmeticBoundaries(t *testing.T) {
	tests := []struct {
		name, value    string
		delta, want    int64
		decr, overflow bool
	}{
		{name: "negative value", value: "-5", delta: 3, want: -2},
		{name: "negative increment", value: "5", delta: -7, want: -2},
		{name: "below zero", value: "1", delta: 2, decr: true, want: -1},
		{name: "negative decrement", value: "-5", delta: -7, decr: true, want: 2},
		{name: "increment max", value: "9223372036854775807", delta: 1, overflow: true},
		{name: "increment min", value: "-9223372036854775808", delta: -1, overflow: true},
		{name: "decrement min", value: "-9223372036854775808", delta: 1, decr: true, overflow: true},
		{name: "decrement max", value: "9223372036854775807", delta: -1, decr: true, overflow: true},
		{name: "min delta increment", value: "0", delta: math.MinInt64, want: math.MinInt64},
		{name: "min delta decrement", value: "0", delta: math.MinInt64, decr: true, overflow: true},
		{name: "min minus min", value: "-9223372036854775808", delta: math.MinInt64, decr: true, want: 0},
		{name: "minus one minus min", value: "-1", delta: math.MinInt64, decr: true, want: math.MaxInt64},
		{name: "max plus min", value: "9223372036854775807", delta: math.MinInt64, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &TestTimeSource{t: 1000}
			st := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: clock}
			if err := st.Set("counter", tt.value, 1060); err != nil {
				t.Fatal(err)
			}
			operation := st.Incr
			if tt.decr {
				operation = st.Decr
			}
			got, err := operation("counter", tt.delta, clock.Now())
			if tt.overflow {
				if err != ErrOverflow {
					t.Fatalf("got %d, %v; expected overflow", got, err)
				}
				stored, _, _ := st.Get("counter")
				if stored != tt.value {
					t.Fatal("overflow changed the value")
				}
			} else if err != nil || got != tt.want {
				t.Fatalf("got %d, %v; want %d", got, err, tt.want)
			}
			if st.values["counter"].expiresAt != 1060 {
				t.Fatal("expiration changed")
			}
		})
	}
}

func TestMutationPayloadValidation(t *testing.T) {
	for _, kind := range []uint32{setEventMagic, addEventMagic, replaceEventMagic, deleteEventMagic, incrEventMagic, decrEventMagic} {
		payload, err := encodeMutation(mutation{kind: kind, key: []byte("k"), value: []byte("v"), now: 1000, delta: math.MinInt64})
		if err != nil {
			t.Fatal(err)
		}
		badLength := bytes.Clone(payload)
		binary.BigEndian.PutUint32(badLength[len(payload)-10:], math.MaxUint32)
		for _, malformed := range [][]byte{payload[:3], payload[:len(payload)-1], append(bytes.Clone(payload), 0), badLength} {
			s := CreateServer(CreateMemoryStorage())
			if _, err := s.Apply(context.Background(), &enginev1.ApplyRequest{Offset: uint64(len(malformed)), Payload: malformed}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("kind %x: accepted malformed payload: %v", kind, err)
			}
			if s.appliedOffset != 0 {
				t.Fatal("invalid command advanced the offset")
			}
		}
	}
}

type replayProposer struct {
	engine            *server
	offset            uint64
	payloads, results [][]byte
}

func (p *replayProposer) Propose(ctx context.Context, in *replv1.ProposeRequest, _ ...grpc.CallOption) (*replv1.ProposeResponse, error) {
	p.offset += uint64(len(in.Payload))
	resp, err := p.engine.Apply(ctx, &enginev1.ApplyRequest{Offset: p.offset, Payload: in.Payload})
	if err != nil {
		return nil, err
	}
	p.payloads = append(p.payloads, bytes.Clone(in.Payload))
	p.results = append(p.results, bytes.Clone(resp.Result))
	return &replv1.ProposeResponse{Result: resp.Result}, nil
}

func TestMutationReplayUsesRecordedTimeAndPreservesResults(t *testing.T) {
	clock := &TestTimeSource{t: 1000}
	storage := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: clock}
	s := newServer(storage, clock)
	p := &replayProposer{engine: s}
	s.proposer = p
	ctx := context.Background()
	if _, err := s.Set(ctx, &kvv1.SetRequest{Key: []byte("expired-later"), Value: []byte("old"), Ttl: 1}); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("expired-later"), Value: []byte("must-not-appear")}); err != nil || r.GetSuccess() {
		t.Fatalf("conditional no-op: %v %v", r, err)
	}
	if r, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("counter"), Value: []byte("-5"), Ttl: 60}); err != nil || !r.GetSuccess() {
		t.Fatalf("Add: %v %v", r, err)
	}
	if r, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("counter"), Delta: 3}); err != nil || r.GetValue() != -2 {
		t.Fatalf("Incr: %v %v", r, err)
	}
	if r, err := s.Decr(ctx, &kvv1.DecrRequest{Key: []byte("counter"), Delta: -4}); err != nil || r.GetValue() != 2 {
		t.Fatalf("Decr: %v %v", r, err)
	}
	if r, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("counter"), Value: []byte("text"), Ttl: 60}); err != nil || !r.GetSuccess() {
		t.Fatalf("Replace: %v %v", r, err)
	}
	if _, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("counter"), Delta: 1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("business rejection: %v", err)
	}
	if r, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("counter")}); err != nil || !r.GetSuccess() {
		t.Fatalf("Delete: %v %v", r, err)
	}
	if r, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("missing"), Value: []byte("ignored")}); err != nil || r.GetSuccess() {
		t.Fatalf("Replace missing: %v %v", r, err)
	}
	clock.SetTime(2000)
	freshStorage := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: clock}
	fresh := newServer(freshStorage, clock)
	var offset uint64
	for i, payload := range p.payloads {
		offset += uint64(len(payload))
		req := &enginev1.ApplyRequest{Offset: offset, Payload: payload}
		resp, err := fresh.Apply(ctx, req)
		if err != nil || !bytes.Equal(resp.GetResult(), p.results[i]) {
			t.Fatalf("replay entry %d: %v %v", i, resp, err)
		}
		duplicate, err := fresh.Apply(ctx, req)
		if err != nil || !bytes.Equal(resp.GetResult(), duplicate.GetResult()) {
			t.Fatalf("duplicate entry %d: %v %v", i, duplicate, err)
		}
	}
	if len(freshStorage.values) != len(storage.values) {
		t.Fatal("replay changed the key set")
	}
	for key, value := range storage.values {
		if freshStorage.values[key] != value {
			t.Fatalf("replay changed %q", key)
		}
	}
	if _, err := fresh.Get(ctx, &kvv1.GetRequest{Key: []byte("expired-later")}); status.Code(err) != codes.NotFound {
		t.Fatalf("replay resurrected expired key: %v", err)
	}
	if offset != s.appliedOffset || offset != fresh.appliedOffset {
		t.Fatal("no-op or rejection lost an offset")
	}
}

func TestExtendedMethodsRejectRequestsDuringRecovery(t *testing.T) {
	s := CreateServer(CreateMemoryStorage())
	ctx := context.Background()
	if _, err := s.SetReady(ctx, &enginev1.SetReadyRequest{}); err != nil {
		t.Fatal(err)
	}
	checks := []func() error{
		func() error { _, err := s.GetExpireTime(ctx, &kvv1.GetExpireTimeRequest{Key: []byte("k")}); return err },
		func() error { _, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("k")}); return err },
		func() error { _, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("k")}); return err },
		func() error { _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("k")}); return err },
		func() error { _, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("k"), Delta: 1}); return err },
		func() error { _, err := s.Decr(ctx, &kvv1.DecrRequest{Key: []byte("k"), Delta: 1}); return err },
	}
	for i, check := range checks {
		if err := check(); status.Code(err) != codes.Unavailable {
			t.Fatalf("method %d: %v", i, err)
		}
	}
}

func TestFailedExtendedProposalsDoNotChangeStorage(t *testing.T) {
	ctx := context.Background()
	clock := &TestTimeSource{t: 1000}
	storage := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: clock}
	if err := storage.Set("key", "5", 1060); err != nil {
		t.Fatal(err)
	}
	s := newServer(storage, clock)
	var calls int
	s.proposer = callbackProposer{propose: func(context.Context, []byte) error {
		calls++
		return status.Error(codes.Unavailable, "repl unavailable")
	}}
	checks := []func() error{
		func() error {
			_, err := s.Add(ctx, &kvv1.AddRequest{Key: []byte("new"), Value: []byte("x")})
			return err
		},
		func() error {
			_, err := s.Replace(ctx, &kvv1.ReplaceRequest{Key: []byte("key"), Value: []byte("x")})
			return err
		},
		func() error { _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: []byte("key")}); return err },
		func() error { _, err := s.Incr(ctx, &kvv1.IncrRequest{Key: []byte("key"), Delta: 1}); return err },
		func() error { _, err := s.Decr(ctx, &kvv1.DecrRequest{Key: []byte("key"), Delta: 1}); return err },
	}
	for i, check := range checks {
		if err := check(); status.Code(err) != codes.Unavailable {
			t.Fatalf("method %d: %v", i, err)
		}
	}
	if calls != len(checks) || s.appliedOffset != 0 || len(storage.values) != 1 || storage.values["key"] != (StorageEntry{value: "5", expiresAt: 1060}) {
		t.Fatal("failed proposal changed storage or skipped Propose")
	}
}
