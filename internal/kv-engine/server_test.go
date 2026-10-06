package kvengine

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	kvv2 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v2"
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
func newTestServer(t *testing.T, storage Storage) (kvv2.KVServiceClient, healthgrpc.HealthClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	if err != nil {
		t.FailNow()
	}

	grpcServer := grpc.NewServer()
	srv := CreateServer(storage)
	kvv2.RegisterKVServiceServer(grpcServer, &srv)

	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(grpcServer, healthcheck)
	healthcheck.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)

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

	return kvv2.NewKVServiceClient(conn), healthgrpc.NewHealthClient(conn), cleanup
}

func TestSetGetRoundTrip(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Set(ctx, &kvv2.SetRequest{Key: []byte("key1"), Value: []byte("value1")})
	assert.NoError(t, err)
	assert.Equal(t, codes.OK, status.Code(err))

	resp, err := client.Get(ctx, &kvv2.GetRequest{Key: []byte("key1")})
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

	_, err := client.Set(ctx, &kvv2.SetRequest{Key: []byte(""), Value: []byte("value")})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetEmptyKey(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &kvv2.GetRequest{Key: []byte("")})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetNotFound(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &kvv2.GetRequest{Key: []byte("missing")})
	assert.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestHealthCheck(t *testing.T) {
	_, healthClient, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := healthClient.Check(ctx, &healthgrpc.HealthCheckRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, healthgrpc.HealthCheckResponse_SERVING, resp.GetStatus())
	}
}

// encodeSetPayload builds the wire payload Apply expects for a Set:
// uvarint(cmd) | uvarint(ttl) | uvarint(len(key)) | uvarint(len(value)) | key | value
func encodeSetPayload(ttl uint64, key, value []byte) []byte {
	payload := binary.AppendUvarint(nil, setCommandID)
	payload = binary.AppendUvarint(payload, ttl)
	payload = binary.AppendUvarint(payload, uint64(len(key)))
	payload = binary.AppendUvarint(payload, uint64(len(value)))
	payload = append(payload, key...)
	return append(payload, value...)
}

func TestStatusInitialOffsetIsZero(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(0), resp.GetOffset())
	}
}

func TestApplySuccesfulCase(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("applied-key"), []byte("applied-value"))
	wantOffset := uint64(len(payload))

	_, err := client.Apply(ctx, &kvv2.ApplyRequest{Offset: 0, Payload: payload})
	assert.NoError(t, err)

	got, err := client.Get(ctx, &kvv2.GetRequest{Key: []byte("applied-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("applied-value"), got.GetValue())
	}

	statusResp, err := client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, wantOffset, statusResp.GetOffset())
	}

	// The next command must be accepted when its offset lines up with where
	// the server currently is.
	second := encodeSetPayload(0, []byte("second-key"), []byte("second-value"))

	_, err = client.Apply(ctx, &kvv2.ApplyRequest{Offset: wantOffset, Payload: second})
	assert.NoError(t, err)

	got, err = client.Get(ctx, &kvv2.GetRequest{Key: []byte("second-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("second-value"), got.GetValue())
	}

	statusResp, err = client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, wantOffset+uint64(len(second)), statusResp.GetOffset())
	}
}

func TestApplyIgnoresDuplicateOffset(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("dup-key"), []byte("dup-value"))

	_, err := client.Apply(ctx, &kvv2.ApplyRequest{Offset: 0, Payload: payload})
	assert.NoError(t, err)

	statusResp, err := client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	var offsetAfterFirst uint64
	if err == nil {
		offsetAfterFirst = statusResp.GetOffset()
		assert.Equal(t, uint64(len(payload)), offsetAfterFirst)
	}

	// Replay the very same command at the same offset. It must be ignored:
	// no error, and the offset must not advance again.
	_, err = client.Apply(ctx, &kvv2.ApplyRequest{Offset: 0, Payload: payload})
	assert.NoError(t, err)

	statusResp, err = client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, offsetAfterFirst, statusResp.GetOffset())
	}

	// Replaying at an offset already passed must not corrupt the stored value.
	got, err := client.Get(ctx, &kvv2.GetRequest{Key: []byte("dup-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("dup-value"), got.GetValue())
	}
}

func TestApplyMalformedPayloads(t *testing.T) {
	valid := encodeSetPayload(0, []byte("k"), []byte("v"))

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "nil payload", payload: nil},
		{name: "empty payload", payload: []byte{}},
		{name: "command only, no fields", payload: []byte{setCommandID}},
		{name: "command and ttl only", payload: []byte{setCommandID, 0x00}},
		{name: "no value length", payload: []byte{setCommandID, 0x00, 0x01}},
		{name: "no key bytes", payload: []byte{setCommandID, 0x00, 0x01, 0x01}},
		{name: "no value bytes", payload: []byte{setCommandID, 0x00, 0x01, 0x01, 'k'}},
		{name: "unknown command type", payload: []byte{0x02, 0x00, 0x01, 0x01, 'k', 'v'}},
		{name: "key length longer than payload", payload: []byte{setCommandID, 0x00, 0x7F, 0x01, 'k', 'v'}},
		{name: "value length longer than payload", payload: []byte{setCommandID, 0x00, 0x01, 0x7F, 'k', 'v'}},
		{name: "unterminated uvarint", payload: bytes.Repeat([]byte{0xFF}, 10)},
	}

	// Every strict prefix of a known-good payload is missing a trailing
	// field, so each one must be rejected too.
	for i := 0; i < len(valid); i++ {
		tests = append(tests, struct {
			name    string
			payload []byte
		}{
			name:    fmt.Sprintf("truncated to %d bytes", i),
			payload: append([]byte(nil), valid[:i]...),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _, cleanup := newTestServer(t, CreateMemoryStorage())
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := client.Apply(ctx, &kvv2.ApplyRequest{Offset: 0, Payload: tt.payload})
			assert.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err),
				"payload %v should be rejected as InvalidArgument", tt.payload)

			statusResp, err := client.Status(ctx, &kvv2.StatusRequest{})
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, uint64(0), statusResp.GetOffset(),
					"a rejected payload must not advance the offset")
			}
		})
	}
}

func TestApplyFailedSetDoesNotAdvanceOffset(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A payload with a valid header but an empty key makes Set fail.
	payload := encodeSetPayload(0, []byte(""), []byte("value"))

	_, err := client.Apply(ctx, &kvv2.ApplyRequest{Offset: 0, Payload: payload})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	statusResp, err := client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(0), statusResp.GetOffset())
	}

	// A failing command must not poison the offset: a following valid Apply
	// still works and advances from the original position.
	good := encodeSetPayload(0, []byte("good-key"), []byte("good-value"))
	_, err = client.Apply(ctx, &kvv2.ApplyRequest{Offset: 0, Payload: good})
	assert.NoError(t, err)

	got, err := client.Get(ctx, &kvv2.GetRequest{Key: []byte("good-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("good-value"), got.GetValue())
	}

	statusResp, err = client.Status(ctx, &kvv2.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(len(good)), statusResp.GetOffset())
	}
}

func TestApplyRaces(t *testing.T) {
	client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("k"), []byte("v"))

	wg := new(sync.WaitGroup)
	wg.Add(10)

	for range 10 {
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			offset := 0
			for range 1000 {
				if rand.IntN(2) == 0 {
					_, _ = client.Apply(ctx, &kvv2.ApplyRequest{
						Offset:  uint64(offset),
						Payload: payload,
					})
					offset += 1
				} else {
					_, _ = client.Status(ctx, &kvv2.StatusRequest{})
				}
			}
		}(wg)
	}

	wg.Wait()
}
