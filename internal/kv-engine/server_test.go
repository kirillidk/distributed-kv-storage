package kvengine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
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
func newTestServer(t *testing.T, storage Storage) (kvv1.KVServiceClient, enginev1.EngineServiceClient, healthgrpc.HealthClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	if err != nil {
		t.FailNow()
	}

	grpcServer := grpc.NewServer()
	srv := CreateServer(storage)
	kvv1.RegisterKVServiceServer(grpcServer, &srv)
	enginev1.RegisterEngineServiceServer(grpcServer, &srv)

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

	return kvv1.NewKVServiceClient(conn), enginev1.NewEngineServiceClient(conn), healthgrpc.NewHealthClient(conn), cleanup
}

func TestSetGetRoundTrip(t *testing.T) {
	client, _, _, cleanup := newTestServer(t, CreateMemoryStorage())
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
	client, _, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Set(ctx, &kvv1.SetRequest{Key: []byte(""), Value: []byte("value")})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetEmptyKey(t *testing.T) {
	client, _, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("")})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetNotFound(t *testing.T) {
	client, _, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &kvv1.GetRequest{Key: []byte("missing")})
	assert.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestHealthCheck(t *testing.T) {
	_, _, healthClient, cleanup := newTestServer(t, CreateMemoryStorage())
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
// uint32(cmd) | uint64(expiresAt) | uint32(len(key)) | uint32(len(value)) | key | value
func encodeSetPayload(expiresAt uint64, key, value []byte) []byte {
	order := binary.BigEndian
	payload := order.AppendUint32(nil, setEventMagic)
	payload = order.AppendUint64(payload, expiresAt)
	payload = order.AppendUint32(payload, uint32(len(key)))
	payload = order.AppendUint32(payload, uint32(len(value)))
	payload = append(payload, key...)
	return append(payload, value...)
}

func TestStatusInitialOffsetIsZero(t *testing.T) {
	_, client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(0), resp.GetOffset())
	}
}

func TestApplySuccesfulCase(t *testing.T) {
	kvClient, engineClient, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("applied-key"), []byte("applied-value"))
	wantOffset := uint64(len(payload))

	_, err := engineClient.Apply(ctx, &enginev1.ApplyRequest{Offset: wantOffset, Payload: payload})
	assert.NoError(t, err)

	got, err := kvClient.Get(ctx, &kvv1.GetRequest{Key: []byte("applied-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("applied-value"), got.GetValue())
	}

	statusResp, err := engineClient.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, wantOffset, statusResp.GetOffset())
	}

	// The next command must be accepted when its offset continues exactly
	// where the previous one left off.
	second := encodeSetPayload(0, []byte("second-key"), []byte("second-value"))
	secondOffset := wantOffset + uint64(len(second))

	_, err = engineClient.Apply(ctx, &enginev1.ApplyRequest{Offset: secondOffset, Payload: second})
	assert.NoError(t, err)

	got, err = kvClient.Get(ctx, &kvv1.GetRequest{Key: []byte("second-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("second-value"), got.GetValue())
	}

	statusResp, err = engineClient.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, secondOffset, statusResp.GetOffset())
	}
}

func TestApplyIgnoresDuplicateOffset(t *testing.T) {
	kvClient, engineClient, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("dup-key"), []byte("dup-value"))
	offset := uint64(len(payload))

	_, err := engineClient.Apply(ctx, &enginev1.ApplyRequest{Offset: offset, Payload: payload})
	assert.NoError(t, err)

	statusResp, err := engineClient.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	var offsetAfterFirst uint64
	if err == nil {
		offsetAfterFirst = statusResp.GetOffset()
		assert.Equal(t, offset, offsetAfterFirst)
	}

	// Replay the very same command at the same offset. It must be ignored:
	// no error, and the offset must not advance again.
	_, err = engineClient.Apply(ctx, &enginev1.ApplyRequest{Offset: offset, Payload: payload})
	assert.NoError(t, err)

	statusResp, err = engineClient.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, offsetAfterFirst, statusResp.GetOffset())
	}

	// Replaying at an offset already passed must not corrupt the stored value.
	got, err := kvClient.Get(ctx, &kvv1.GetRequest{Key: []byte("dup-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("dup-value"), got.GetValue())
	}
}

func TestApplyMalformedPayloads(t *testing.T) {
	valid := encodeSetPayload(0, []byte("k"), []byte("v"))
	order := binary.BigEndian

	header := func(cmd uint32, expiresAt uint64, keySize, valueSize uint32) []byte {
		payload := order.AppendUint32(nil, cmd)
		payload = order.AppendUint64(payload, expiresAt)
		payload = order.AppendUint32(payload, keySize)
		return binary.BigEndian.AppendUint32(payload, valueSize)
	}

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "unknown command type", payload: append(header(0xdeadbeef, 0, 1, 1), 'k', 'v')},
		{name: "key size longer than payload", payload: append(header(setEventMagic, 0, 0xFF, 1), 'k', 'v')},
		{name: "value size longer than payload", payload: append(header(setEventMagic, 0, 1, 0xFF), 'k', 'v')},
		{name: "key size overflowing uint32", payload: header(setEventMagic, 0, ^uint32(0), 1)},
		{name: "value size overflowing uint32", payload: header(setEventMagic, 0, 1, ^uint32(0))},
		{name: "no key bytes", payload: header(setEventMagic, 0, 1, 1)},
		{name: "no value bytes", payload: append(header(setEventMagic, 0, 1, 1), 'k')},
		{name: "key size zero with trailing bytes", payload: append(header(setEventMagic, 0, 0, 1), 'v')},
	}

	// Every strict prefix of a known-good payload is missing a trailing
	// field, so each one must be rejected too. The zero length prefix is
	// skipped: it never reaches the parser.
	for i := 1; i < len(valid); i++ {
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
			_, client, _, cleanup := newTestServer(t, CreateMemoryStorage())
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := client.Apply(ctx, &enginev1.ApplyRequest{
				Offset:  uint64(len(tt.payload)),
				Payload: tt.payload,
			})
			assert.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err),
				"payload %v should be rejected as InvalidArgument", tt.payload)

			statusResp, err := client.Status(ctx, &enginev1.StatusRequest{})
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, uint64(0), statusResp.GetOffset(),
					"a rejected payload must not advance the offset")
			}
		})
	}
}

func TestApplyInvalidOffset(t *testing.T) {
	kvClient, client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("offset-key"), []byte("offset-value"))

	// An offset ahead of the server that is not exactly the end of the command
	// leaves a gap, which must be reported rather than silently accepted.
	for _, offset := range []uint64{
		uint64(len(payload)) + 1,
		uint64(len(payload)) + 100,
		^uint64(0),
	} {
		_, err := client.Apply(ctx, &enginev1.ApplyRequest{Offset: offset, Payload: payload})
		assert.Error(t, err, "offset %d should be rejected", offset)
		assert.Equal(t, codes.InvalidArgument, status.Code(err),
			"offset %d should be InvalidArgument", offset)
	}

	// None of that may have moved the offset or written the key.
	statusResp, err := client.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(0), statusResp.GetOffset())
	}

	// The correct offset still works afterwards.
	_, err = client.Apply(ctx, &enginev1.ApplyRequest{Offset: uint64(len(payload)), Payload: payload})
	assert.NoError(t, err)

	got, err := kvClient.Get(ctx, &kvv1.GetRequest{Key: []byte("offset-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("offset-value"), got.GetValue())
	}
}

func TestApplyFailedSetDoesNotAdvanceOffset(t *testing.T) {
	kvClient, engineClient, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A payload with a valid header but an empty key makes Set fail.
	payload := encodeSetPayload(0, []byte(""), []byte("value"))

	_, err := engineClient.Apply(ctx, &enginev1.ApplyRequest{Offset: uint64(len(payload)), Payload: payload})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	statusResp, err := engineClient.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(0), statusResp.GetOffset())
	}

	// A failing command must not poison the offset: a following valid Apply
	// still works and advances from the original position.
	good := encodeSetPayload(0, []byte("good-key"), []byte("good-value"))
	_, err = engineClient.Apply(ctx, &enginev1.ApplyRequest{Offset: uint64(len(good)), Payload: good})
	assert.NoError(t, err)

	got, err := kvClient.Get(ctx, &kvv1.GetRequest{Key: []byte("good-key")})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, []byte("good-value"), got.GetValue())
	}

	statusResp, err = engineClient.Status(ctx, &enginev1.StatusRequest{})
	assert.NoError(t, err)
	if err == nil {
		assert.Equal(t, uint64(len(good)), statusResp.GetOffset())
	}
}

func TestApplyRaces(t *testing.T) {
	_, client, _, cleanup := newTestServer(t, CreateMemoryStorage())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	payload := encodeSetPayload(0, []byte("k"), []byte("v"))

	// Offset is the position after applying, so a caller tracking its own
	// stream position advances by the length of each payload it sends.
	wg := new(sync.WaitGroup)
	wg.Add(10)

	for range 10 {
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			offset := 0
			for range 1000 {
				if rand.IntN(2) == 0 {
					_, _ = client.Apply(ctx, &enginev1.ApplyRequest{
						Offset:  uint64(offset),
						Payload: payload,
					})
					offset += 1
				} else {
					_, _ = client.Status(ctx, &enginev1.StatusRequest{})
				}
			}
		}(wg)
	}

	wg.Wait()
}
