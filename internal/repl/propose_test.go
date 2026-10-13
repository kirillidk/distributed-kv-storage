package repl

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This engine accepts arbitrary bytes so these tests cannot depend on KV events.
type recordingEngine struct {
	mu             sync.Mutex
	offset         uint64
	applyCalls     []*enginev1.ApplyRequest
	getOffsetCalls int
	failNext       bool
	walPath        string
}

func (e *recordingEngine) Apply(_ context.Context, in *enginev1.ApplyRequest, _ ...grpc.CallOption) (*enginev1.ApplyResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.walPath != "" {
		data, err := os.ReadFile(e.walPath)
		if err != nil {
			return nil, err
		}
		var last []byte
		for pos := 0; pos < len(data); {
			if len(data)-pos < 16 {
				return nil, status.Error(codes.Internal, "incomplete WAL at Apply")
			}
			end := pos + 12 + int(binary.BigEndian.Uint32(data[pos:]))
			if end+4 > len(data) || crc32.ChecksumIEEE(data[pos:end]) != binary.BigEndian.Uint32(data[end:]) {
				return nil, status.Error(codes.Internal, "invalid WAL at Apply")
			}
			if binary.BigEndian.Uint64(data[pos+4:]) == in.Offset {
				last = data[pos+12 : end]
			}
			pos = end + 4
		}
		if !bytes.Equal(last, in.Payload) {
			return nil, status.Error(codes.Internal, "payload was not persisted before Apply")
		}
	}
	e.applyCalls = append(e.applyCalls, &enginev1.ApplyRequest{Offset: in.Offset, Payload: bytes.Clone(in.Payload)})
	if e.failNext {
		e.failNext = false
		return nil, status.Error(codes.Unavailable, "temporary failure")
	}
	if in.Offset > e.offset {
		if in.Offset-e.offset != uint64(len(in.Payload)) {
			return nil, status.Error(codes.FailedPrecondition, "offset gap")
		}
		e.offset = in.Offset
	}
	return &enginev1.ApplyResponse{}, nil
}

func (e *recordingEngine) GetOffset(context.Context, *enginev1.GetOffsetRequest, ...grpc.CallOption) (*enginev1.GetOffsetResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.getOffsetCalls++
	return &enginev1.GetOffsetResponse{Offset: e.offset}, nil
}

func (e *recordingEngine) SetReady(context.Context, *enginev1.SetReadyRequest, ...grpc.CallOption) (*enginev1.SetReadyResponse, error) {
	return &enginev1.SetReadyResponse{}, nil
}

func TestProposePersistsOpaqueBytesAndDoesNotReplayPerRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w := openTestWAL(t, path)
	e := &recordingEngine{walPath: path}
	s := &proposalServer{wal: w, engine: e}
	ctx := context.Background()
	if _, err := s.Propose(ctx, &replv1.ProposeRequest{Payload: []byte("before recovery")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Propose before recovery: %v", err)
	}
	if w.LastOffset() != 0 {
		t.Fatal("unready proposal was logged")
	}
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{{0, 255, 7}, []byte("unknown-command"), {128, 0}} {
		if _, err := s.Propose(ctx, &replv1.ProposeRequest{Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	if e.getOffsetCalls != 1 || len(e.applyCalls) != 3 {
		t.Fatalf("requests caused replay: GetOffset=%d Apply=%d", e.getOffsetCalls, len(e.applyCalls))
	}
	if _, err := s.Propose(ctx, &replv1.ProposeRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty proposal: %v", err)
	}
	s.unavailable()
	e.offset = 0
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	if e.offset != w.LastOffset() || len(e.applyCalls) != 6 {
		t.Fatalf("restart did not replay all records: %v", e)
	}
}

func TestFailedApplyIsRecoveredInBackground(t *testing.T) {
	w := openTestWAL(t, filepath.Join(t.TempDir(), "wal"))
	e := &recordingEngine{failNext: true}
	s := &proposalServer{wal: w, engine: e}
	if err := s.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Propose(context.Background(), &replv1.ProposeRequest{Payload: []byte{1, 2, 3}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("failed Apply: %v", err)
	}
	if s.ready || w.LastOffset() != 3 {
		t.Fatal("failed Apply must retain the payload and block new proposals")
	}
	if _, err := s.Propose(context.Background(), &replv1.ProposeRequest{Payload: []byte{4}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("proposal after failed Apply: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.recoverEngine(ctx, testLogger()) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		ready := s.ready
		s.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background recovery did not complete")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := s.Propose(context.Background(), &replv1.ProposeRequest{Payload: []byte{4}}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.offset != 4 {
		t.Fatalf("offset=%d", e.offset)
	}
}
