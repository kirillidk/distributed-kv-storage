package repl

import (
	"context"
	"log/slog"
	"sync"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const engineRPCTimeout = time.Second

// proposalServer treats payloads as opaque bytes. Recovery and proposals share
// a lock to keep Apply offsets contiguous, including after an uncertain RPC result.
type proposalServer struct {
	replv1.UnimplementedReplServiceServer
	mu       sync.Mutex
	wal      *WAL
	engine   enginev1.EngineServiceClient
	ready    bool
	setReady func(bool)
}

func (s *proposalServer) readiness(ready bool) {
	s.ready = ready
	if s.setReady != nil {
		s.setReady(ready)
	}
}

func (s *proposalServer) unavailable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readiness(false)
}

func (s *proposalServer) Propose(ctx context.Context, in *replv1.ProposeRequest) (*replv1.ProposeResponse, error) {
	if len(in.Payload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty payload")
	}
	if len(in.Payload) > replication.MaxPayloadSize {
		return nil, status.Error(codes.ResourceExhausted, "payload is too large")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return nil, status.Error(codes.Unavailable, "engine recovery is in progress")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	offset, err := s.wal.Append(in.Payload)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.apply(ctx, offset, in.Payload); err != nil {
		s.readiness(false)
		return nil, err
	}
	return &replv1.ProposeResponse{}, nil
}

func (s *proposalServer) recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.ready {
		return nil
	}
	if err := s.engineReady(ctx, false); err != nil {
		return err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, engineRPCTimeout)
	state, err := s.engine.GetOffset(rpcCtx, &enginev1.GetOffsetRequest{})
	cancel()
	if err != nil {
		return err
	}
	if err := s.wal.Replay(state.Offset, func(offset uint64, payload []byte) error {
		return s.apply(ctx, offset, payload)
	}); err != nil {
		return err
	}
	if err := s.engineReady(ctx, true); err != nil {
		return err
	}
	s.readiness(true)
	return nil
}

func (s *proposalServer) apply(ctx context.Context, offset uint64, payload []byte) error {
	rpcCtx, cancel := context.WithTimeout(ctx, engineRPCTimeout)
	defer cancel()
	_, err := s.engine.Apply(rpcCtx, &enginev1.ApplyRequest{Offset: offset, Payload: payload})
	return err
}

func (s *proposalServer) engineReady(ctx context.Context, ready bool) error {
	rpcCtx, cancel := context.WithTimeout(ctx, engineRPCTimeout)
	defer cancel()
	_, err := s.engine.SetReady(rpcCtx, &enginev1.SetReadyRequest{Ready: ready})
	return err
}

// Runs for the lifetime of one engine process. Ready engines cause no WAL
// reads or RPCs here. Startup and failed Apply results trigger background replay.
func (s *proposalServer) recoverEngine(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	previousError := ""
	for {
		err := s.recover(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil && err.Error() != previousError {
			logger.Warn("engine WAL recovery pending", "error", err)
			previousError = err.Error()
		} else if err == nil && previousError != "" {
			logger.Info("engine WAL recovery completed")
			previousError = ""
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
