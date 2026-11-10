package repl

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/api/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const engineRPCTimeout = time.Second

// proposalServer treats payloads as opaque bytes. Recovery and proposals share
// a lock to keep Apply offsets contiguous, including after an uncertain RPC result.
type proposalServer struct {
	replv1.UnimplementedReplServiceServer
	mu            sync.Mutex
	wal           *WAL
	raft          *raftNode
	engine        enginev1.EngineServiceClient
	ready         bool
	appliedOffset uint64
	setReady      func(bool)
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
	if in == nil || len(in.Payload) == 0 {
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
	var offset uint64
	var err error
	if s.raft != nil {
		offset, err = s.raft.appendProposal(ctx, in.Payload)
		if err != nil {
			return nil, err
		}
	} else {
		offset, err = s.wal.Append(in.Payload)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	var result []byte
	if s.raft != nil {
		// Earlier proposals may have committed after their clients canceled.
		// Apply the entire missing committed prefix before returning this result.
		result, err = s.applyThrough(ctx, offset)
	} else {
		result, err = s.apply(ctx, offset, in.Payload)
	}
	if err != nil {
		s.readiness(false)
		return nil, err
	}
	return &replv1.ProposeResponse{Result: result}, nil
}

func (s *proposalServer) recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.ready {
		if s.raft != nil {
			_, committed := s.raft.committed()
			if s.appliedOffset < committed {
				if _, err := s.applyThrough(ctx, committed); err != nil {
					s.readiness(false)
					return err
				}
			}
		}
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
	if s.raft != nil {
		_, committed := s.raft.committed()
		s.appliedOffset = state.Offset
		if _, err := s.applyThrough(ctx, committed); err != nil {
			return err
		}
	} else {
		if err := s.wal.Replay(state.Offset, func(offset uint64, payload []byte) error {
			_, err := s.apply(ctx, offset, payload)
			return err
		}); err != nil {
			return err
		}
	}
	if err := s.engineReady(ctx, true); err != nil {
		return err
	}
	s.readiness(true)
	return nil
}

// Called under s.mu, with through no greater than the durable commitOffset.
// WAL reads finish before each engine RPC so a slow Apply cannot block Raft.
func (s *proposalServer) applyThrough(ctx context.Context, through uint64) ([]byte, error) {
	if s.appliedOffset > through {
		return nil, fmt.Errorf("engine offset %d exceeds committed offset %d", s.appliedOffset, through)
	}
	first, err := s.wal.IndexAtOffset(s.appliedOffset)
	if err != nil {
		return nil, err
	}
	last, err := s.wal.IndexAtOffset(through)
	if err != nil {
		return nil, err
	}
	var result []byte
	for index := first + 1; index <= last; index++ {
		entry, err := s.wal.ReadEntry(index)
		if err != nil {
			return nil, err
		}
		result, err = s.apply(ctx, entry.Offset, entry.Payload)
		if err != nil {
			return nil, err
		}
		s.appliedOffset = entry.Offset
	}
	return result, nil
}

func (s *proposalServer) apply(ctx context.Context, offset uint64, payload []byte) ([]byte, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, engineRPCTimeout)
	defer cancel()
	resp, err := s.engine.Apply(rpcCtx, &enginev1.ApplyRequest{Offset: offset, Payload: payload})
	if err != nil {
		return nil, err
	}
	return resp.GetResult(), nil
}

func (s *proposalServer) engineReady(ctx context.Context, ready bool) error {
	rpcCtx, cancel := context.WithTimeout(ctx, engineRPCTimeout)
	defer cancel()
	_, err := s.engine.SetReady(rpcCtx, &enginev1.SetReadyRequest{Ready: ready})
	return err
}

// Runs for the lifetime of one engine process. Startup and failed Apply results
// trigger recovery; new commits are applied on leaders and followers alike.
func (s *proposalServer) recoverEngine(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	previousError := ""
	var commitReady <-chan struct{}
	if s.raft != nil {
		commitReady = s.raft.commitReady
	}
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
		case <-commitReady:
		}
	}
}
