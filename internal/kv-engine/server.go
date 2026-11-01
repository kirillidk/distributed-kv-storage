package kvengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/api/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type server struct {
	kvv1.UnimplementedKVServiceServer
	enginev1.UnimplementedEngineServiceServer
	storage       Storage
	timeSource    TimeSource
	appliedOffset uint64
	appliedResult []byte
	lock          sync.RWMutex
	proposer      replv1.ReplServiceClient
	ready         bool
	health        *health.Server
}

func convertError(e error) error {
	switch {
	case errors.Is(e, ErrEmptyKey):
		return status.Error(codes.InvalidArgument, e.Error())
	case errors.Is(e, ErrKeyNotFound):
		return status.Error(codes.NotFound, e.Error())
	case errors.Is(e, ErrNotANumber):
		return status.Error(codes.FailedPrecondition, e.Error())
	case errors.Is(e, ErrOverflow):
		return status.Error(codes.InvalidArgument, e.Error())
	default:
		return status.Error(codes.Internal, e.Error())
	}
}

func (s *server) Set(ctx context.Context, in *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	if _, err := s.mutateValue(ctx, setEventMagic, in.Key, in.Value, in.Ttl); err != nil {
		return nil, err
	}
	return &kvv1.SetResponse{}, nil
}

func (s *server) Get(_ context.Context, in *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if !s.isReady() {
		return nil, status.Error(codes.Unavailable, "engine recovery is in progress")
	}
	value, expiresAt, err := s.storage.Get(string(in.Key))
	if err != nil {
		return nil, convertError(err)
	}
	return &kvv1.GetResponse{Value: []byte(value), ExpiresAt: expiresAt}, nil
}

func (s *server) GetExpireTime(_ context.Context, in *kvv1.GetExpireTimeRequest) (*kvv1.GetExpireTimeResponse, error) {
	if !s.isReady() {
		return nil, status.Error(codes.Unavailable, "engine recovery is in progress")
	}
	_, expiresAt, err := s.storage.Get(string(in.Key))
	if err != nil {
		return nil, convertError(err)
	}
	return &kvv1.GetExpireTimeResponse{ExpiresAt: expiresAt}, nil
}

func (s *server) Add(ctx context.Context, in *kvv1.AddRequest) (*kvv1.AddResponse, error) {
	r, err := s.mutateValue(ctx, addEventMagic, in.Key, in.Value, in.Ttl)
	if err != nil {
		return nil, err
	}
	return &kvv1.AddResponse{Success: r.success}, nil
}

func (s *server) Replace(ctx context.Context, in *kvv1.ReplaceRequest) (*kvv1.ReplaceResponse, error) {
	r, err := s.mutateValue(ctx, replaceEventMagic, in.Key, in.Value, in.Ttl)
	if err != nil {
		return nil, err
	}
	return &kvv1.ReplaceResponse{Success: r.success}, nil
}

func (s *server) Delete(ctx context.Context, in *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	r, err := s.mutate(ctx, mutation{kind: deleteEventMagic, key: in.Key, now: s.timeSource.Now()})
	if err != nil {
		return nil, err
	}
	return &kvv1.DeleteResponse{Success: r.success}, nil
}

func (s *server) Incr(ctx context.Context, in *kvv1.IncrRequest) (*kvv1.IncrResponse, error) {
	r, err := s.mutate(ctx, mutation{kind: incrEventMagic, key: in.Key, delta: in.Delta, now: s.timeSource.Now()})
	if err != nil {
		return nil, err
	}
	return &kvv1.IncrResponse{Value: r.value}, nil
}

func (s *server) Decr(ctx context.Context, in *kvv1.DecrRequest) (*kvv1.DecrResponse, error) {
	r, err := s.mutate(ctx, mutation{kind: decrEventMagic, key: in.Key, delta: in.Delta, now: s.timeSource.Now()})
	if err != nil {
		return nil, err
	}
	return &kvv1.DecrResponse{Value: r.value}, nil
}

func (s *server) mutateValue(ctx context.Context, kind uint32, key, value []byte, ttl uint64) (mutationResult, error) {
	if !s.isReady() {
		return mutationResult{}, status.Error(codes.Unavailable, "engine recovery is in progress")
	}
	now := s.timeSource.Now()
	expiresAt, err := expiresAtFromTTL(ttl, now)
	if err != nil {
		return mutationResult{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return s.mutate(ctx, mutation{kind: kind, key: key, value: value, now: now, expiresAt: expiresAt})
}

func (s *server) mutate(ctx context.Context, cmd mutation) (mutationResult, error) {
	if !s.isReady() {
		return mutationResult{}, status.Error(codes.Unavailable, "engine recovery is in progress")
	}
	if len(cmd.key) == 0 {
		return mutationResult{}, convertError(ErrEmptyKey)
	}
	if err := ctx.Err(); err != nil {
		return mutationResult{}, status.FromContextError(err).Err()
	}
	payload, err := encodeMutation(cmd)
	if err != nil {
		return mutationResult{}, status.Error(codes.ResourceExhausted, err.Error())
	}
	var result mutationResult
	if s.proposer != nil {
		// Apply calls back into this server: never hold its lock during Propose.
		resp, err := s.proposer.Propose(ctx, &replv1.ProposeRequest{Payload: payload})
		if err != nil {
			return result, err
		}
		// Set has no business result; accept responses from older repl versions.
		if cmd.kind == setEventMagic {
			return mutationResult{success: true}, nil
		}
		result, err = decodeMutationResult(resp.GetResult())
		if err != nil {
			return result, status.Error(codes.Internal, err.Error())
		}
	} else {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.ready {
			return result, status.Error(codes.Unavailable, "engine recovery is in progress")
		}
		if err := ctx.Err(); err != nil {
			return result, status.FromContextError(err).Err()
		}
		result = s.applyMutation(cmd)
	}
	if result.err != nil {
		return result, convertError(result.err)
	}
	return result, nil
}

// Called with the Apply lock held. Storage methods perform each conditional
// check and mutation atomically using the timestamp recorded in the event.
func (s *server) applyMutation(cmd mutation) mutationResult {
	var r mutationResult
	key, value := string(cmd.key), string(cmd.value)
	switch cmd.kind {
	case setEventMagic:
		r.err = s.storage.Set(key, value, cmd.expiresAt)
		r.success = r.err == nil
	case addEventMagic:
		r.success, r.err = s.storage.Add(key, value, cmd.expiresAt, cmd.now)
	case replaceEventMagic:
		r.success, r.err = s.storage.Replace(key, value, cmd.expiresAt, cmd.now)
	case deleteEventMagic:
		r.success, r.err = s.storage.Delete(key, cmd.now)
	case incrEventMagic:
		r.value, r.err = s.storage.Incr(key, cmd.delta, cmd.now)
		r.success = r.err == nil
	case decrEventMagic:
		r.value, r.err = s.storage.Decr(key, cmd.delta, cmd.now)
		r.success = r.err == nil
	}
	return r
}

func (s *server) Apply(ctx context.Context, in *enginev1.ApplyRequest) (*enginev1.ApplyResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if in.Offset <= s.appliedOffset {
		if in.Offset == s.appliedOffset {
			return &enginev1.ApplyResponse{Result: bytes.Clone(s.appliedResult)}, nil
		}
		return &enginev1.ApplyResponse{}, nil
	}
	if in.Offset-s.appliedOffset != uint64(len(in.Payload)) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"invalid offset: applied=%d payload_size=%d got=%d", s.appliedOffset, len(in.Payload), in.Offset)
	}
	cmd, err := decodeMutation(in.Payload)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := encodeMutationResult(s.applyMutation(cmd))
	if err != nil {
		return nil, convertError(err)
	}
	// A committed command can be a no-op (including a business rejection).
	// Its offset must still advance so subsequent entries and replay can proceed.
	s.appliedOffset, s.appliedResult = in.Offset, result
	return &enginev1.ApplyResponse{Result: bytes.Clone(result)}, nil
}

func (s *server) GetOffset(_ context.Context, _ *enginev1.GetOffsetRequest) (*enginev1.GetOffsetResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return &enginev1.GetOffsetResponse{Offset: s.appliedOffset}, nil
}

func (s *server) isReady() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.ready
}

func (s *server) SetReady(ctx context.Context, in *enginev1.SetReadyRequest) (*enginev1.SetReadyResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	s.ready = in.Ready
	state := healthgrpc.HealthCheckResponse_NOT_SERVING
	if s.ready {
		state = healthgrpc.HealthCheckResponse_SERVING
	}
	s.health.SetServingStatus("", state)
	return &enginev1.SetReadyResponse{}, nil
}

func (s *server) RunServer(port int) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(replication.MaxMessageSize))
	kvv1.RegisterKVServiceServer(grpcServer, s)
	enginev1.RegisterEngineServiceServer(grpcServer, s)
	healthcheck := s.health
	healthgrpc.RegisterHealthServer(grpcServer, healthcheck)

	serverStopped := make(chan struct{}, 1)
	go func() {
		<-signalChan
		log.Println("Shutting down...")
		healthcheck.Shutdown()
		timer := time.AfterFunc(10*time.Second, func() {
			log.Println("Server couldn't stop gracefully in time. Doing force stop.")
			grpcServer.Stop()
			// Unblock the main function.
			select {
			case serverStopped <- struct{}{}:
			default:
			}
		})
		defer timer.Stop()
		grpcServer.GracefulStop() // gracefully stop server after in-flight server streaming rpc finishes
		log.Println("Server stopped gracefully.")
		// Unblock the main function.
		select {
		case serverStopped <- struct{}{}:
		default:
		}
	}()

	err = grpcServer.Serve(lis)
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatalf("failed to serve: %v", err)
	}
	<-serverStopped
}

func expiresAtFromTTL(ttl, now uint64) (uint64, error) {
	if ttl == 0 || ttl > SECONDS_IN_MONTH {
		return ttl, nil
	}
	if ttl > ^uint64(0)-now {
		return 0, errors.New("ttl overflows expiration timestamp")
	}
	return now + ttl, nil
}

func newServer(storage Storage, timeSource TimeSource) *server {
	s := &server{storage: storage, timeSource: timeSource, ready: true, health: health.NewServer()}
	s.health.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	return s
}

func CreateServer(storage Storage) *server {
	return newServer(storage, SystemTimeSource{})
}

func CreateServerWithProposer(storage Storage, proposer replv1.ReplServiceClient) *server {
	s := CreateServer(storage)
	s.proposer = proposer
	if proposer != nil {
		s.ready = false
		s.health.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)
	}
	return s
}
