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
	metrics       *metrics
}

func convertError(e error) *status.Status {
	switch {
	case errors.Is(e, ErrEmptyKey):
		return status.New(codes.InvalidArgument, e.Error())
	case errors.Is(e, ErrKeyNotFound):
		return status.New(codes.NotFound, e.Error())
	case errors.Is(e, ErrNotANumber):
		return status.New(codes.FailedPrecondition, e.Error())
	case errors.Is(e, ErrOverflow):
		return status.New(codes.InvalidArgument, e.Error())
	default:
		return status.New(codes.Internal, e.Error())
	}
}

func (s *server) Error(method string, code codes.Code, msg string) error {
	s.metrics.logError(method, code)
	err := status.Error(code, msg)
	return err
}

func (s *server) ErrorS(method string, st *status.Status) error {
	s.metrics.logError(method, st.Code())
	err := st.Err()
	return err
}

func (s *server) Set(ctx context.Context, in *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	timer := s.metrics.requestTimer("Set")
	defer timer.ObserveDuration()
	if _, err := s.mutateValue(ctx, setEventMagic, in.Key, in.Value, in.Ttl); err != nil {
		return nil, s.ErrorS("Set", err)
	}
	s.metrics.logSuccess("Set")
	return &kvv1.SetResponse{}, nil
}

func (s *server) Get(_ context.Context, in *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	timer := s.metrics.requestTimer("Get")
	defer timer.ObserveDuration()
	if !s.isReady() {
		return nil, s.Error("Get", codes.Unavailable, "engine recovery is in progress")
	}
	value, expiresAt, err := s.storage.Get(string(in.Key))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			s.metrics.getMisses.WithLabelValues("Get").Inc()
		}
		return nil, s.ErrorS("Get", convertError(err))
	}
	s.metrics.getHits.WithLabelValues("Get").Inc()
	s.metrics.logSuccess("Get")
	return &kvv1.GetResponse{Value: []byte(value), ExpiresAt: expiresAt}, nil
}

func (s *server) GetExpireTime(_ context.Context, in *kvv1.GetExpireTimeRequest) (*kvv1.GetExpireTimeResponse, error) {
	timer := s.metrics.requestTimer("GetExpireTime")
	defer timer.ObserveDuration()
	if !s.isReady() {
		return nil, s.Error("GetExpireTime", codes.Unavailable, "engine recovery is in progress")
	}
	_, expiresAt, err := s.storage.Get(string(in.Key))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			s.metrics.getMisses.WithLabelValues("GetExpireTime").Inc()
		}
		return nil, s.ErrorS("GetExpireTime", convertError(err))
	}
	s.metrics.getHits.WithLabelValues("GetExpireTime").Inc()
	s.metrics.logSuccess("GetExpireTime")
	return &kvv1.GetExpireTimeResponse{ExpiresAt: expiresAt}, nil
}

func (s *server) Add(ctx context.Context, in *kvv1.AddRequest) (*kvv1.AddResponse, error) {
	timer := s.metrics.requestTimer("Add")
	defer timer.ObserveDuration()
	r, err := s.mutateValue(ctx, addEventMagic, in.Key, in.Value, in.Ttl)
	if err != nil {
		return nil, s.ErrorS("Add", err)
	}
	s.metrics.logSuccess("Add")
	return &kvv1.AddResponse{Success: r.success}, nil
}

func (s *server) Replace(ctx context.Context, in *kvv1.ReplaceRequest) (*kvv1.ReplaceResponse, error) {
	timer := s.metrics.requestTimer("Replace")
	defer timer.ObserveDuration()
	r, err := s.mutateValue(ctx, replaceEventMagic, in.Key, in.Value, in.Ttl)
	if err != nil {
		return nil, s.ErrorS("Replace", err)
	}
	s.metrics.logSuccess("Replace")
	return &kvv1.ReplaceResponse{Success: r.success}, nil
}

func (s *server) Delete(ctx context.Context, in *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	timer := s.metrics.requestTimer("Delete")
	defer timer.ObserveDuration()
	r, err := s.mutate(ctx, mutation{kind: deleteEventMagic, key: in.Key, now: s.timeSource.Now()})
	if err != nil {
		return nil, s.ErrorS("Delete", err)
	}
	s.metrics.logSuccess("Delete")
	return &kvv1.DeleteResponse{Success: r.success}, nil
}

func (s *server) Incr(ctx context.Context, in *kvv1.IncrRequest) (*kvv1.IncrResponse, error) {
	timer := s.metrics.requestTimer("Incr")
	defer timer.ObserveDuration()
	r, err := s.mutate(ctx, mutation{kind: incrEventMagic, key: in.Key, delta: in.Delta, now: s.timeSource.Now()})
	if err != nil {
		return nil, s.ErrorS("Incr", err)
	}
	s.metrics.logSuccess("Incr")
	return &kvv1.IncrResponse{Value: r.value}, nil
}

func (s *server) Decr(ctx context.Context, in *kvv1.DecrRequest) (*kvv1.DecrResponse, error) {
	timer := s.metrics.requestTimer("Decr")
	defer timer.ObserveDuration()
	r, err := s.mutate(ctx, mutation{kind: decrEventMagic, key: in.Key, delta: in.Delta, now: s.timeSource.Now()})
	if err != nil {
		return nil, s.ErrorS("Decr", err)
	}
	s.metrics.logSuccess("Decr")
	return &kvv1.DecrResponse{Value: r.value}, nil
}

func (s *server) mutateValue(ctx context.Context, kind uint32, key, value []byte, ttl uint64) (mutationResult, *status.Status) {
	if !s.isReady() {
		return mutationResult{}, status.New(codes.Unavailable, "engine recovery is in progress")
	}
	now := s.timeSource.Now()
	expiresAt, err := expiresAtFromTTL(ttl, now)
	if err != nil {
		return mutationResult{}, status.New(codes.InvalidArgument, err.Error())
	}
	return s.mutate(ctx, mutation{kind: kind, key: key, value: value, now: now, expiresAt: expiresAt})
}

func (s *server) mutate(ctx context.Context, cmd mutation) (mutationResult, *status.Status) {
	if !s.isReady() {
		return mutationResult{}, status.New(codes.Unavailable, "engine recovery is in progress")
	}
	if len(cmd.key) == 0 {
		return mutationResult{}, convertError(ErrEmptyKey)
	}
	if err := ctx.Err(); err != nil {
		return mutationResult{}, status.FromContextError(err)
	}
	payload, err := encodeMutation(cmd)
	if err != nil {
		return mutationResult{}, status.New(codes.ResourceExhausted, err.Error())
	}
	var result mutationResult
	if s.proposer != nil {
		// Apply calls back into this server: never hold its lock during Propose.
		resp, err := s.proposer.Propose(ctx, &replv1.ProposeRequest{Payload: payload})
		if err != nil {
			return result, status.Convert(err)
		}
		// Set has no business result; accept responses from older repl versions.
		if cmd.kind == setEventMagic {
			return mutationResult{success: true}, nil
		}
		result, err = decodeMutationResult(resp.GetResult())
		if err != nil {
			return result, status.New(codes.Internal, err.Error())
		}
	} else {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.ready {
			return result, status.New(codes.Unavailable, "engine recovery is in progress")
		}
		if err := ctx.Err(); err != nil {
			return result, status.FromContextError(err)
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
		s.metrics.entriesCount.Set(float64(s.storage.EntryCount()))
		r.success = r.err == nil
	case addEventMagic:
		r.success, r.err = s.storage.Add(key, value, cmd.expiresAt, cmd.now)
		s.metrics.entriesCount.Set(float64(s.storage.EntryCount()))
	case replaceEventMagic:
		r.success, r.err = s.storage.Replace(key, value, cmd.expiresAt, cmd.now)
	case deleteEventMagic:
		r.success, r.err = s.storage.Delete(key, cmd.now)
		s.metrics.entriesCount.Set(float64(s.storage.EntryCount()))
	case incrEventMagic:
		r.value, r.err = s.storage.Incr(key, cmd.delta, cmd.now)
		r.success = r.err == nil
	case decrEventMagic:
		r.value, r.err = s.storage.Decr(key, cmd.delta, cmd.now)
		r.success = r.err == nil
	}
	return r
}

// Internal function not included in Prometheus metrics
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
		return nil, convertError(err).Err()
	}
	// A committed command can be a no-op (including a business rejection).
	// Its offset must still advance so subsequent entries and replay can proceed.
	s.appliedOffset, s.appliedResult = in.Offset, result
	s.metrics.appliedOffset.Set(float64(s.appliedOffset))
	return &enginev1.ApplyResponse{Result: bytes.Clone(result)}, nil
}

// Internal function not included in Prometheus metrics
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
	readyChanged := s.ready != in.Ready
	s.ready = in.Ready
	state := healthgrpc.HealthCheckResponse_NOT_SERVING
	if s.ready {
		state = healthgrpc.HealthCheckResponse_SERVING
	}
	s.health.SetServingStatus("", state)
	if readyChanged {
		log.Printf("state -> %s", state)
		s.metrics.setReadiness(s.ready)
	}
	return &enginev1.SetReadyResponse{}, nil
}

func (s *server) RunServer(port int, metricsAddr string) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalChan)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(replication.MaxMessageSize))
	kvv1.RegisterKVServiceServer(grpcServer, s)
	enginev1.RegisterEngineServiceServer(grpcServer, s)
	healthcheck := s.health
	healthgrpc.RegisterHealthServer(grpcServer, healthcheck)
	log.Printf("gRPC server listening on %s", lis.Addr())

	metricsServer := s.metrics.startMetricsServer(metricsAddr)
	// Also drain HTTP when the gRPC shutdown deadline forces RunServer to return.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Metrics server graceful shutdown failed: %v", err)
			if err := metricsServer.Close(); err != nil {
				log.Printf("Metrics server force close failed: %v", err)
			}
		}
	}()

	serverStopped := make(chan struct{}, 1)
	go func() {
		<-signalChan
		log.Println("Shutting down...")
		healthcheck.Shutdown()
		s.metrics.setReadiness(false)
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
		log.Println("gRPC server stopped gracefully.")
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
	s := &server{storage: storage, timeSource: timeSource, ready: true, health: health.NewServer(), metrics: newMetrics()}
	s.health.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	s.metrics.setReadiness(true)
	s.metrics.entriesCount.Set(float64(storage.EntryCount()))
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
		s.metrics.setReadiness(false)
	}
	return s
}
