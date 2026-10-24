package kvengine

import (
	"bytes"
	"context"
	"encoding/binary"
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
	lock          sync.RWMutex
	proposer      replv1.ReplServiceClient
	ready         bool
	health        *health.Server
}

const setEventMagic uint32 = 0x6f825fba

func convertError(e error) error {
	switch {
	case errors.Is(e, ErrEmptyKey):
		return status.Error(codes.InvalidArgument, e.Error())
	case errors.Is(e, ErrKeyNotFound):
		return status.Error(codes.NotFound, e.Error())
	default:
		return status.Error(codes.Internal, e.Error())
	}
}

func (s *server) Set(ctx context.Context, in *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	if !s.isReady() {
		return nil, status.Error(codes.Unavailable, "engine recovery is in progress")
	}
	if len(in.Key) == 0 {
		return nil, convertError(ErrEmptyKey)
	}
	expiresAt, err := expiresAtFromTTL(in.Ttl, s.timeSource.Now())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.proposer != nil {
		payload, err := encodeSetEvent(in.Key, in.Value, expiresAt)
		if err != nil {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		// Do not hold the Apply lock while calling repl: Propose calls back into Apply.
		if _, err := s.proposer.Propose(ctx, &replv1.ProposeRequest{Payload: payload}); err != nil {
			return nil, err
		}
		return &kvv1.SetResponse{}, nil
	}
	err = s.storage.Set(string(in.Key), string(in.Value), expiresAt)
	if err != nil {
		return nil, convertError(err)
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

func (s *server) Apply(_ context.Context, in *enginev1.ApplyRequest) (*enginev1.ApplyResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if in.Offset <= s.appliedOffset {
		return &enginev1.ApplyResponse{}, nil
	}
	if in.Offset-s.appliedOffset != uint64(len(in.Payload)) {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"invalid offset: applied=%d payload_size=%d got=%d",
			s.appliedOffset,
			len(in.Payload),
			in.Offset,
		)
	}
	payload := in.Payload
	r := bytes.NewReader(payload)

	order := binary.BigEndian

	var cmd uint32
	if err := binary.Read(r, order, &cmd); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	switch cmd {
	case setEventMagic:
		var expiresAt uint64
		if err := binary.Read(r, order, &expiresAt); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		var keySize, valueSize uint32
		if err := binary.Read(r, order, &keySize); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if err := binary.Read(r, order, &valueSize); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		remaining := uint64(r.Len())
		if uint64(keySize) > remaining || uint64(valueSize) != remaining-uint64(keySize) {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}
		key := make([]byte, keySize)
		if n, _ := r.Read(key); uint32(n) != keySize {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}

		value := make([]byte, valueSize)
		if n, _ := r.Read(value); uint32(n) != valueSize {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}

		err := s.storage.Set(string(key), string(value), expiresAt)
		if err != nil {
			return nil, convertError(err)
		}

		s.appliedOffset = in.Offset
		return &enginev1.ApplyResponse{}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid command type")
	}
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

func encodeSetEvent(key, value []byte, expiresAt uint64) ([]byte, error) {
	if uint64(len(key))+uint64(len(value))+20 > replication.MaxPayloadSize {
		return nil, errors.New("Set payload is too large")
	}
	payload := make([]byte, 20+len(key)+len(value))
	binary.BigEndian.PutUint32(payload, setEventMagic)
	binary.BigEndian.PutUint64(payload[4:], expiresAt)
	binary.BigEndian.PutUint32(payload[12:], uint32(len(key)))
	binary.BigEndian.PutUint32(payload[16:], uint32(len(value)))
	copy(payload[20:], key)
	copy(payload[20+len(key):], value)
	return payload, nil
}
