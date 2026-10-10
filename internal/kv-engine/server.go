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
	appliedOffset uint64
	lock          sync.RWMutex
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

func (s *server) Set(_ context.Context, in *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	err := s.storage.Set(string(in.Key), string(in.Value), in.Ttl)
	if err != nil {
		return nil, convertError(err)
	}
	return &kvv1.SetResponse{}, nil
}

func (s *server) Get(_ context.Context, in *kvv1.GetRequest) (*kvv1.GetResponse, error) {
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
	if in.Offset != s.appliedOffset+uint64(len(in.Payload)) {
		return nil, status.Error(codes.InvalidArgument, "invalid offset")
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

		if keySize > uint32(r.Len()) {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}
		key := make([]byte, keySize)
		if n, _ := r.Read(key); uint32(n) != keySize {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}

		if valueSize > uint32(r.Len()) {
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

func (s *server) Status(_ context.Context, in *enginev1.StatusRequest) (*enginev1.StatusResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return &enginev1.StatusResponse{Offset: s.appliedOffset}, nil
}

func (s *server) RunServer(port int) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvv1.RegisterKVServiceServer(grpcServer, s)
	enginev1.RegisterEngineServiceServer(grpcServer, s)
	healthcheck := health.NewServer()
	healthcheck.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)
	healthgrpc.RegisterHealthServer(grpcServer, healthcheck)

	serverStopped := make(chan struct{}, 1)
	go func() {
		healthcheck.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
		log.Println("state -> SERVING")

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

func CreateServer(storage Storage) server {
	return server{storage: storage}
}
