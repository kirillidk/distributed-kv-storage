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
	"syscall"
	"time"

	kvv2 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type server struct {
	kvv2.UnimplementedKVServiceServer
	storage       Storage
	appliedOffset uint64
}

const setCommandID = 1

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

func (s *server) Set(_ context.Context, in *kvv2.SetRequest) (*kvv2.SetResponse, error) {
	err := s.storage.Set(string(in.Key), string(in.Value), in.Ttl)
	if err != nil {
		return nil, convertError(err)
	}
	return &kvv2.SetResponse{}, nil
}

func (s *server) Get(_ context.Context, in *kvv2.GetRequest) (*kvv2.GetResponse, error) {
	res, err := s.storage.Get(string(in.Key))
	if err != nil {
		return nil, convertError(err)
	}
	return &kvv2.GetResponse{Value: []byte(res)}, nil
}

func (s *server) Apply(_ context.Context, in *kvv2.ApplyRequest) (*kvv2.ApplyResponse, error) {
	if s.appliedOffset != in.Offset {
		return &kvv2.ApplyResponse{}, nil
	}
	payload := in.Payload
	r := bytes.NewReader(payload)

	cmd, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	switch cmd {
	case setCommandID:
		ttl, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		keyLength, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		valueLength, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		key := make([]byte, keyLength)
		if n, _ := r.Read(key); uint64(n) != keyLength {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}
		value := make([]byte, valueLength)
		if n, _ := r.Read(value); uint64(n) != valueLength {
			return nil, status.Error(codes.InvalidArgument, "malformed payload")
		}

		err = s.storage.Set(string(key), string(value), ttl)
		if err != nil {
			return nil, convertError(err)
		}

		s.appliedOffset += uint64(len(payload))
		return &kvv2.ApplyResponse{}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid command type")
	}
}

func (s *server) Status(_ context.Context, in *kvv2.StatusRequest) (*kvv2.StatusResponse, error) {
	return &kvv2.StatusResponse{Offset: s.appliedOffset}, nil
}

func (s *server) RunServer(port int) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvv2.RegisterKVServiceServer(grpcServer, s)
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
	return server{storage: storage, appliedOffset: 0}
}
