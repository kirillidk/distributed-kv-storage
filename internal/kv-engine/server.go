package kvengine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type server struct {
	kvv1.UnimplementedKVServiceServer
	storage Storage
}

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
	res, err := s.storage.Get(string(in.Key))
	if err != nil {
		return nil, convertError(err)
	}
	return &kvv1.GetResponse{Value: []byte(res)}, nil
}

func (s *server) RunServer(port int) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc := grpc.NewServer()
	kvv1.RegisterKVServiceServer(grpc, s)
	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(grpc, healthcheck)

	serverStopped := make(chan struct{}, 1)
	go func() {
		healthcheck.SetServingStatus("kv-engine", healthgrpc.HealthCheckResponse_SERVING)
		log.Println("state -> SERVING")

		<-signalChan
		log.Println("Shutting down...")
		healthcheck.SetServingStatus("kv-engine", healthgrpc.HealthCheckResponse_NOT_SERVING)
		timer := time.AfterFunc(10*time.Second, func() {
			log.Println("Server couldn't stop gracefully in time. Doing force stop.")
			grpc.Stop()
			// Unblock the main function.
			select {
			case serverStopped <- struct{}{}:
			default:
			}
		})
		defer timer.Stop()
		grpc.GracefulStop() // gracefully stop server after in-flight server streaming rpc finishes
		log.Println("Server stopped gracefully.")
		// Unblock the main function.
		select {
		case serverStopped <- struct{}{}:
		default:
		}
	}()

	err = grpc.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	<-serverStopped
}

func CreateServer(storage Storage) server {
	return server{storage: storage}
}
