package repl

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const healthInterval = time.Second
const healthTimeout = time.Second

func monitorHealth(ctx context.Context, address string, logger *slog.Logger) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("create engine health client", "address", address, "error", err)
		return
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()

	previous := ""
	for {
		checkCtx, cancel := context.WithTimeout(ctx, healthTimeout)
		response, err := client.Check(checkCtx, &healthpb.HealthCheckRequest{})
		cancel()
		if ctx.Err() != nil {
			return
		}
		state := "unavailable"
		if err == nil {
			state = response.Status.String()
		}
		if state != previous {
			if err != nil {
				logger.Info("engine health changed", "address", address, "state", state, "error", err)
			} else {
				logger.Info("engine health changed", "address", address, "state", state)
			}
			previous = state
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
