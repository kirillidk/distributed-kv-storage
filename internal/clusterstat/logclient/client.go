package logclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	replAddr map[string]string
	timeout  time.Duration
}

func New(components []config.Component, timeout time.Duration) *Client {
	m := make(map[string]string)
	for _, c := range components {
		if c.Type == config.TypeRepl && c.NodeID != "" {
			m[c.NodeID] = c.Address
		}
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Client{replAddr: m, timeout: timeout}
}

func toLogComponent(typ config.ComponentType) (replv1.LogComponent, error) {
	switch typ {
	case config.TypeRepl:
		return replv1.LogComponent_LOG_COMPONENT_REPL, nil
	case config.TypeKVEngine:
		return replv1.LogComponent_LOG_COMPONENT_KV_ENGINE, nil
	default:
		return 0, fmt.Errorf("unsupported component type %q", typ)
	}
}

type Result struct {
	Lines      []string
	NextOffset uint64
	EOF        bool
	ReplAddr   string
}

func (c *Client) ReadSnapshot(ctx context.Context, nodeID string, typ config.ComponentType, maxTotal int) (*Result, error) {
	addr, ok := c.replAddr[nodeID]
	if !ok {
		return nil, fmt.Errorf("no repl address for node %q", nodeID)
	}
	comp, err := toLogComponent(typ)
	if err != nil {
		return nil, err
	}
	if maxTotal <= 0 {
		maxTotal = 256 * 1024
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial repl %s: %w", addr, err)
	}
	defer conn.Close()

	cli := replv1.NewReplAdminServiceClient(conn)
	var raw []byte
	var offset uint64
	var eof bool

	for len(raw) < maxTotal {
		reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
		resp, err := cli.ReadLogs(reqCtx, &replv1.ReadLogsRequest{
			Component: comp,
			Offset:    offset,
			MaxBytes:  64 * 1024,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		raw = append(raw, resp.GetData()...)
		offset = resp.GetNextOffset()
		eof = resp.GetEof()
		if eof || len(resp.GetData()) == 0 {
			break
		}
	}

	return &Result{
		Lines:      splitLines(string(raw)),
		NextOffset: offset,
		EOF:        eof,
		ReplAddr:   addr,
	}, nil
}

func splitLines(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, "\n")
	if n := len(parts); n > 0 && parts[n-1] == "" {
		parts = parts[:n-1]
	}
	return parts
}
