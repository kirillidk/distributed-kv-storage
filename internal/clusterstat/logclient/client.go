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

const PageBytes = 256 * 1024

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
	// Data preserves bytes split across pages, including partial UTF-8 characters.
	Data       []byte
	Lines      []string
	NextOffset uint64
	EOF        bool
	ReplAddr   string
}

func (c *Client) ReadPage(ctx context.Context, nodeID string, typ config.ComponentType, offset uint64, maxTotal int) (*Result, error) {
	addr, ok := c.replAddr[nodeID]
	if !ok {
		return nil, fmt.Errorf("no repl address for node %q", nodeID)
	}
	comp, err := toLogComponent(typ)
	if err != nil {
		return nil, err
	}
	if maxTotal <= 0 || maxTotal > PageBytes {
		maxTotal = PageBytes
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial repl %s: %w", addr, err)
	}
	defer conn.Close()

	cli := replv1.NewReplAdminServiceClient(conn)
	raw := []byte{}
	var eof bool

	for len(raw) < maxTotal {
		maxBytes := min(64*1024, maxTotal-len(raw))
		reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
		resp, err := cli.ReadLogs(reqCtx, &replv1.ReadLogsRequest{
			Component: comp,
			Offset:    offset,
			MaxBytes:  uint32(maxBytes),
		})
		cancel()
		if err != nil {
			return nil, err
		}
		if len(resp.GetData()) > maxBytes || resp.GetNextOffset() < offset ||
			resp.GetNextOffset()-offset != uint64(len(resp.GetData())) ||
			(len(resp.GetData()) == 0 && !resp.GetEof()) {
			return nil, fmt.Errorf("invalid ReadLogs page")
		}
		raw = append(raw, resp.GetData()...)
		offset = resp.GetNextOffset()
		eof = resp.GetEof()
		if eof || len(resp.GetData()) == 0 {
			break
		}
	}

	return &Result{
		Data:       raw,
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
