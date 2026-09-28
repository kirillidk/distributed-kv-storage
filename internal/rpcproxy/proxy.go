package rpcproxy

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Proxy struct {
	kvv1.UnimplementedKVServiceServer
	clients []kvv1.KVServiceClient
	conns   []*grpc.ClientConn
	rng     *rand.Rand
	mu      sync.Mutex
}

func New(ctx context.Context, cfg Config, opts ...grpc.DialOption) (*Proxy, error) {
	if len(cfg.Engines) == 0 {
		return nil, fmt.Errorf("at least one engine is required")
	}
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	p := &Proxy{rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	conns := make([]*grpc.ClientConn, 0, len(cfg.Engines))
	for _, engine := range cfg.Engines {
		conn, err := grpc.DialContext(ctx, engine.Address, opts...)
		if err != nil {
			for _, opened := range conns {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("dial engine %q at %q: %w", engine.Name, engine.Address, err)
		}
		conns = append(conns, conn)
		p.conns = append(p.conns, conn)
		p.clients = append(p.clients, kvv1.NewKVServiceClient(conn))
	}
	return p, nil
}

func (p *Proxy) Close() error {
	var firstErr error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *Proxy) selectedClient() kvv1.KVServiceClient {
	p.mu.Lock()
	i := p.rng.Intn(len(p.clients))
	p.mu.Unlock()
	return p.clients[i]
}

func (p *Proxy) Set(ctx context.Context, req *kvv1.SetRequest) (*kvv1.SetResponse, error) {
	if len(p.clients) == 0 {
		return nil, status.Error(codes.Unavailable, "no KV engines configured")
	}
	return p.selectedClient().Set(ctx, req)
}

func (p *Proxy) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if len(p.clients) == 0 {
		return nil, status.Error(codes.Unavailable, "no KV engines configured")
	}
	return p.selectedClient().Get(ctx, req)
}
