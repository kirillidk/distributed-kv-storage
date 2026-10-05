package collector

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kirillidk/distributed-kv-storage/internal/clusterstat/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type Status string

const (
	StatusUnknown Status = "UNKNOWN"
	StatusUP      Status = "UP"
	StatusDOWN    Status = "DOWN"
)

type ComponentState struct {
	NodeID    string
	Type      config.ComponentType
	Address   string
	Status    Status
	CheckedAt time.Time
	Error     string
}

type Collector struct {
	components   []config.Component
	pollInterval time.Duration
	checkTimeout time.Duration

	mu     sync.RWMutex
	states map[componentKey]*ComponentState
}

type componentKey struct {
	nodeID  string
	typeID  config.ComponentType
	address string
}

func keyFor(comp config.Component) componentKey {
	return componentKey{nodeID: comp.NodeID, typeID: comp.Type, address: comp.Address}
}

func New(components []config.Component, pollInterval, checkTimeout time.Duration) *Collector {
	c := &Collector{
		components:   components,
		pollInterval: pollInterval,
		checkTimeout: checkTimeout,
		states:       make(map[componentKey]*ComponentState, len(components)),
	}
	for _, comp := range components {
		c.states[keyFor(comp)] = &ComponentState{
			NodeID:  comp.NodeID,
			Type:    comp.Type,
			Address: comp.Address,
			Status:  StatusUnknown,
		}
	}
	return c
}

func (c *Collector) Snapshot() []ComponentState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]ComponentState, 0, len(c.states))
	for _, s := range c.states {
		out = append(out, *s)
	}
	return out
}

func (c *Collector) Run(ctx context.Context) {
	c.pollOnce(ctx)

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollOnce(ctx)
		}
	}
}

func (c *Collector) pollOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, comp := range c.components {
		wg.Add(1)
		go func(comp config.Component) {
			defer wg.Done()
			c.checkOne(ctx, comp)
		}(comp)
	}
	wg.Wait()
}

func (c *Collector) checkOne(parentCtx context.Context, comp config.Component) {
	ctx, cancel := context.WithTimeout(parentCtx, c.checkTimeout)
	defer cancel()

	newStatus := StatusDOWN
	errMsg := ""

	conn, err := grpc.NewClient(
		comp.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		errMsg = err.Error()
	} else {
		defer conn.Close()

		client := grpc_health_v1.NewHealthClient(conn)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{
			Service: "",
		})
		if err != nil {
			errMsg = err.Error()
		} else if resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
			newStatus = StatusUP
		} else {
			errMsg = "health status: " + resp.GetStatus().String()
		}
	}

	if parentCtx.Err() != nil {
		return
	}

	c.update(comp, newStatus, errMsg)
}

func (c *Collector) update(comp config.Component, status Status, errMsg string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.states[keyFor(comp)]
	old := st.Status

	st.Status = status
	st.CheckedAt = time.Now().UTC()
	st.Error = errMsg

	if old != status {
		log.Printf("component %s (%s) %s → %s  error=%q",
			comp.Address, st.Type, old, status, errMsg)
	}
}
