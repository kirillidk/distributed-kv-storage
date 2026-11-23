package collector

import (
	"context"
	"log"
	"slices"
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

const maxLogEntries = 200

type ComponentState struct {
	ShardID   string
	NodeID    string
	Type      config.ComponentType
	Address   string
	Status    Status
	CheckedAt time.Time
	Error     string
}

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	OldStatus Status    `json:"old_status"`
	NewStatus Status    `json:"new_status"`
	Error     string    `json:"error"`
	Message   string    `json:"message"`
}

type Collector struct {
	components   []config.Component
	pollInterval time.Duration
	checkTimeout time.Duration

	mu     sync.RWMutex
	states map[componentKey]*ComponentState
	logs   map[componentKey][]LogEntry
}

type componentKey struct {
	nodeID  string
	typeID  config.ComponentType
	shardID string
}

func keyFor(comp config.Component) componentKey {
	return componentKey{nodeID: comp.NodeID, typeID: comp.Type, shardID: comp.ShardID}
}

func New(components []config.Component, pollInterval, checkTimeout time.Duration) *Collector {
	c := &Collector{
		components:   components,
		pollInterval: pollInterval,
		checkTimeout: checkTimeout,
		states:       make(map[componentKey]*ComponentState, len(components)),
		logs:         make(map[componentKey][]LogEntry, len(components)),
	}
	for _, comp := range components {
		c.states[keyFor(comp)] = &ComponentState{
			NodeID:  comp.NodeID,
			ShardID: comp.ShardID,
			Type:    comp.Type,
			Address: comp.Address,
			Status:  StatusUnknown,
		}
		c.logs[keyFor(comp)] = nil
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

// An unqualified engine URL is unambiguous only on a node with one engine.
func (c *Collector) findLocked(nodeID string, typ config.ComponentType, shardID string) (componentKey, bool) {
	key := componentKey{nodeID: nodeID, typeID: typ, shardID: shardID}
	if _, ok := c.states[key]; ok || shardID != "" {
		return key, ok
	}
	found := false
	for k := range c.states {
		if k.nodeID == nodeID && k.typeID == typ {
			if found {
				return componentKey{}, false
			}
			key, found = k, true
		}
	}
	return key, found
}

func (c *Collector) Lookup(nodeID string, typ config.ComponentType, shardID string) (ComponentState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	k, ok := c.findLocked(nodeID, typ, shardID)
	if !ok {
		return ComponentState{}, false
	}
	return *c.states[k], true
}

func (c *Collector) Logs(nodeID string, typ config.ComponentType, shardID string) ([]LogEntry, ComponentState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	k, ok := c.findLocked(nodeID, typ, shardID)
	if !ok {
		return nil, ComponentState{}, false
	}
	return slices.Clone(c.logs[k]), *c.states[k], true
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

	key := keyFor(comp)
	st := c.states[key]
	old := st.Status

	st.Status = status
	st.CheckedAt = time.Now().UTC()
	st.Error = errMsg

	if old != status {
		msg := old + " → " + status
		entry := LogEntry{
			Timestamp: st.CheckedAt,
			OldStatus: old,
			NewStatus: status,
			Error:     errMsg,
			Message:   string(msg),
		}
		logs := append(c.logs[key], entry)
		if len(logs) > maxLogEntries {
			logs = logs[len(logs)-maxLogEntries:]
		}
		c.logs[key] = logs

		log.Printf("component %s (%s) %s → %s  error=%q",
			comp.Address, st.Type, old, status, errMsg)
	}
}
