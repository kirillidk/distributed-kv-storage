package repl

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kirillidk/distributed-kv-storage/api/replication"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var testRaftTiming = raftTiming{
	electionMin: 150 * time.Millisecond,
	electionMax: 300 * time.Millisecond,
	heartbeat:   30 * time.Millisecond,
	rpcTimeout:  80 * time.Millisecond,
}

// Every node has its own WAL, durable state and real gRPC transport. Offline
// members remain in each peer set and therefore still count towards quorum.
type raftTestCluster struct {
	nodes     []*raftNode
	servers   []*grpc.Server
	listeners []net.Listener
	cancels   []context.CancelFunc
	done      []chan error
	leaders   map[uint64]string
}

func newRaftTestCluster(t *testing.T, size, live int) *raftTestCluster {
	t.Helper()
	c := &raftTestCluster{
		nodes: make([]*raftNode, size), servers: make([]*grpc.Server, size),
		listeners: make([]net.Listener, size), cancels: make([]context.CancelFunc, size),
		done: make([]chan error, size), leaders: make(map[uint64]string),
	}
	for i := range size {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		c.listeners[i] = listener
	}
	for i := range size {
		peers := make(map[string]raftv1.RaftServiceClient)
		for j := range size {
			if i == j {
				continue
			}
			conn, err := grpc.NewClient(c.listeners[j].Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			peers[fmt.Sprintf("n%d", j)] = raftv1.NewRaftServiceClient(conn)
		}
		dir := t.TempDir()
		wal := openTestWAL(t, filepath.Join(dir, "wal"))
		node, err := newRaftNode(fmt.Sprintf("n%d", i), peers, wal, filepath.Join(dir, "state"), testRaftTiming, testLogger())
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[i] = node
		c.servers[i] = grpc.NewServer(grpc.MaxRecvMsgSize(maxRaftMessageSize))
		raftv1.RegisterRaftServiceServer(c.servers[i], node)
	}
	t.Cleanup(func() {
		for i := range size {
			c.stop(t, i)
		}
	})
	for i := range live {
		c.start(i)
	}
	return c
}

func (c *raftTestCluster) start(i int) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancels[i] = cancel
	c.done[i] = make(chan error, 1)
	go c.servers[i].Serve(c.listeners[i])
	go func() { c.done[i] <- c.nodes[i].run(ctx) }()
}

func (c *raftTestCluster) stop(t *testing.T, i int) {
	t.Helper()
	if c.cancels[i] == nil {
		return
	}
	c.cancels[i]()
	c.servers[i].Stop()
	select {
	case err := <-c.done[i]:
		if err != nil {
			t.Errorf("node %d stopped with error: %v", i, err)
		}
	case <-time.After(3 * time.Second):
		t.Errorf("node %d did not stop", i)
	}
	c.cancels[i] = nil
}

func (c *raftTestCluster) states(t *testing.T) map[int]raftStatus {
	t.Helper()
	states := make(map[int]raftStatus)
	for i, node := range c.nodes {
		if c.cancels[i] == nil {
			continue
		}
		s := node.status()
		states[i] = s
		if s.Role == leader {
			if previous := c.leaders[s.Term]; previous != "" && previous != node.id {
				t.Fatalf("two leaders in term %d: %s and %s", s.Term, previous, node.id)
			}
			c.leaders[s.Term] = node.id
		}
	}
	return states
}

func (c *raftTestCluster) waitLeader(t *testing.T, minTerm uint64) (int, raftStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		states := c.states(t)
		for i, s := range states {
			if s.Role != leader || s.Term < minTerm {
				continue
			}
			agree := true
			votes := 0
			for _, other := range states {
				agree = agree && other.Term == s.Term && other.LeaderID == c.nodes[i].id
				if other.VotedFor == c.nodes[i].id && other.Term == s.Term {
					votes++
				}
			}
			if agree && votes >= len(c.nodes)/2+1 {
				return i, s
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader with a majority: %+v", c.states(t))
	return 0, raftStatus{}
}

func TestRaftElectsLeaderWithMajorityAndHeartbeatsKeepTermStable(t *testing.T) {
	c := newRaftTestCluster(t, 5, 3)
	id, elected := c.waitLeader(t, 1)
	for deadline := time.Now().Add(4 * testRaftTiming.electionMax); time.Now().Before(deadline); {
		for i, s := range c.states(t) {
			if s.Term != elected.Term || s.LeaderID != c.nodes[id].id || (s.Role == leader) != (i == id) {
				t.Fatalf("heartbeat did not maintain leadership: elected=%+v, node=%d state=%+v", elected, i, s)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRaftReelectsAfterLeaderFailure(t *testing.T) {
	c := newRaftTestCluster(t, 3, 3)
	id, first := c.waitLeader(t, 1)
	c.stop(t, id)
	next, elected := c.waitLeader(t, first.Term+1)
	if next == id || elected.Term <= first.Term {
		t.Fatalf("failed to replace leader: first=%+v, next=%+v", first, elected)
	}
	for i, s := range c.states(t) {
		assertRaftStateOnDisk(t, c.nodes[i], s.Term, s.VotedFor)
	}
}

func TestRaftCannotElectWithoutQuorum(t *testing.T) {
	for _, size := range []int{3, 4, 5} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			live := size / 2
			c := newRaftTestCluster(t, size, live)
			for deadline := time.Now().Add(4 * testRaftTiming.electionMax); time.Now().Before(deadline); {
				for i, s := range c.states(t) {
					if s.Role == leader || s.LeaderID != "" {
						t.Fatalf("node %d elected with only %d/%d available: %+v", i, live, size, s)
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
			for i, s := range c.states(t) {
				if s.Term < 2 {
					t.Fatalf("node %d did not retry elections: %+v", i, s)
				}
			}
			// Availability must recover once a true majority is online.
			c.start(live)
			c.waitLeader(t, 2)
		})
	}
}

func TestRaftSingleNodeProposalsUseElectedTerm(t *testing.T) {
	c := newRaftTestCluster(t, 1, 1)
	r := c.nodes[0]
	if _, err := r.appendProposal(context.Background(), []byte("too early")); status.Code(err) != codes.Unavailable {
		t.Fatalf("proposal before election: %v", err)
	}
	_, elected := c.waitLeader(t, 1)
	offset, err := r.appendProposal(context.Background(), []byte("opaque"))
	if err != nil || offset != 6 {
		t.Fatalf("proposal: offset=%d, %v", offset, err)
	}
	index, term := r.wal.LastEntry()
	if index != 1 || term != elected.Term {
		t.Fatalf("WAL metadata: index=%d term=%d", index, term)
	}
	c.stop(t, 0)
	r.wal.Close()
	reopened := openTestWAL(t, r.wal.file.Name())
	if index, term := reopened.LastEntry(); index != 1 || term != elected.Term {
		t.Fatalf("WAL metadata after restart: index=%d term=%d", index, term)
	}
}

func TestRaftMultiNodeProposalsCommitOnMajority(t *testing.T) {
	c := newRaftTestCluster(t, 3, 3)
	id, _ := c.waitLeader(t, 1)
	for i, r := range c.nodes {
		if i == id {
			continue
		}
		if _, err := r.appendProposal(context.Background(), []byte("not the leader")); status.Code(err) != codes.Unavailable {
			t.Fatalf("node %d proposal: %v", i, err)
		}
		if r.wal.LastOffset() != 0 {
			t.Fatalf("node %d wrote an unreplicated proposal", i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Exercise the actual AppendEntries wire limit with a maximum-size command.
	payload := make([]byte, replication.MaxPayloadSize)
	payload[0], payload[len(payload)-1] = 7, 255
	offset, err := c.nodes[id].appendProposal(ctx, payload)
	if err != nil || offset != uint64(len(payload)) {
		t.Fatalf("leader proposal: %d, %v", offset, err)
	}
	for _, r := range c.nodes {
		waitForCommit(t, r, offset)
		entry, err := r.wal.ReadEntry(1)
		if err != nil || !bytes.Equal(entry.Payload, payload) {
			t.Fatalf("node %s replicated different payload: %v", r.id, err)
		}
	}
}

func TestRaftOfflineFollowersCatchUpAfterQuorumCommit(t *testing.T) {
	c := newRaftTestCluster(t, 5, 3)
	id, _ := c.waitLeader(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var offset uint64
	for _, payload := range []string{"first", "a longer second record"} {
		var err error
		offset, err = c.nodes[id].appendProposal(ctx, []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
	}
	c.start(3)
	c.start(4)
	for _, r := range c.nodes {
		waitForCommit(t, r, offset)
		if index, _ := r.wal.LastEntry(); index != 2 {
			t.Fatalf("node %s did not catch up: %d records", r.id, index)
		}
	}
}

func TestRaftLosingQuorumCannotCommitAndStopUnblocksProposal(t *testing.T) {
	c := newRaftTestCluster(t, 3, 3)
	id, _ := c.waitLeader(t, 1)
	for i := range c.nodes {
		if i != id {
			c.stop(t, i)
		}
	}
	r := c.nodes[id]
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := r.appendProposal(ctx, []byte("pending")); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("no-quorum proposal: %v", err)
	}
	if r.status().CommitOffset != 0 || r.wal.LastOffset() != 7 {
		t.Fatal("proposal was committed or lost without quorum")
	}
	done := make(chan error, 1)
	ctx, stopRequest := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopRequest()
	go func() { _, err := r.appendProposal(ctx, []byte("next")); done <- err }()
	waitForWAL(t, r, 11)
	c.stop(t, id)
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped leader left proposal pending: %v", err)
	}
}
