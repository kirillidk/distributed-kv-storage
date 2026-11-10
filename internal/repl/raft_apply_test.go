package repl

import (
	"bytes"
	"context"
	"testing"
	"time"

	enginev1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/engine/v1"
	kvv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/kv/v1"
	replv1 "github.com/kirillidk/distributed-kv-storage/api/gen/go/repl/v1"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func waitForWAL(t *testing.T, r *raftNode, offset uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.wal.LastOffset() == offset {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("WAL offset = %d, want %d", r.wal.LastOffset(), offset)
}

func TestRaftProposalWaitsForQuorumAndCanceledCommitIsApplied(t *testing.T) {
	r := testRaft(t)
	leadInTerm(t, r, 1)
	e := &recordingEngine{result: []byte("committed result")}
	s := &proposalServer{wal: r.wal, raft: r, engine: e}
	if err := s.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.Propose(ctx, &replv1.ProposeRequest{Payload: []byte("first")})
		done <- err
	}()
	waitForWAL(t, r, 5)
	select {
	case err := <-done:
		t.Fatalf("proposal completed without quorum: %v", err)
	default:
	}
	e.mu.Lock()
	calls := len(e.applyCalls)
	e.mu.Unlock()
	if calls != 0 || r.status().CommitOffset != 0 {
		t.Fatal("uncommitted proposal reached Apply")
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("cancel pending proposal: %v", err)
	}
	if err := s.recover(context.Background()); err != nil || len(e.applyCalls) != 0 {
		t.Fatalf("recovery applied uncommitted tail: %v", err)
	}
	// The client's cancellation does not remove its durable entry. Once quorum
	// returns, it must be applied before any following proposal's result.
	r.peers["n2"] = raftClientStub{append: acceptAppend}
	ctx, cancelNext := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelNext()
	go func() {
		response, err := s.Propose(ctx, &replv1.ProposeRequest{Payload: []byte("second")})
		if err == nil && !bytes.Equal(response.Result, e.result) {
			err = status.Error(codes.Internal, "lost opaque engine result")
		}
		done <- err
	}()
	waitForWAL(t, r, 11)
	replicateRound(r)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(e.applyCalls) != 2 || e.applyCalls[0].Offset != 5 || string(e.applyCalls[0].Payload) != "first" || e.applyCalls[1].Offset != 11 || string(e.applyCalls[1].Payload) != "second" {
		t.Fatalf("Apply skipped or reordered commands: %v", e.applyCalls)
	}
}

func TestPendingProposalFailsOnTermChangeAndOnlyReplacementIsApplied(t *testing.T) {
	r := testRaft(t)
	leadInTerm(t, r, 1)
	e := &recordingEngine{}
	s := &proposalServer{wal: r.wal, raft: r, engine: e}
	if err := s.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.Propose(ctx, &replv1.ProposeRequest{Payload: []byte("old")})
		done <- err
	}()
	waitForWAL(t, r, 3)
	response, err := r.AppendEntries(ctx, &raftv1.AppendEntriesRequest{
		Term: 2, LeaderId: "n2", Entries: []*raftv1.LogEntry{{Term: 2, Payload: []byte("new")}}, LeaderCommit: 1,
	})
	if err != nil || !response.Success {
		t.Fatalf("replacement: %v, %v", response, err)
	}
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("old proposal completed using replacement at same offset: %v", err)
	}
	if err := s.recover(ctx); err != nil {
		t.Fatal(err)
	}
	if len(e.applyCalls) != 1 || string(e.applyCalls[0].Payload) != "new" {
		t.Fatalf("applied discarded proposal: %v", e.applyCalls)
	}
}

func TestRaftRecoveryAppliesOnlyPersistedCommitAndRetriesApply(t *testing.T) {
	r := testRaft(t)
	response, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term: 2, LeaderId: "n2", LeaderCommit: 1,
		Entries: []*raftv1.LogEntry{{Term: 1, Payload: []byte("committed")}, {Term: 2, Payload: []byte("uncommitted")}},
	})
	if err != nil || !response.Success {
		t.Fatalf("initial AppendEntries: %v, %v", response, err)
	}
	r.wal.Close()
	w := openTestWAL(t, r.wal.file.Name())
	r, err = newRaftNode(r.id, r.peers, w, r.statePath, r.timing, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	e := &recordingEngine{failNext: true}
	s := &proposalServer{wal: w, raft: r, engine: e}
	if err := s.recover(context.Background()); status.Code(err) != codes.Unavailable || s.ready {
		t.Fatalf("failed Apply did not leave recovery pending: %v", err)
	}
	if err := s.recover(context.Background()); err != nil || !s.ready || e.offset != 9 {
		t.Fatalf("recover committed prefix: %v, offset=%d", err, e.offset)
	}
	for _, call := range e.applyCalls {
		if call.Offset != 9 || string(call.Payload) != "committed" {
			t.Fatalf("replayed an uncommitted command: %v", call)
		}
	}
	response, err = r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term: 2, LeaderId: "n2", PrevLogIndex: 2, PrevLogTerm: 2, LeaderCommit: 2,
	})
	if err != nil || !response.Success {
		t.Fatalf("commit heartbeat: %v, %v", response, err)
	}
	// Start the same worker used by the supervisor; the healthy follower must
	// consume new commits even without a client proposal or an engine restart.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.recoverEngine(ctx, testLogger()) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		offset := e.offset
		e.mu.Unlock()
		if offset == w.LastOffset() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("follower did not apply new committed entries in the background")
}

func TestSlowCommittedApplyDoesNotBlockReplication(t *testing.T) {
	r := testRaft(t)
	if _, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: "n2", Entries: []*raftv1.LogEntry{{Term: 1, Payload: []byte("first")}}, LeaderCommit: 1,
	}); err != nil {
		t.Fatal(err)
	}
	e := &recordingEngine{}
	blocked := blockedApply{EngineServiceClient: e, started: make(chan struct{}), release: make(chan struct{})}
	s := &proposalServer{wal: r.wal, raft: r, engine: blocked}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.recover(ctx) }()
	<-blocked.started
	replicated := make(chan error, 1)
	go func() {
		_, err := r.AppendEntries(ctx, &raftv1.AppendEntriesRequest{
			Term: 1, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 1,
			Entries: []*raftv1.LogEntry{{Term: 1, Payload: []byte("second")}}, LeaderCommit: 2,
		})
		replicated <- err
	}()
	select {
	case err := <-replicated:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("engine Apply blocked AppendEntries")
	}
	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRaftClusterAppliesKVCommandsOnEveryNodeAndAfterFailover(t *testing.T) {
	c := newRaftTestCluster(t, 3, 3)
	clients := make([]kvv1.KVServiceClient, 3)
	proposals := make([]*proposalServer, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i, r := range c.nodes {
		s, client, _ := recoveryFixture(t)
		s.wal, s.raft = r.wal, r
		proposals[i], clients[i] = s, client
		if err := s.recover(ctx); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); s.recoverEngine(ctx, testLogger()) }()
		t.Cleanup(func() { cancel(); <-done })
	}
	requestCtx, stopRequest := context.WithTimeout(ctx, 10*time.Second)
	defer stopRequest()
	id, elected := c.waitLeader(t, 1)
	if _, err := clients[id].Set(requestCtx, &kvv1.SetRequest{Key: []byte("counter"), Value: []byte("10")}); err != nil {
		t.Fatal(err)
	}
	if response, err := clients[id].Incr(requestCtx, &kvv1.IncrRequest{Key: []byte("counter"), Delta: 2}); err != nil || response.GetValue() != 12 {
		t.Fatalf("committed Incr result: %v, %v", response, err)
	}
	offset := c.nodes[id].status().CommitOffset
	for i, s := range proposals {
		waitForCommit(t, c.nodes[i], offset)
		if err := s.recover(requestCtx); err != nil {
			t.Fatal(err)
		}
		response, err := clients[i].Get(requestCtx, &kvv1.GetRequest{Key: []byte("counter")})
		if err != nil || string(response.GetValue()) != "12" {
			t.Fatalf("node %d applied different state: %v, %v", i, response, err)
		}
	}
	c.stop(t, id)
	next, _ := c.waitLeader(t, elected.Term+1)
	if response, err := clients[next].Incr(requestCtx, &kvv1.IncrRequest{Key: []byte("counter"), Delta: 5}); err != nil || response.GetValue() != 17 {
		t.Fatalf("Incr after failover: %v, %v", response, err)
	}
	// Both surviving engines converge to the same durable commit offset.
	for i, s := range proposals {
		if i == id {
			continue
		}
		waitForCommit(t, c.nodes[i], c.nodes[next].status().CommitOffset)
		if err := s.recover(requestCtx); err != nil {
			t.Fatal(err)
		}
		state, err := s.engine.GetOffset(requestCtx, &enginev1.GetOffsetRequest{})
		if err != nil || state.GetOffset() != c.nodes[i].status().CommitOffset {
			t.Fatalf("node %d engine/commit offsets differ: %v, %v", i, state, err)
		}
	}
}
