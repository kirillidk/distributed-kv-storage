package repl

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/kirillidk/distributed-kv-storage/api/replication"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func waitForCommit(t *testing.T, r *raftNode, offset uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.status().CommitOffset == offset {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s: commit offset %d, want %d", r.id, r.status().CommitOffset, offset)
}

func leadInTerm(t *testing.T, r *raftNode, term uint64) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.persistLocked(raftHardState{Term: term, VotedFor: r.id, CommitIndex: r.hard.CommitIndex}); err != nil {
		t.Fatal(err)
	}
	r.becomeLeaderLocked()
}

func replicateRound(r *raftNode) {
	r.mu.Lock()
	r.heartbeatsLocked(context.Background())
	r.mu.Unlock()
	r.workers.Wait()
}

func acceptAppend(_ context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return &raftv1.AppendEntriesResponse{Term: in.Term, Success: true}, nil
}

func TestAppendEntriesReplacesOnlyConflictingUncommittedSuffix(t *testing.T) {
	r := testRaft(t, 1, 2, 4, 4)
	ctx := context.Background()
	before, err := os.ReadFile(r.wal.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	request := &raftv1.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 2, PrevLogTerm: 3, LeaderCommit: 99,
		Entries: []*raftv1.LogEntry{{Term: 3, Payload: []byte{0, 255}}, {Term: 5, Payload: []byte("replacement")}},
	}
	response, err := r.AppendEntries(ctx, request)
	if err != nil || response.Success || r.status().CommitOffset != 0 {
		t.Fatalf("mismatched prefix: %v, %v", response, err)
	}
	after, err := os.ReadFile(r.wal.file.Name())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected AppendEntries changed the WAL")
	}
	request.PrevLogTerm = 2
	response, err = r.AppendEntries(ctx, request)
	wantOffset := uint64(2*len("opaque payload") + 2 + len("replacement"))
	if err != nil || !response.Success || r.status().CommitOffset != wantOffset {
		t.Fatalf("replacement: %v, %v, commit=%d", response, err, r.status().CommitOffset)
	}
	// A repeated, shorter batch must neither duplicate records nor truncate
	// the matching suffix. A stale leaderCommit cannot move commit backwards.
	request.Entries = request.Entries[:1]
	request.LeaderCommit = 1
	response, err = r.AppendEntries(ctx, request)
	if err != nil || !response.Success || r.wal.LastOffset() != wantOffset || r.status().CommitOffset != wantOffset {
		t.Fatalf("duplicate: %v, %v", response, err)
	}
	request.Entries = []*raftv1.LogEntry{{Term: 5, Payload: []byte("cannot overwrite commit")}}
	response, err = r.AppendEntries(ctx, request)
	if status.Code(err) != codes.FailedPrecondition || response != nil || r.wal.LastOffset() != wantOffset {
		t.Fatalf("committed conflict: %v, %v", response, err)
	}
	r.wal.Close()
	w := openTestWAL(t, r.wal.file.Name())
	for index, want := range []struct {
		term    uint64
		payload []byte
	}{{1, []byte("opaque payload")}, {2, []byte("opaque payload")}, {3, []byte{0, 255}}, {5, []byte("replacement")}} {
		entry, err := w.ReadEntry(uint64(index + 1))
		if err != nil || entry.Term != want.term || !bytes.Equal(entry.Payload, want.payload) {
			t.Fatalf("durable entry %d: %+v, %v", index, entry, err)
		}
	}
	reopened, err := newRaftNode(r.id, r.peers, w, r.statePath, r.timing, testLogger())
	if err != nil || reopened.status().CommitOffset != wantOffset {
		t.Fatalf("lost committed offset after restart: %v", err)
	}
}

func TestFollowerCommitsOnlyPrefixMatchedByRequest(t *testing.T) {
	r := testRaft(t, 1, 2, 2)
	for _, request := range []*raftv1.AppendEntriesRequest{
		{Term: 3, LeaderId: "n2", LeaderCommit: 100},
		{Term: 3, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 100},
		{Term: 3, LeaderId: "n2", PrevLogIndex: 2, PrevLogTerm: 3, LeaderCommit: 100},
	} {
		response, err := r.AppendEntries(context.Background(), request)
		if err != nil || response.Success != (request.PrevLogIndex != 2) {
			t.Fatalf("heartbeat: %v, %v", response, err)
		}
		index, _ := r.committed()
		if index != min(request.PrevLogIndex, 1) {
			t.Fatalf("committed unmatched suffix: %d", index)
		}
	}
}

func TestAppendEntriesValidatesEntireBatchBeforeChangingWAL(t *testing.T) {
	for name, entries := range map[string][]*raftv1.LogEntry{
		"nil":                {nil},
		"empty payload":      {{Term: 2}},
		"term beyond leader": {{Term: 4, Payload: []byte("x")}},
		"regressing term":    {{Term: 3, Payload: []byte("x")}, {Term: 2, Payload: []byte("y")}},
		"term before prefix": {{Term: 0, Payload: []byte("x")}},
		"oversize":           {{Term: 2, Payload: make([]byte, replication.MaxPayloadSize+1)}},
	} {
		t.Run(name, func(t *testing.T) {
			r := testRaft(t, 1)
			response, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
				Term: 3, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries,
			})
			if err == nil || response != nil || r.status().Term != 1 || r.wal.LastOffset() != uint64(len("opaque payload")) {
				t.Fatalf("invalid batch mutated state: %v, %v", response, err)
			}
		})
	}
	r := testRaft(t)
	if _, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term: 1, LeaderId: "n2", PrevLogIndex: math.MaxUint64, Entries: []*raftv1.LogEntry{{Term: 1, Payload: []byte("x")}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("index overflow: %v", err)
	}
}

func TestAppendEntriesDoesNotAcknowledgePersistenceFailure(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%v", failCommit), func(t *testing.T) {
			r := testRaft(t, 1)
			if failCommit {
				r.statePath = filepath.Join(t.TempDir(), "missing", "state")
			} else {
				r.wal.Close()
			}
			response, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
				Term: 1, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 1,
				Entries: []*raftv1.LogEntry{{Term: 1, Payload: []byte("new")}}, LeaderCommit: 2,
			})
			if status.Code(err) != codes.Unavailable || response != nil || r.status().CommitOffset != 0 || !r.stopped {
				t.Fatalf("acknowledged persistence failure: %v, %v", response, err)
			}
		})
	}
}

func TestQuorumCommitRequiresDistinctDurableVotesAndCurrentTerm(t *testing.T) {
	r := testRaft(t, 1)
	r.peers["n4"] = raftClientStub{} // Four members require three durable copies.
	r.peers["n2"] = raftClientStub{append: acceptAppend}
	leadInTerm(t, r, 2)
	for range 3 {
		replicateRound(r)
	}
	if r.status().CommitOffset != 0 {
		t.Fatal("repeated responses from one follower counted as a majority")
	}
	r.peers["n3"] = raftClientStub{append: acceptAppend}
	replicateRound(r)
	if r.status().CommitOffset != 0 {
		t.Fatal("majority replication committed an old-term entry")
	}
	offset, err := r.wal.AppendTerm(2, []byte("current term"))
	if err != nil {
		t.Fatal(err)
	}
	// Even a response carrying success is not a durable confirmation in this
	// leadership term when it carries a stale term or an RPC error.
	r.peers["n3"] = raftClientStub{append: func(context.Context, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
		return &raftv1.AppendEntriesResponse{Term: 1, Success: true}, nil
	}}
	r.peers["n4"] = raftClientStub{append: func(context.Context, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
		return &raftv1.AppendEntriesResponse{Term: 2, Success: true}, status.Error(codes.Unavailable, "disk failed")
	}}
	replicateRound(r)
	if r.status().CommitOffset != 0 {
		t.Fatal("committed without current-term durable quorum")
	}
	r.peers["n3"] = raftClientStub{append: acceptAppend}
	replicateRound(r)
	index, committed := r.committed()
	if index != 2 || committed != offset {
		t.Fatalf("current-term quorum did not commit entire prefix: %d, %d", index, committed)
	}
	hard, err := loadRaftState(r.statePath)
	if err != nil || hard.CommitIndex != 2 {
		t.Fatalf("commit was not persisted: %+v, %v", hard, err)
	}
}

func TestLeaderBacktracksAndRepairsFollowerLog(t *testing.T) {
	r := testRaft(t, 1, 3, 3)
	f := testRaft(t, 1, 2, 2)
	f.id = "n2"
	f.peers = map[string]raftv1.RaftServiceClient{"n1": raftClientStub{}, "n3": raftClientStub{}}
	var preceding []uint64
	r.peers["n2"] = raftClientStub{append: func(ctx context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
		preceding = append(preceding, in.PrevLogIndex)
		return f.AppendEntries(ctx, in)
	}}
	leadInTerm(t, r, 4)
	offset, err := r.wal.AppendTerm(4, []byte("commits earlier terms too"))
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		replicateRound(r)
	}
	if len(preceding) != 4 || preceding[0] != 3 || preceding[1] != 2 || preceding[2] != 1 || preceding[3] != 4 {
		t.Fatalf("did not backtrack to matching prefix: %v", preceding)
	}
	if r.status().CommitOffset != offset || f.status().CommitOffset != offset {
		t.Fatalf("commit was not propagated: leader=%+v follower=%+v", r.status(), f.status())
	}
	for index := uint64(1); index <= 4; index++ {
		want, err := r.wal.ReadEntry(index)
		if err != nil {
			t.Fatal(err)
		}
		got, err := f.wal.ReadEntry(index)
		if err != nil || got.Term != want.Term || got.Offset != want.Offset || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("entry %d differs: %+v, %v", index, got, err)
		}
	}
}

func TestDelayedReplicationReplyCannotCommitAfterStepDown(t *testing.T) {
	r := testRaft(t, 1)
	leadInTerm(t, r, 1)
	release := make(chan struct{})
	r.peers["n2"] = raftClientStub{append: func(ctx context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
		<-release
		return acceptAppend(ctx, in)
	}}
	r.mu.Lock()
	r.heartbeatsLocked(context.Background())
	r.mu.Unlock()
	response, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 2, CandidateId: "n3", LastLogIndex: 1, LastLogTerm: 1})
	close(release)
	r.workers.Wait()
	if err != nil || !response.VoteGranted || r.status().CommitOffset != 0 || r.status().Role != follower {
		t.Fatalf("old reply changed commit: %v, %v, %+v", response, err, r.status())
	}
	// Progress is reset at each election; a new leader must obtain fresh acks.
	leadInTerm(t, r, 3)
	if r.matchIndex["n2"] != 0 || r.nextIndex["n2"] != 2 {
		t.Fatal("replication progress survived an election")
	}
}

func TestReplicationBatchesRespectMessageAndEntryLimits(t *testing.T) {
	r := testRaft(t)
	leadInTerm(t, r, 1)
	for range maxAppendEntries + 1 {
		if _, err := r.wal.AppendTerm(1, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if _, err := r.wal.AppendTerm(1, make([]byte, replication.MaxPayloadSize)); err != nil {
			t.Fatal(err)
		}
	}
	var counts []int
	var preceding []uint64
	r.peers["n2"] = raftClientStub{append: func(ctx context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
		if proto.Size(in) > maxRaftMessageSize || len(in.Entries) > maxAppendEntries {
			t.Errorf("oversized batch: %d bytes, %d entries", proto.Size(in), len(in.Entries))
		}
		counts = append(counts, len(in.Entries))
		preceding = append(preceding, in.PrevLogIndex)
		return acceptAppend(ctx, in)
	}}
	for range 3 {
		replicateRound(r)
	}
	if !slices.Equal(counts, []int{maxAppendEntries, 2, 1}) || !slices.Equal(preceding, []uint64{0, maxAppendEntries, maxAppendEntries + 2}) {
		t.Fatalf("unexpected batching: counts=%v prev=%v", counts, preceding)
	}
	if r.status().CommitOffset != r.wal.LastOffset() {
		t.Fatal("batched replication failed to commit the complete log")
	}
}
