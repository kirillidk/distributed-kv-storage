package repl

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftClientStub struct {
	vote   func(context.Context, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error)
	append func(context.Context, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error)
}

func (s raftClientStub) RequestVote(ctx context.Context, in *raftv1.RequestVoteRequest, _ ...grpc.CallOption) (*raftv1.RequestVoteResponse, error) {
	if s.vote != nil {
		return s.vote(ctx, in)
	}
	return nil, status.Error(codes.Unavailable, "offline")
}

func (s raftClientStub) AppendEntries(ctx context.Context, in *raftv1.AppendEntriesRequest, _ ...grpc.CallOption) (*raftv1.AppendEntriesResponse, error) {
	if s.append != nil {
		return s.append(ctx, in)
	}
	return nil, status.Error(codes.Unavailable, "offline")
}

func testRaft(t *testing.T, terms ...uint64) *raftNode {
	t.Helper()
	dir := t.TempDir()
	w := openTestWAL(t, filepath.Join(dir, "wal"))
	for _, term := range terms {
		if _, err := w.AppendTerm(term, []byte("opaque payload")); err != nil {
			t.Fatal(err)
		}
	}
	_, lastTerm := w.LastEntry()
	path := filepath.Join(dir, "state")
	if err := saveRaftState(path, raftHardState{Term: lastTerm}); err != nil {
		t.Fatal(err)
	}
	r, err := newRaftNode("n1", map[string]raftv1.RaftServiceClient{"n2": raftClientStub{}, "n3": raftClientStub{}}, w, path, defaultRaftTiming, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func assertRaftStateOnDisk(t *testing.T, r *raftNode, term uint64, vote string) {
	t.Helper()
	got, err := loadRaftState(r.statePath)
	if err != nil || got.Term != term || got.VotedFor != vote {
		t.Fatalf("disk state = %+v, %v; want term=%d vote=%q", got, err, term, vote)
	}
}

func TestRequestVoteWALFreshness(t *testing.T) {
	for _, test := range []struct {
		name  string
		index uint64
		term  uint64
		grant bool
	}{
		{"empty", 0, 0, false},
		{"longer but older term", 100, 1, false},
		{"same term shorter", 1, 2, false},
		{"same log", 2, 2, true},
		{"same term longer", 3, 2, true},
		{"shorter but newer term", 1, 3, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := testRaft(t, 1, 2)
			due := r.electionDue
			response, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{
				Term: 4, CandidateId: "n2", LastLogIndex: test.index, LastLogTerm: test.term,
			})
			if err != nil || response.Term != 4 || response.VoteGranted != test.grant {
				t.Fatalf("RequestVote = %v, %v", response, err)
			}
			vote := ""
			if test.grant {
				vote = "n2"
			} else if !r.electionDue.Equal(due) {
				t.Fatal("rejected vote reset election timeout")
			}
			assertRaftStateOnDisk(t, r, 4, vote)
		})
	}
}

func TestRequestVotePersistsOneVotePerTermAcrossRestart(t *testing.T) {
	r := testRaft(t)
	for _, id := range []string{"n2", "n2", "n3"} {
		response, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 7, CandidateId: id})
		if err != nil || response.VoteGranted != (id == "n2") {
			t.Fatalf("vote for %s: %v, %v", id, response, err)
		}
		assertRaftStateOnDisk(t, r, 7, "n2")
		var reopenErr error
		r, reopenErr = newRaftNode(r.id, r.peers, r.wal, r.statePath, r.timing, testLogger())
		if reopenErr != nil {
			t.Fatal(reopenErr)
		}
	}
	due := r.electionDue
	response, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 6, CandidateId: "n3"})
	if err != nil || response.Term != 7 || response.VoteGranted || !r.electionDue.Equal(due) {
		t.Fatalf("stale vote: %v, %v", response, err)
	}
	response, err = r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 8, CandidateId: "n3"})
	if err != nil || !response.VoteGranted {
		t.Fatalf("new term vote: %v, %v", response, err)
	}
	assertRaftStateOnDisk(t, r, 8, "n3")
}

func TestAppendEntriesHeartbeatTermsAndConsistency(t *testing.T) {
	r := testRaft(t, 1, 2)
	r.mu.Lock()
	r.startElectionLocked(context.Background())
	r.mu.Unlock()
	r.workers.Wait()
	assertRaftStateOnDisk(t, r, 3, "n1")
	before, err := os.ReadFile(r.wal.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	due := r.electionDue
	response, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 2, LeaderId: "n2"})
	if err != nil || response.Success || response.Term != 3 || r.status().Role != candidate || !r.electionDue.Equal(due) {
		t.Fatalf("stale heartbeat: %v %v %+v", response, err, r.status())
	}
	for _, test := range []struct {
		index uint64
		term  uint64
		match bool
	}{{0, 0, true}, {1, 1, true}, {2, 2, true}, {3, 2, false}, {2, 1, false}} {
		response, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
			Term: 3, LeaderId: "n2", PrevLogIndex: test.index, PrevLogTerm: test.term, LeaderCommit: 2,
		})
		if err != nil || response.Success != test.match || r.status().Role != follower || r.status().LeaderID != "n2" {
			t.Fatalf("heartbeat %+v: %v, %v; %+v", test, response, err, r.status())
		}
		assertRaftStateOnDisk(t, r, 3, "n1") // Same-term heartbeat cannot erase our vote.
	}
	response, err = r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 4, LeaderId: "n3"})
	if err != nil || !response.Success || r.status().LeaderID != "n3" {
		t.Fatalf("higher term heartbeat: %v, %v", response, err)
	}
	assertRaftStateOnDisk(t, r, 4, "")
	after, err := os.ReadFile(r.wal.file.Name())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("heartbeats changed the WAL: %v", err)
	}
}

func TestElectionPersistsSelfVoteBeforeRPC(t *testing.T) {
	r := testRaft(t, 2, 3)
	var calls atomic.Int32
	for id := range r.peers {
		r.peers[id] = raftClientStub{vote: func(_ context.Context, in *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
			state, err := loadRaftState(r.statePath)
			if err != nil || state.Term != in.Term || state.VotedFor != r.id {
				t.Errorf("self vote was not durable before RPC: %+v, %v", state, err)
			}
			if in.LastLogIndex != 2 || in.LastLogTerm != 3 || in.Term != 4 {
				t.Errorf("wrong WAL metadata: %v", in)
			}
			calls.Add(1)
			return &raftv1.RequestVoteResponse{Term: in.Term}, nil
		}}
	}
	r.mu.Lock()
	r.startElectionLocked(context.Background())
	r.mu.Unlock()
	r.workers.Wait()
	if calls.Load() != 2 || r.status().Role != candidate {
		t.Fatalf("election without votes: %d RPCs, %+v", calls.Load(), r.status())
	}
	assertRaftStateOnDisk(t, r, 4, "n1")
	reopened, err := newRaftNode(r.id, r.peers, r.wal, r.statePath, r.timing, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	response, err := reopened.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 4, CandidateId: "n2", LastLogIndex: 2, LastLogTerm: 3})
	if err != nil || response.VoteGranted {
		t.Fatalf("forgot self vote after restart: %v, %v", response, err)
	}
}

func TestDelayedVotesCannotElectAfterStepDown(t *testing.T) {
	for _, term := range []uint64{1, 2} {
		t.Run(fmt.Sprintf("heartbeat-term-%d", term), func(t *testing.T) {
			r := testRaft(t)
			release := make(chan struct{})
			r.peers["n2"] = raftClientStub{vote: func(_ context.Context, in *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
				<-release
				return &raftv1.RequestVoteResponse{Term: in.Term, VoteGranted: true}, nil
			}}
			r.mu.Lock()
			r.startElectionLocked(context.Background())
			r.mu.Unlock()
			response, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: term, LeaderId: "n3"})
			close(release)
			r.workers.Wait()
			if err != nil || !response.Success || r.status().Role != follower || r.status().Term != term {
				t.Fatalf("late vote changed leadership: %v, %v; %+v", response, err, r.status())
			}
		})
	}
}

func TestHigherTermRepliesStepDownAndPersist(t *testing.T) {
	for _, fromHeartbeat := range []bool{false, true} {
		t.Run(fmt.Sprintf("heartbeat=%v", fromHeartbeat), func(t *testing.T) {
			r := testRaft(t)
			r.peers["n2"] = raftClientStub{
				vote: func(_ context.Context, in *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
					term := uint64(10)
					if fromHeartbeat {
						term = in.Term
					}
					return &raftv1.RequestVoteResponse{Term: term, VoteGranted: fromHeartbeat}, nil
				},
				append: func(_ context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
					if len(in.Entries) != 0 || in.Term != 1 {
						t.Errorf("unexpected heartbeat: %v", in)
					}
					return &raftv1.AppendEntriesResponse{Term: 10}, nil
				},
			}
			r.mu.Lock()
			r.startElectionLocked(context.Background())
			r.mu.Unlock()
			r.workers.Wait()
			if fromHeartbeat {
				if r.status().Role != leader {
					t.Fatalf("majority did not elect leader: %+v", r.status())
				}
				r.mu.Lock()
				r.heartbeatsLocked(context.Background())
				r.mu.Unlock()
				r.workers.Wait()
			}
			if r.status().Role != follower || r.status().Term != 10 {
				t.Fatalf("higher term reply did not demote: %+v", r.status())
			}
			assertRaftStateOnDisk(t, r, 10, "")
		})
	}
}

func TestRaftPersistenceFailureStopsVotingAndElections(t *testing.T) {
	for _, duringElection := range []bool{false, true} {
		t.Run(fmt.Sprintf("election=%v", duringElection), func(t *testing.T) {
			r := testRaft(t)
			r.statePath = filepath.Join(t.TempDir(), "missing-directory", "state")
			var calls atomic.Int32
			r.peers["n2"] = raftClientStub{vote: func(context.Context, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
				calls.Add(1)
				return &raftv1.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
			}}
			if duringElection {
				r.mu.Lock()
				r.startElectionLocked(context.Background())
				r.mu.Unlock()
				r.workers.Wait()
			} else {
				response, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 1, CandidateId: "n2"})
				if status.Code(err) != codes.Unavailable || response != nil {
					t.Fatalf("acknowledged unpersisted vote: %v, %v", response, err)
				}
			}
			if calls.Load() != 0 || r.status().Role != follower || r.fatalErr == nil {
				t.Fatal("continued election after persistence failure")
			}
			if _, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 1, LeaderId: "n3"}); status.Code(err) != codes.Unavailable {
				t.Fatalf("stopped node accepted heartbeat: %v", err)
			}
		})
	}
}

func TestRaftRejectsUnknownMembers(t *testing.T) {
	r := testRaft(t)
	due := r.electionDue
	if _, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 50, CandidateId: "outsider"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown candidate: %v", err)
	}
	if _, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 50, LeaderId: "outsider"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown leader: %v", err)
	}
	if r.status().Term != 0 || !r.electionDue.Equal(due) {
		t.Fatal("unknown member changed election state")
	}
}

func TestElectionTimeoutIsRandomizedOnEveryReset(t *testing.T) {
	r := testRaft(t)
	durations := make(map[time.Duration]bool)
	for range 50 {
		before := time.Now()
		r.resetElectionLocked()
		after := time.Now()
		if r.electionDue.Before(before.Add(r.timing.electionMin)) || r.electionDue.After(after.Add(r.timing.electionMax)) {
			t.Fatal("election timeout is outside configured range")
		}
		durations[r.electionDue.Sub(before).Truncate(time.Millisecond)] = true
	}
	if len(durations) < 2 {
		t.Fatal("election timeout is not randomized")
	}
}

func TestRaftRPCsRemainAvailableDuringEngineReplay(t *testing.T) {
	r := testRaft(t, 2)
	started := make(chan struct{})
	release := make(chan struct{})
	replayDone := make(chan error, 1)
	go func() {
		replayDone <- r.wal.Replay(0, func(uint64, []byte) error {
			close(started)
			<-release
			return nil
		})
	}()
	t.Cleanup(func() {
		close(release)
		if err := <-replayDone; err != nil {
			t.Error(err)
		}
	})
	<-started
	done := make(chan error, 1)
	go func() {
		vote, err := r.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 3, CandidateId: "n2", LastLogIndex: 1, LastLogTerm: 2})
		if err != nil || !vote.GetVoteGranted() {
			done <- fmt.Errorf("vote during replay: %v, %v", vote, err)
			return
		}
		heartbeat, err := r.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 3, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 2})
		if err != nil || !heartbeat.GetSuccess() {
			done <- fmt.Errorf("heartbeat during replay: %v, %v", heartbeat, err)
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("engine recovery blocked Raft RPCs")
	}
}
