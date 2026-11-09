package repl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftRole string

const (
	follower  raftRole = "follower"
	candidate raftRole = "candidate"
	leader    raftRole = "leader"
)

type raftTiming struct {
	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	rpcTimeout  time.Duration
}

var defaultRaftTiming = raftTiming{
	electionMin: 500 * time.Millisecond,
	electionMax: time.Second,
	heartbeat:   100 * time.Millisecond,
	rpcTimeout:  200 * time.Millisecond,
}

type raftStatus struct {
	Role         raftRole
	Term         uint64
	VotedFor     string
	LeaderID     string
	CommitOffset uint64
}

// raftNode implements elections, log replication and quorum commit. All state transitions
// and WAL appends are serialized by mu; network calls never hold that lock.
// Membership is static and includes every node in the manifest, even offline ones.
type raftNode struct {
	raftv1.UnimplementedRaftServiceServer
	mu             sync.Mutex
	id             string
	peers          map[string]raftv1.RaftServiceClient
	wal            *WAL
	statePath      string
	hard           raftHardState
	role           raftRole
	leaderID       string
	votes          map[string]bool
	timing         raftTiming
	electionDue    time.Time
	heartbeatDue   time.Time
	heartbeatBusy  map[string]bool
	nextIndex      map[string]uint64
	matchIndex     map[string]uint64
	commitOffset   uint64
	changed        chan struct{}
	commitReady    chan struct{}
	wake           chan struct{}
	workers        sync.WaitGroup
	started        bool
	stopped        bool
	fatalErr       error
	logger         *slog.Logger
	onLeaderChange func(bool)
}

func newRaftNode(id string, peers map[string]raftv1.RaftServiceClient, wal *WAL, statePath string, timing raftTiming, logger *slog.Logger) (*raftNode, error) {
	if id == "" || wal == nil || statePath == "" {
		return nil, errors.New("Raft requires a node ID, WAL and state path")
	}
	if timing.electionMin <= 0 || timing.electionMax <= timing.electionMin ||
		timing.heartbeat <= 0 || timing.heartbeat >= timing.electionMin ||
		timing.rpcTimeout <= 0 || timing.rpcTimeout >= timing.electionMin {
		return nil, errors.New("invalid Raft timing")
	}
	peerCopy := make(map[string]raftv1.RaftServiceClient, len(peers))
	for peerID, client := range peers {
		if peerID == "" || peerID == id || client == nil {
			return nil, errors.New("invalid Raft peer")
		}
		peerCopy[peerID] = client
	}
	hard, err := loadRaftState(statePath)
	_, lastTerm := wal.LastEntry()
	if errors.Is(err, os.ErrNotExist) && lastTerm == 0 {
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("open Raft state: %w", err)
	}
	if hard.Term < lastTerm {
		return nil, errors.New("Raft state term is older than the WAL")
	}
	commitOffset, _, err := wal.EntryMeta(hard.CommitIndex)
	if err != nil {
		return nil, fmt.Errorf("committed Raft prefix is missing from WAL: %w", err)
	}
	if hard.VotedFor != "" && hard.VotedFor != id && peerCopy[hard.VotedFor] == nil {
		return nil, errors.New("persisted Raft vote is outside the cluster")
	}
	// Also sync a state left by a process that died between rename and the
	// directory fsync, before an unchanged vote can be acknowledged again.
	if err := saveRaftState(statePath, hard); err != nil {
		return nil, fmt.Errorf("sync initial Raft state: %w", err)
	}
	r := &raftNode{
		id: id, peers: peerCopy, wal: wal, statePath: statePath, hard: hard,
		role: follower, timing: timing, wake: make(chan struct{}, 1),
		heartbeatBusy: make(map[string]bool), logger: logger,
		commitOffset: commitOffset, changed: make(chan struct{}), commitReady: make(chan struct{}, 1),
	}
	r.resetElectionLocked()
	return r, nil
}

func (r *raftNode) status() raftStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return raftStatus{Role: r.role, Term: r.hard.Term, VotedFor: r.hard.VotedFor, LeaderID: r.leaderID, CommitOffset: r.commitOffset}
}

func (r *raftNode) signalLocked() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *raftNode) resetElectionLocked() {
	r.electionDue = time.Now().Add(r.timing.electionMin + time.Duration(rand.Int64N(int64(r.timing.electionMax-r.timing.electionMin))))
	r.signalLocked()
}

func (r *raftNode) roleLocked(role raftRole, leaderID string) {
	wasLeader := r.role == leader
	if r.role != role || r.leaderID != leaderID {
		r.logger.Info("raft role changed", "node_id", r.id, "role", role, "term", r.hard.Term, "leader_id", leaderID)
	}
	r.role, r.leaderID = role, leaderID
	r.notifyChangedLocked()
	if r.onLeaderChange != nil && wasLeader != (role == leader) {
		r.onLeaderChange(role == leader)
	}
	r.signalLocked()
}

func (r *raftNode) failLocked(err error) error {
	r.fatalErr = err
	r.stopped = true
	r.roleLocked(follower, "")
	return status.Error(codes.Unavailable, err.Error())
}

func (r *raftNode) persistLocked(state raftHardState) error {
	if state == r.hard {
		return nil
	}
	if err := saveRaftState(r.statePath, state); err != nil {
		return r.failLocked(fmt.Errorf("persist Raft state: %w", err))
	}
	r.hard = state
	return nil
}

func (r *raftNode) higherTermLocked(term uint64) error {
	if term <= r.hard.Term {
		return nil
	}
	if err := r.persistLocked(raftHardState{Term: term, CommitIndex: r.hard.CommitIndex}); err != nil {
		return err
	}
	r.roleLocked(follower, "")
	return nil
}

func (r *raftNode) RequestVote(ctx context.Context, in *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.availableLocked(ctx); err != nil {
		return nil, err
	}
	if in == nil || in.Term == 0 || r.peers[in.CandidateId] == nil ||
		in.LastLogTerm > in.Term || (in.LastLogIndex == 0 && in.LastLogTerm != 0) {
		return nil, status.Error(codes.InvalidArgument, "invalid vote request or unknown candidate")
	}
	if err := r.higherTermLocked(in.Term); err != nil {
		return nil, err
	}
	response := &raftv1.RequestVoteResponse{Term: r.hard.Term}
	if in.Term < r.hard.Term || (r.hard.VotedFor != "" && r.hard.VotedFor != in.CandidateId) {
		return response, nil
	}
	index, term := r.wal.LastEntry()
	if in.LastLogTerm < term || (in.LastLogTerm == term && in.LastLogIndex < index) {
		return response, nil
	}
	if err := r.persistLocked(raftHardState{Term: in.Term, VotedFor: in.CandidateId, CommitIndex: r.hard.CommitIndex}); err != nil {
		return nil, err
	}
	// Rejected votes, including stale-log candidates in a higher term, must not
	// postpone this node's own election indefinitely.
	r.resetElectionLocked()
	response.VoteGranted = true
	return response, nil
}

func (r *raftNode) AppendEntries(ctx context.Context, in *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.availableLocked(ctx); err != nil {
		return nil, err
	}
	if err := r.validateAppendLocked(in); err != nil {
		return nil, err
	}
	if err := r.higherTermLocked(in.Term); err != nil {
		return nil, err
	}
	response := &raftv1.AppendEntriesResponse{Term: r.hard.Term}
	if in.Term < r.hard.Term {
		return response, nil
	}
	r.roleLocked(follower, in.LeaderId)
	r.resetElectionLocked()
	if !r.wal.Matches(in.PrevLogIndex, in.PrevLogTerm) {
		return response, nil
	}
	if err := r.receiveEntriesLocked(in); err != nil {
		return nil, err
	}
	response.Success = true
	return response, nil
}

func (r *raftNode) availableLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if r.stopped {
		return status.Error(codes.Unavailable, "Raft node is stopped")
	}
	return nil
}

func (r *raftNode) run(ctx context.Context) error {
	r.mu.Lock()
	if r.started || r.stopped {
		r.mu.Unlock()
		return errors.New("Raft node cannot be restarted in memory")
	}
	r.started = true
	r.resetElectionLocked()
	r.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		r.mu.Lock()
		r.stopped = true
		r.roleLocked(follower, "")
		r.mu.Unlock()
		r.workers.Wait()
	}()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		case <-r.wake:
		}
		r.mu.Lock()
		if r.fatalErr != nil {
			err := r.fatalErr
			r.mu.Unlock()
			return err
		}
		if ctx.Err() != nil {
			r.mu.Unlock()
			return nil
		}
		if r.role != leader && !time.Now().Before(r.electionDue) {
			r.startElectionLocked(ctx)
		}
		due := r.electionDue
		if r.role == leader {
			if !time.Now().Before(r.heartbeatDue) {
				r.heartbeatsLocked(ctx)
			}
			due = r.heartbeatDue
		}
		timer.Reset(max(0, time.Until(due)))
		r.mu.Unlock()
	}
}

func (r *raftNode) startElectionLocked(ctx context.Context) {
	if r.hard.Term == math.MaxUint64 {
		r.failLocked(errors.New("Raft term exhausted"))
		return
	}
	if err := r.persistLocked(raftHardState{Term: r.hard.Term + 1, VotedFor: r.id, CommitIndex: r.hard.CommitIndex}); err != nil {
		return
	}
	r.roleLocked(candidate, "")
	r.resetElectionLocked()
	r.votes = map[string]bool{r.id: true}
	if r.hasMajorityLocked() {
		r.becomeLeaderLocked()
		return
	}
	index, term := r.wal.LastEntry()
	request := &raftv1.RequestVoteRequest{Term: r.hard.Term, CandidateId: r.id, LastLogIndex: index, LastLogTerm: term}
	for id, peer := range r.peers {
		r.workers.Add(1)
		go func() {
			defer r.workers.Done()
			rpcCtx, cancel := context.WithTimeout(ctx, r.timing.rpcTimeout)
			defer cancel()
			response, err := peer.RequestVote(rpcCtx, request)
			if err != nil || response == nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped {
				return
			}
			// A higher term matters even in a delayed reply to an old election.
			if response.Term > r.hard.Term {
				if err := r.higherTermLocked(response.Term); err == nil {
					r.resetElectionLocked()
				}
				return
			}
			if r.role != candidate || r.hard.Term != request.Term || response.Term != request.Term || !response.VoteGranted {
				return
			}
			r.votes[id] = true
			if r.hasMajorityLocked() {
				r.becomeLeaderLocked()
			}
		}()
	}
}

func (r *raftNode) hasMajorityLocked() bool {
	return len(r.votes) >= (len(r.peers)+1)/2+1
}

func (r *raftNode) becomeLeaderLocked() {
	last, _ := r.wal.LastEntry()
	r.nextIndex = make(map[string]uint64, len(r.peers))
	r.matchIndex = make(map[string]uint64, len(r.peers))
	for id := range r.peers {
		r.nextIndex[id] = last + 1
	}
	r.roleLocked(leader, r.id)
	r.heartbeatDue = time.Time{} // Send the first heartbeat immediately.
}
