package repl

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/kirillidk/distributed-kv-storage/api/replication"
	raftv1 "github.com/kirillidk/distributed-kv-storage/internal/repl/gen/go/repl/raft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// AppendEntries needs room for the leader ID, terms and repeated entry framing
// in addition to a maximum-size engine command.
const maxRaftMessageSize = replication.MaxPayloadSize + 64*1024
const maxAppendEntries = 128

func (r *raftNode) validateAppendLocked(in *raftv1.AppendEntriesRequest) error {
	if in == nil || in.Term == 0 || r.peers[in.LeaderId] == nil ||
		in.PrevLogTerm > in.Term || (in.PrevLogIndex == 0 && in.PrevLogTerm != 0) ||
		uint64(len(in.Entries)) > math.MaxUint64-in.PrevLogIndex {
		return status.Error(codes.InvalidArgument, "invalid AppendEntries or unknown leader")
	}
	if proto.Size(in) > maxRaftMessageSize {
		return status.Error(codes.ResourceExhausted, "AppendEntries is too large")
	}
	term := in.PrevLogTerm
	for _, entry := range in.Entries {
		if entry == nil || len(entry.Payload) == 0 || entry.Term < term || entry.Term > in.Term {
			return status.Error(codes.InvalidArgument, "invalid log entry")
		}
		if len(entry.Payload) > replication.MaxPayloadSize {
			return status.Error(codes.ResourceExhausted, "log payload is too large")
		}
		term = entry.Term
	}
	return nil
}

// The request's preceding index/term has already matched. A duplicate or a
// shorter delayed batch must not remove an otherwise matching local suffix.
func (r *raftNode) receiveEntriesLocked(in *raftv1.AppendEntriesRequest) error {
	for i, entry := range in.Entries {
		index := in.PrevLogIndex + uint64(i) + 1
		if r.wal.Matches(index, entry.Term) {
			continue
		}
		if index <= r.hard.CommitIndex {
			return status.Error(codes.FailedPrecondition, "cannot replace a committed log entry")
		}
		if err := r.wal.TruncateAfter(index - 1); err != nil {
			return r.failLocked(fmt.Errorf("truncate Raft WAL: %w", err))
		}
		for _, remaining := range in.Entries[i:] {
			if _, err := r.wal.AppendTerm(remaining.Term, remaining.Payload); err != nil {
				return r.failLocked(fmt.Errorf("replicate Raft WAL: %w", err))
			}
		}
		break
	}
	// Only this request's matching prefix is proven to belong to the leader.
	// A longer local tail may contain entries from a deposed leader.
	return r.commitLocked(min(in.LeaderCommit, in.PrevLogIndex+uint64(len(in.Entries))))
}

func (r *raftNode) notifyChangedLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *raftNode) committed() (index, offset uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hard.CommitIndex, r.commitOffset
}

func (r *raftNode) commitLocked(index uint64) error {
	if index <= r.hard.CommitIndex {
		return nil
	}
	offset, _, err := r.wal.EntryMeta(index)
	if err != nil {
		return r.failLocked(fmt.Errorf("commit Raft WAL: %w", err))
	}
	hard := r.hard
	hard.CommitIndex = index
	// Persist the boundary before publishing commands to Apply. Restart must
	// replay exactly the committed prefix, never the entire durable WAL.
	if err := r.persistLocked(hard); err != nil {
		return err
	}
	r.commitOffset = offset
	r.notifyChangedLocked()
	select {
	case r.commitReady <- struct{}{}:
	default:
	}
	if r.role == leader {
		r.replicateSoonLocked()
	}
	return nil
}

func (r *raftNode) advanceCommitLocked() error {
	last, _ := r.wal.LastEntry() // Local appends are durable before being published.
	matches := make([]uint64, 1, len(r.peers)+1)
	matches[0] = last
	for id := range r.peers {
		matches = append(matches, r.matchIndex[id])
	}
	slices.Sort(matches)
	quorum := len(matches)/2 + 1
	index := matches[len(matches)-quorum]
	// Counting replicas of a previous-term entry cannot commit it. Committing
	// a current-term entry also commits the entire preceding prefix.
	if index > r.hard.CommitIndex && r.wal.Matches(index, r.hard.Term) {
		return r.commitLocked(index)
	}
	return nil
}

func (r *raftNode) replicateSoonLocked() {
	r.heartbeatDue = time.Time{}
	r.signalLocked()
}

func (r *raftNode) appendRequestLocked(id string) (*raftv1.AppendEntriesRequest, error) {
	prev := r.nextIndex[id] - 1
	_, term, err := r.wal.EntryMeta(prev)
	if err != nil {
		return nil, err
	}
	request := &raftv1.AppendEntriesRequest{
		Term: r.hard.Term, LeaderId: r.id, PrevLogIndex: prev, PrevLogTerm: term,
		LeaderCommit: r.hard.CommitIndex,
	}
	last, _ := r.wal.LastEntry()
	for index := prev + 1; index <= last && len(request.Entries) < maxAppendEntries; index++ {
		entry, err := r.wal.ReadEntry(index)
		if err != nil {
			return nil, err
		}
		request.Entries = append(request.Entries, &raftv1.LogEntry{Term: entry.Term, Payload: entry.Payload})
		if proto.Size(request) > maxRaftMessageSize {
			request.Entries = request.Entries[:len(request.Entries)-1]
			if len(request.Entries) == 0 {
				return nil, fmt.Errorf("log entry %d exceeds AppendEntries limit", index)
			}
			break
		}
	}
	return request, nil
}

func (r *raftNode) heartbeatsLocked(ctx context.Context) {
	r.heartbeatDue = time.Now().Add(r.timing.heartbeat)
	for id, peer := range r.peers {
		if r.heartbeatBusy[id] {
			continue
		}
		request, err := r.appendRequestLocked(id)
		if err != nil {
			r.failLocked(fmt.Errorf("read Raft WAL: %w", err))
			return
		}
		// One outstanding request per peer keeps replies ordered. This flag is
		// retained across elections; an old reply cannot clear a new RPC's slot.
		r.heartbeatBusy[id] = true
		r.workers.Add(1)
		go func() {
			defer r.workers.Done()
			rpcCtx, cancel := context.WithTimeout(ctx, r.timing.rpcTimeout)
			defer cancel()
			response, err := peer.AppendEntries(rpcCtx, request)
			r.mu.Lock()
			defer r.mu.Unlock()
			r.heartbeatBusy[id] = false
			if r.stopped || err != nil || response == nil {
				return
			}
			if response.Term > r.hard.Term {
				if err := r.higherTermLocked(response.Term); err == nil {
					r.resetElectionLocked()
				}
				return
			}
			if r.role != leader || r.hard.Term != request.Term || response.Term != request.Term {
				return
			}
			if !response.Success {
				if r.nextIndex[id] > r.matchIndex[id]+1 {
					r.nextIndex[id]--
					r.replicateSoonLocked()
				}
				return
			}
			matched := request.PrevLogIndex + uint64(len(request.Entries))
			r.matchIndex[id] = max(r.matchIndex[id], matched)
			r.nextIndex[id] = r.matchIndex[id] + 1
			if err := r.advanceCommitLocked(); err != nil {
				return
			}
			last, _ := r.wal.LastEntry()
			if r.nextIndex[id] <= last || request.LeaderCommit < r.hard.CommitIndex {
				r.replicateSoonLocked()
			}
		}()
	}
}

// appendProposal returns only after a current-term quorum commit. Waiting does
// not hold mu, so peer RPCs, elections and client cancellation remain live.
func (r *raftNode) appendProposal(ctx context.Context, payload []byte) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.availableLocked(ctx); err != nil {
		return 0, err
	}
	if r.role != leader {
		return 0, status.Error(codes.Unavailable, "local repl is not the Raft leader")
	}
	if len(payload) == 0 {
		return 0, status.Error(codes.InvalidArgument, "empty payload")
	}
	if len(payload) > replication.MaxPayloadSize {
		return 0, status.Error(codes.ResourceExhausted, "payload is too large")
	}
	offset, err := r.wal.AppendTerm(r.hard.Term, payload)
	if err != nil {
		return 0, r.failLocked(fmt.Errorf("append Raft WAL: %w", err))
	}
	index, term := r.wal.LastEntry()
	if err := r.advanceCommitLocked(); err != nil {
		return 0, err
	}
	r.replicateSoonLocked()
	for {
		if err := r.availableLocked(ctx); err != nil {
			return 0, err
		}
		if r.role != leader || r.hard.Term != term {
			return 0, status.Error(codes.Unavailable, "Raft leadership changed before proposal completed")
		}
		if r.hard.CommitIndex >= index {
			return offset, nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.mu.Lock()
	}
}
