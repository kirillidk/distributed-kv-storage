package repl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRaftStateRejectsCorruptionAndIncompleteWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	want := raftHardState{Term: 17, VotedFor: "node-2"}
	if err := saveRaftState(path, want); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var corruptions [][]byte
	for cut := range len(data) {
		corruptions = append(corruptions, bytes.Clone(data[:cut]))
		flipped := bytes.Clone(data)
		flipped[cut] ^= 1
		corruptions = append(corruptions, flipped)
	}
	corruptions = append(corruptions, append(bytes.Clone(data), 0))
	for _, corrupt := range corruptions {
		if err := os.WriteFile(path, corrupt, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadRaftState(path); err == nil {
			t.Fatalf("accepted corrupt state: %x", corrupt)
		}
	}
	if err := saveRaftState(path, want); err != nil {
		t.Fatal(err)
	}
	if got, err := loadRaftState(path); err != nil || got != want {
		t.Fatalf("restored state: %+v, %v", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("state permissions: %v, %v", info, err)
	}
}

func TestRaftRefusesMissingOrOlderStateWithTermBearingWAL(t *testing.T) {
	r := testRaft(t, 3)
	if err := os.Remove(r.statePath); err != nil {
		t.Fatal(err)
	}
	if _, err := newRaftNode(r.id, r.peers, r.wal, r.statePath, r.timing, testLogger()); err == nil {
		t.Fatal("started with a lost Raft state")
	}
	if err := saveRaftState(r.statePath, raftHardState{Term: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := newRaftNode(r.id, r.peers, r.wal, r.statePath, r.timing, testLogger()); err == nil {
		t.Fatal("started with a Raft term older than WAL")
	}
	if err := os.WriteFile(r.statePath, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := newRaftNode(r.id, r.peers, r.wal, r.statePath, r.timing, testLogger()); err == nil {
		t.Fatal("reset a corrupt state")
	}
	got, err := os.ReadFile(r.statePath)
	if err != nil || string(got) != "corrupt" {
		t.Fatalf("changed corrupt state: %q, %v", got, err)
	}
}
