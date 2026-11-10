package repl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

type raftHardState struct {
	Term        uint64
	VotedFor    string
	CommitIndex uint64
}

// The checksum covers the format tag, term, vote and commit index. A corrupt
// state must never be treated as a fresh node: that could allow a second vote
// or replacement of a committed entry. RFT1 has no known commit boundary.
func loadRaftState(path string) (raftHardState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return raftHardState{}, err
	}
	if len(data) < 20 || (string(data[:4]) != "RFT1" && string(data[:4]) != "RFT2") ||
		binary.BigEndian.Uint32(data[len(data)-4:]) != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return raftHardState{}, errors.New("invalid Raft state or checksum")
	}
	state := raftHardState{Term: binary.BigEndian.Uint64(data[4:])}
	voteStart := 16
	if string(data[:4]) == "RFT2" {
		if len(data) < 28 {
			return raftHardState{}, errors.New("incomplete Raft state")
		}
		state.CommitIndex = binary.BigEndian.Uint64(data[16:])
		voteStart = 24
	}
	if uint64(binary.BigEndian.Uint32(data[12:])) != uint64(len(data)-voteStart-4) {
		return raftHardState{}, errors.New("invalid Raft vote length")
	}
	state.VotedFor = string(data[voteStart : len(data)-4])
	if state.Term == 0 && state.VotedFor != "" {
		return raftHardState{}, errors.New("Raft vote without a term")
	}
	return state, nil
}

// Replace the entire state atomically, then sync the directory before replying
// to an RPC or sending requests in a new term. A failed save is fatal even if
// rename succeeded, since the durability of the new vote is then uncertain.
func saveRaftState(path string, state raftHardState) error {
	data := make([]byte, 28+len(state.VotedFor))
	copy(data, "RFT2")
	binary.BigEndian.PutUint64(data[4:], state.Term)
	binary.BigEndian.PutUint32(data[12:], uint32(len(state.VotedFor)))
	binary.BigEndian.PutUint64(data[16:], state.CommitIndex)
	copy(data[24:], state.VotedFor)
	binary.BigEndian.PutUint32(data[len(data)-4:], crc32.ChecksumIEEE(data[:len(data)-4]))
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("sync Raft state directory: %w", err)
	}
	return nil
}
