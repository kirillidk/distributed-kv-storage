package repl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrChecksum = errors.New("WAL checksum mismatch")
	ErrOffset   = errors.New("invalid WAL offset")
)

const walHeaderSize = 12
const walChecksumSize = 4

// WAL stores big-endian records: uint32 payload size, uint64 offset, payload,
// uint32 IEEE CRC32 of the header and payload. Offset is the cumulative payload
// size, matching EngineService.Apply; framing bytes do not count.
// A WAL file must have a single owner. Methods are safe for concurrent use.
type WAL struct {
	mu         sync.Mutex
	file       *os.File
	lastOffset uint64
	end        int64
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	w := &WAL{file: f}
	if err := w.scan(0, nil); err != nil {
		f.Close()
		return nil, err
	}
	// Persist both the file inode and its directory entry before accepting writes.
	// Sync on every open also covers a file left by an interrupted first startup.
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("sync WAL: %w", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sync WAL directory: %w", err)
	}
	return w, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (w *WAL) LastOffset() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastOffset
}

// Append persists a payload before returning its end offset.
func (w *WAL) Append(payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(payload) == 0 || uint64(len(payload)) > math.MaxUint32 {
		return 0, errors.New("WAL payload size must be between 1 and uint32 max")
	}
	if uint64(len(payload)) > math.MaxUint64-w.lastOffset {
		return 0, ErrOffset
	}
	offset := w.lastOffset + uint64(len(payload))
	header := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	binary.BigEndian.PutUint64(header[4:], offset)
	hash := crc32.NewIEEE()
	hash.Write(header)
	hash.Write(payload)
	checksum := make([]byte, walChecksumSize)
	binary.BigEndian.PutUint32(checksum, hash.Sum32())
	position := w.end
	for _, part := range [][]byte{header, payload, checksum} {
		n, err := w.file.WriteAt(part, position)
		if err == nil && n != len(part) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return 0, w.rollback(err)
		}
		position += int64(n)
	}
	if err := w.file.Sync(); err != nil {
		return 0, w.rollback(err)
	}
	w.end = position
	w.lastOffset = offset
	return offset, nil
}

func (w *WAL) rollback(cause error) error {
	return errors.Join(cause, w.file.Truncate(w.end), w.file.Sync())
}

// Replay validates the file and delivers records after a known engine offset.
// It removes an incomplete final record, including partial framing. The callback
// must not call WAL methods. Payload ownership is transferred to the callback.
func (w *WAL) Replay(after uint64, apply func(offset uint64, payload []byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.scan(after, apply)
}

func (w *WAL) scan(after uint64, apply func(uint64, []byte) error) error {
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	var position int64
	var last uint64
	found := after == 0
	header := make([]byte, walHeaderSize)
	var checksum [walChecksumSize]byte
	hash := crc32.NewIEEE()
	buffer := make([]byte, 32*1024)
	for position < info.Size() {
		if info.Size()-position < walHeaderSize {
			break
		}
		if _, err := w.file.ReadAt(header, position); err != nil {
			return err
		}
		size := int64(binary.BigEndian.Uint32(header))
		offset := binary.BigEndian.Uint64(header[4:])
		validOffset := size != 0 && uint64(size) <= math.MaxUint64-last && offset == last+uint64(size)
		length := int64(walHeaderSize+walChecksumSize) + size
		if length > info.Size()-position {
			// A complete but inconsistent header is corrupt, not an interrupted
			// append. Never truncate committed records because size was damaged.
			if !validOffset {
				return fmt.Errorf("%w at byte %d", ErrOffset, position)
			}
			break
		}
		hash.Reset()
		hash.Write(header)
		reader := io.NewSectionReader(w.file, position+walHeaderSize, size)
		var payload []byte
		if apply != nil && offset > after {
			payload, err = io.ReadAll(io.TeeReader(reader, hash))
		} else {
			_, err = io.CopyBuffer(hash, reader, buffer)
		}
		if err != nil {
			return err
		}
		if _, err := w.file.ReadAt(checksum[:], position+walHeaderSize+size); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(checksum[:]) != hash.Sum32() {
			return fmt.Errorf("%w at byte %d", ErrChecksum, position)
		}
		if !validOffset {
			return fmt.Errorf("%w at byte %d", ErrOffset, position)
		}
		if offset == after {
			found = true
		}
		if offset > after && apply != nil {
			if !found {
				return ErrOffset
			}
			if err := apply(offset, payload); err != nil {
				return err
			}
		}
		last = offset
		position += length
	}
	if !found {
		return ErrOffset
	}
	if position != info.Size() {
		if err := w.file.Truncate(position); err != nil {
			return err
		}
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	w.lastOffset, w.end = last, position
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
