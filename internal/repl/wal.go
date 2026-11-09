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
	"sort"
	"sync"
)

var (
	ErrChecksum = errors.New("WAL checksum mismatch")
	ErrOffset   = errors.New("invalid WAL offset")
	ErrTerm     = errors.New("invalid WAL term")
)

const walHeaderSize = 12
const walTermHeaderSize = 20
const walChecksumSize = 4
const walTermFlag uint32 = 1 << 31

// WAL stores big-endian records: uint32 payload size, uint64 offset, payload,
// uint32 IEEE CRC32 of the header and payload. Offset is the cumulative payload
// size, matching EngineService.Apply; framing bytes do not count.
// Term-bearing records set the high bit of the size, complement the offset,
// and add a uint64 term after it. Complementing the offset makes a damaged
// format bit fail offset validation instead of looking like an incomplete tail.
// Legacy records remain readable and have term zero.
// A WAL file must have a single owner. Methods are safe for concurrent use.
type WAL struct {
	mu         sync.Mutex
	metaMu     sync.RWMutex
	file       *os.File
	lastOffset uint64
	end        int64
	records    []walRecord
}

type walRecord struct {
	term   uint64
	offset uint64
	end    int64
}

type walEntry struct {
	Term    uint64
	Offset  uint64
	Payload []byte
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
	w.metaMu.RLock()
	defer w.metaMu.RUnlock()
	return w.lastOffset
}

// LastEntry returns the Raft record index and term as one consistent snapshot.
// Metadata reads do not wait for Replay's engine callbacks, so engine recovery
// cannot block elections or heartbeats.
func (w *WAL) LastEntry() (index, term uint64) {
	w.metaMu.RLock()
	defer w.metaMu.RUnlock()
	if len(w.records) == 0 {
		return 0, 0
	}
	return uint64(len(w.records)), w.records[len(w.records)-1].term
}

func (w *WAL) Matches(index, term uint64) bool {
	w.metaMu.RLock()
	defer w.metaMu.RUnlock()
	if index == 0 {
		return term == 0
	}
	return index <= uint64(len(w.records)) && w.records[index-1].term == term
}

// EntryMeta maps a Raft record index to the engine byte offset and term.
// Index zero denotes the empty prefix.
func (w *WAL) EntryMeta(index uint64) (offset, term uint64, err error) {
	w.metaMu.RLock()
	defer w.metaMu.RUnlock()
	if index == 0 {
		return 0, 0, nil
	}
	if index > uint64(len(w.records)) {
		return 0, 0, ErrOffset
	}
	r := w.records[index-1]
	return r.offset, r.term, nil
}

func (w *WAL) IndexAtOffset(offset uint64) (uint64, error) {
	w.metaMu.RLock()
	defer w.metaMu.RUnlock()
	if offset == 0 {
		return 0, nil
	}
	i := sort.Search(len(w.records), func(i int) bool { return w.records[i].offset >= offset })
	if i == len(w.records) || w.records[i].offset != offset {
		return 0, ErrOffset
	}
	return uint64(i + 1), nil
}

// ReadEntry returns an owned, checksum-validated payload. No WAL lock needs to
// be held while sending it to a peer or applying it to the engine.
func (w *WAL) ReadEntry(index uint64) (walEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if index == 0 || index > uint64(len(w.records)) {
		return walEntry{}, ErrOffset
	}
	r := w.records[index-1]
	var start int64
	if index > 1 {
		start = w.records[index-2].end
	}
	data := make([]byte, r.end-start)
	if _, err := w.file.ReadAt(data, start); err != nil {
		return walEntry{}, err
	}
	end := len(data) - walChecksumSize
	if crc32.ChecksumIEEE(data[:end]) != binary.BigEndian.Uint32(data[end:]) {
		return walEntry{}, ErrChecksum
	}
	headerSize := walHeaderSize
	sizeAndFlag := binary.BigEndian.Uint32(data)
	offset := binary.BigEndian.Uint64(data[4:])
	var term uint64
	if sizeAndFlag&walTermFlag != 0 {
		headerSize = walTermHeaderSize
		if end < headerSize {
			return walEntry{}, ErrOffset
		}
		offset = ^offset
		term = binary.BigEndian.Uint64(data[12:])
	}
	if offset != r.offset || int64(sizeAndFlag&^walTermFlag) != int64(end-headerSize) {
		return walEntry{}, ErrOffset
	}
	if term != r.term {
		return walEntry{}, ErrTerm
	}
	return walEntry{Term: term, Offset: offset, Payload: data[headerSize:end]}, nil
}

// TruncateAfter durably removes a conflicting suffix. The Raft caller must
// ensure that index is at or beyond its committed prefix.
func (w *WAL) TruncateAfter(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if index > uint64(len(w.records)) {
		return ErrOffset
	}
	if index == uint64(len(w.records)) {
		return nil
	}
	var end int64
	var offset uint64
	if index > 0 {
		end, offset = w.records[index-1].end, w.records[index-1].offset
	}
	if err := w.file.Truncate(end); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.end = end
	w.metaMu.Lock()
	w.records = w.records[:index]
	w.lastOffset = offset
	w.metaMu.Unlock()
	return nil
}

// Append persists a payload before returning its end offset.
// It writes the legacy format, with term zero.
func (w *WAL) Append(payload []byte) (uint64, error) {
	return w.append(0, payload, false)
}

// AppendTerm persists the term and payload in the same checksummed record.
func (w *WAL) AppendTerm(term uint64, payload []byte) (uint64, error) {
	return w.append(term, payload, true)
}

func (w *WAL) append(term uint64, payload []byte, withTerm bool) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(payload) == 0 || uint64(len(payload)) >= uint64(walTermFlag) {
		return 0, errors.New("WAL payload size must be between 1 and int32 max")
	}
	if len(w.records) > 0 && term < w.records[len(w.records)-1].term {
		return 0, ErrTerm
	}
	if uint64(len(payload)) > math.MaxUint64-w.lastOffset {
		return 0, ErrOffset
	}
	offset := w.lastOffset + uint64(len(payload))
	header := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	binary.BigEndian.PutUint64(header[4:], offset)
	if withTerm {
		header = append(header, make([]byte, 8)...)
		binary.BigEndian.PutUint32(header, uint32(len(payload))|walTermFlag)
		binary.BigEndian.PutUint64(header[4:], ^offset)
		binary.BigEndian.PutUint64(header[12:], term)
	}
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
	w.metaMu.Lock()
	w.lastOffset = offset
	w.records = append(w.records, walRecord{term: term, offset: offset, end: position})
	w.metaMu.Unlock()
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
	var records []walRecord
	found := after == 0
	headerBuffer := make([]byte, walTermHeaderSize)
	var checksum [walChecksumSize]byte
	hash := crc32.NewIEEE()
	buffer := make([]byte, 32*1024)
	for position < info.Size() {
		if info.Size()-position < walHeaderSize {
			break
		}
		header := headerBuffer[:walHeaderSize]
		if _, err := w.file.ReadAt(header, position); err != nil {
			return err
		}
		sizeAndFlag := binary.BigEndian.Uint32(header)
		size := int64(sizeAndFlag &^ walTermFlag)
		offset := binary.BigEndian.Uint64(header[4:])
		if sizeAndFlag&walTermFlag != 0 {
			offset = ^offset
		}
		validOffset := size != 0 && uint64(size) <= math.MaxUint64-last && offset == last+uint64(size)
		headerSize := int64(walHeaderSize)
		if sizeAndFlag&walTermFlag != 0 {
			headerSize = walTermHeaderSize
		}
		length := headerSize + walChecksumSize + size
		if length > info.Size()-position {
			// A complete but inconsistent header is corrupt, not an interrupted
			// append. Never truncate committed records because size was damaged.
			if !validOffset {
				return fmt.Errorf("%w at byte %d", ErrOffset, position)
			}
			break
		}
		var term uint64
		if headerSize == walTermHeaderSize {
			header = headerBuffer[:walTermHeaderSize]
			if _, err := w.file.ReadAt(header[walHeaderSize:], position+walHeaderSize); err != nil {
				return err
			}
			term = binary.BigEndian.Uint64(header[walHeaderSize:])
		}
		hash.Reset()
		hash.Write(header)
		reader := io.NewSectionReader(w.file, position+headerSize, size)
		var payload []byte
		if apply != nil && offset > after {
			payload, err = io.ReadAll(io.TeeReader(reader, hash))
		} else {
			_, err = io.CopyBuffer(hash, reader, buffer)
		}
		if err != nil {
			return err
		}
		if _, err := w.file.ReadAt(checksum[:], position+headerSize+size); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(checksum[:]) != hash.Sum32() {
			return fmt.Errorf("%w at byte %d", ErrChecksum, position)
		}
		if !validOffset {
			return fmt.Errorf("%w at byte %d", ErrOffset, position)
		}
		if len(records) > 0 && term < records[len(records)-1].term {
			return fmt.Errorf("%w at byte %d", ErrTerm, position)
		}
		records = append(records, walRecord{term: term, offset: offset, end: position + length})
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
	w.end = position
	w.metaMu.Lock()
	w.lastOffset = last
	w.records = records
	w.metaMu.Unlock()
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
