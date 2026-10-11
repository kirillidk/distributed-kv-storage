package repl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTestWAL(t *testing.T, path string) *WAL {
	t.Helper()
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func appendTestWAL(t *testing.T, w *WAL, payload []byte) uint64 {
	t.Helper()
	offset, err := w.Append(payload)
	if err != nil {
		t.Fatal(err)
	}
	return offset
}

func TestWALFormatAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w := openTestWAL(t, path)
	if w.LastOffset() != 0 {
		t.Fatal("new WAL must start at zero")
	}
	first := []byte{0, 1, 255}
	if got := appendTestWAL(t, w, first); got != 3 {
		t.Fatalf("offset = %d", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 19 || binary.BigEndian.Uint32(data) != 3 || binary.BigEndian.Uint64(data[4:]) != 3 || !bytes.Equal(data[12:15], first) || binary.BigEndian.Uint32(data[15:]) != crc32.ChecksumIEEE(data[:15]) {
		t.Fatalf("unexpected record: %x", data)
	}
	appendTestWAL(t, w, []byte("next"))
	w.Close()
	w = openTestWAL(t, path)
	if w.LastOffset() != 7 {
		t.Fatalf("recovered offset = %d", w.LastOffset())
	}
	if got := appendTestWAL(t, w, []byte("!")); got != 8 {
		t.Fatalf("offset = %d", got)
	}
	var offsets []uint64
	var payloads [][]byte
	if err := w.Replay(3, func(offset uint64, p []byte) error {
		offsets = append(offsets, offset)
		payloads = append(payloads, p)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != 7 || offsets[1] != 8 || string(payloads[0]) != "next" || string(payloads[1]) != "!" {
		t.Fatalf("unexpected replay: %v %q", offsets, payloads)
	}
	for _, offset := range []uint64{1, 9} {
		if err := w.Replay(offset, func(uint64, []byte) error { t.Fatal("must not replay from invalid offset"); return nil }); !errors.Is(err, ErrOffset) {
			t.Fatalf("offset %d: %v", offset, err)
		}
	}
}

func TestWALTruncatesEveryIncompleteTail(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	w := openTestWAL(t, source)
	appendTestWAL(t, w, []byte("first"))
	boundary := w.end
	appendTestWAL(t, w, []byte("second"))
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	for cut := boundary + 1; cut < int64(len(data)); cut++ {
		for _, onOpen := range []bool{true, false} {
			path := filepath.Join(t.TempDir(), "wal")
			var recovered *WAL
			if !onOpen {
				recovered = openTestWAL(t, path)
			}
			if err := os.WriteFile(path, data[:cut], 0600); err != nil {
				t.Fatal(err)
			}
			if onOpen {
				recovered = openTestWAL(t, path)
			} else if err := recovered.Replay(0, nil); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != boundary || recovered.LastOffset() != 5 {
				t.Fatalf("cut %d: size=%d offset=%d", cut, info.Size(), recovered.LastOffset())
			}
			if got := appendTestWAL(t, recovered, []byte("new")); got != 8 {
				t.Fatalf("offset = %d", got)
			}
			if err := recovered.Replay(0, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "partial-first")
	if err := os.WriteFile(path, data[:3], 0600); err != nil {
		t.Fatal(err)
	}
	if got := openTestWAL(t, path).LastOffset(); got != 0 {
		t.Fatalf("offset = %d", got)
	}
}

func TestWALChecksumMismatchDoesNotTruncate(t *testing.T) {
	for _, corrupt := range []int{4, 12, 19} {
		path := filepath.Join(t.TempDir(), "wal")
		w := openTestWAL(t, path)
		appendTestWAL(t, w, []byte("data"))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data[corrupt] ^= 1
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := w.Replay(0, nil); !errors.Is(err, ErrChecksum) {
			t.Fatalf("read: %v", err)
		}
		w.Close()
		if _, err := OpenWAL(path); !errors.Is(err, ErrChecksum) {
			t.Fatalf("open: %v", err)
		}
		actual, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, data) {
			t.Fatal("corrupt complete record was changed")
		}
	}
}

func TestWALConcurrentAppends(t *testing.T) {
	w := openTestWAL(t, filepath.Join(t.TempDir(), "wal"))
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := w.Append([]byte("abc")); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if w.LastOffset() != 60 {
		t.Fatalf("offset = %d", w.LastOffset())
	}
	if err := w.Replay(0, nil); err != nil {
		t.Fatal(err)
	}
}

func TestWALRejectsEmptyAndClosedWrites(t *testing.T) {
	w := openTestWAL(t, filepath.Join(t.TempDir(), "wal"))
	if _, err := w.Append(nil); err == nil {
		t.Fatal("empty payload accepted")
	}
	w.Close()
	if _, err := w.Append([]byte("test")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed append: %v", err)
	}
	if w.LastOffset() != 0 {
		t.Fatal("failed append advanced offset")
	}
}

func BenchmarkWALScan(b *testing.B) {
	const records = 100000
	payload := bytes.Repeat([]byte{1}, 21)
	length := walHeaderSize + len(payload) + walChecksumSize
	data := make([]byte, records*length)
	for i := range records {
		record := data[i*length : (i+1)*length]
		binary.BigEndian.PutUint32(record, uint32(len(payload)))
		binary.BigEndian.PutUint64(record[4:], uint64((i+1)*len(payload)))
		copy(record[walHeaderSize:], payload)
		binary.BigEndian.PutUint32(record[length-walChecksumSize:], crc32.ChecksumIEEE(record[:length-walChecksumSize]))
	}
	path := filepath.Join(b.TempDir(), "wal")
	if err := os.WriteFile(path, data, 0600); err != nil {
		b.Fatal(err)
	}
	w, err := OpenWAL(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		if err := w.Replay(0, nil); err != nil {
			b.Fatal(err)
		}
	}
}
