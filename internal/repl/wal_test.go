package repl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

func TestWALCorruptPayloadSizeDoesNotTruncate(t *testing.T) {
	for record := range 2 {
		for sizeByte := range 4 {
			for _, onOpen := range []bool{true, false} {
				t.Run(fmt.Sprintf("record=%d/byte=%d/open=%t", record, sizeByte, onOpen), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "wal")
					w := openTestWAL(t, path)
					appendTestWAL(t, w, []byte("data"))
					appendTestWAL(t, w, []byte("next"))
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if onOpen {
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
					}
					const recordSize = walHeaderSize + 4 + walChecksumSize
					data[record*recordSize+sizeByte] ^= 1
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
					if onOpen {
						var reopened *WAL
						reopened, err = OpenWAL(path)
						if reopened != nil {
							reopened.Close()
						}
					} else {
						err = w.Replay(0, nil)
						if w.LastOffset() != 8 {
							t.Error("corruption changed lastOffset")
						}
					}
					if !errors.Is(err, ErrChecksum) && !errors.Is(err, ErrOffset) {
						t.Errorf("expected corruption error, got %v", err)
					}
					actual, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(actual, data) {
						t.Errorf("corrupt log was changed: bytes before=%d after=%d", len(data), len(actual))
					}
				})
			}
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

func TestWALTermRecordsAndLegacyPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w := openTestWAL(t, path)
	appendTestWAL(t, w, []byte("legacy"))
	boundary := w.end
	if _, err := w.AppendTerm(7, []byte("new")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	record := data[boundary:]
	if len(record) != walTermHeaderSize+3+walChecksumSize ||
		binary.BigEndian.Uint32(record) != walTermFlag|3 ||
		^binary.BigEndian.Uint64(record[4:]) != 9 || binary.BigEndian.Uint64(record[12:]) != 7 ||
		string(record[20:23]) != "new" || binary.BigEndian.Uint32(record[23:]) != crc32.ChecksumIEEE(record[:23]) {
		t.Fatalf("unexpected term record: %x", record)
	}
	w.Close()
	w = openTestWAL(t, path)
	if index, term := w.LastEntry(); index != 2 || term != 7 || w.LastOffset() != 9 {
		t.Fatalf("recovered metadata: index=%d term=%d offset=%d", index, term, w.LastOffset())
	}
	if !w.Matches(1, 0) || !w.Matches(2, 7) || w.Matches(2, 0) || w.Matches(3, 7) {
		t.Fatal("wrong terms for mixed WAL")
	}
	var replayed []string
	if err := w.Replay(0, func(_ uint64, payload []byte) error {
		replayed = append(replayed, string(payload))
		return nil
	}); err != nil || len(replayed) != 2 || replayed[0] != "legacy" || replayed[1] != "new" {
		t.Fatalf("mixed replay: %v, %v", replayed, err)
	}
	if _, err := w.AppendTerm(6, []byte("out of order")); !errors.Is(err, ErrTerm) {
		t.Fatalf("append older term: %v", err)
	}
	if _, err := w.AppendTerm(8, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if index, term := w.LastEntry(); index != 3 || term != 8 {
		t.Fatalf("appended metadata: index=%d term=%d", index, term)
	}

	// Every possible interrupted append must recover the legacy prefix and
	// its term/index, including cuts inside the new term header.
	for cut := boundary + 1; cut < int64(len(data)); cut++ {
		tailPath := filepath.Join(t.TempDir(), "wal")
		if err := os.WriteFile(tailPath, data[:cut], 0600); err != nil {
			t.Fatal(err)
		}
		recovered := openTestWAL(t, tailPath)
		if index, term := recovered.LastEntry(); index != 1 || term != 0 || recovered.LastOffset() != 6 {
			t.Fatalf("cut %d: index=%d term=%d offset=%d", cut, index, term, recovered.LastOffset())
		}
		if _, err := recovered.AppendTerm(8, []byte("replacement")); err != nil {
			t.Fatal(err)
		}
		if err := recovered.Replay(0, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWALTruncateAndReadPreserveRecordBoundariesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w := openTestWAL(t, path)
	appendTestWAL(t, w, []byte("legacy"))
	if _, err := w.AppendTerm(1, []byte("discarded suffix")); err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateAfter(3); !errors.Is(err, ErrOffset) {
		t.Fatalf("truncated past the end: %v", err)
	}
	if err := w.TruncateAfter(1); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w = openTestWAL(t, path)
	if index, term := w.LastEntry(); index != 1 || term != 0 || w.LastOffset() != 6 {
		t.Fatal("truncation was not durable")
	}
	if _, err := w.AppendTerm(2, []byte("new")); err != nil {
		t.Fatal(err)
	}
	for index, want := range []walEntry{{Term: 0, Offset: 6, Payload: []byte("legacy")}, {Term: 2, Offset: 9, Payload: []byte("new")}} {
		entry, err := w.ReadEntry(uint64(index + 1))
		if err != nil || entry.Term != want.Term || entry.Offset != want.Offset || !bytes.Equal(entry.Payload, want.Payload) {
			t.Fatalf("entry %d: %+v, %v", index, entry, err)
		}
		gotIndex, err := w.IndexAtOffset(entry.Offset)
		if err != nil || gotIndex != uint64(index+1) {
			t.Fatalf("offset %d maps to %d: %v", entry.Offset, gotIndex, err)
		}
	}
	for _, offset := range []uint64{1, 8, 10} {
		if _, err := w.IndexAtOffset(offset); !errors.Is(err, ErrOffset) {
			t.Fatalf("accepted offset %d between records: %v", offset, err)
		}
	}
	// Replication reads revalidate checksums instead of silently transmitting
	// corruption introduced after the log was opened.
	if _, err := w.file.WriteAt([]byte{255}, walHeaderSize); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ReadEntry(1); !errors.Is(err, ErrChecksum) {
		t.Fatalf("read corrupted payload: %v", err)
	}
	if err := w.TruncateAfter(0); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w = openTestWAL(t, path)
	if index, _ := w.LastEntry(); index != 0 || w.LastOffset() != 0 {
		t.Fatal("empty prefix was not restored")
	}
}

func TestWALCorruptFormatBitAndTermNeverTruncate(t *testing.T) {
	for _, withTerm := range []bool{false, true} {
		positions := []int{0}
		if withTerm {
			positions = append(positions, 12)
		}
		for _, position := range positions {
			path := filepath.Join(t.TempDir(), "wal")
			w := openTestWAL(t, path)
			if withTerm {
				if _, err := w.AppendTerm(1, []byte("data")); err != nil {
					t.Fatal(err)
				}
			} else {
				appendTestWAL(t, w, []byte("data"))
			}
			w.Close()
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[position] ^= 0x80
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenWAL(path); err == nil {
				reopened.Close()
				t.Fatal("accepted corrupted WAL format bit or term")
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("corruption truncated WAL: %v", err)
			}
		}
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
