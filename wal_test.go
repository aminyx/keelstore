package keelstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type walRecord struct {
	kind  uint8
	seq   uint64
	key   string
	value string
}

func writeWAL(t *testing.T, path string, records []walRecord) *walWriter {
	t.Helper()
	w, err := newWALWriter(path)
	if err != nil {
		t.Fatalf("newWALWriter: %v", err)
	}
	for _, r := range records {
		if err := w.Append(encodeWALPayload(nil, r.kind, r.seq, []byte(r.key), []byte(r.value))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return w
}

func replayInto(t *testing.T, path string) ([]walRecord, walReplayResult) {
	t.Helper()
	var got []walRecord
	res, err := replayWAL(path, func(kind uint8, seq uint64, key, value []byte) error {
		got = append(got, walRecord{kind, seq, string(key), string(value)})
		return nil
	})
	if err != nil {
		t.Fatalf("replayWAL: %v", err)
	}
	return got, res
}

func TestWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	want := []walRecord{
		{kindValue, 1, "alpha", "one"},
		{kindValue, 2, "beta", ""},
		{kindDelete, 3, "alpha", ""},
		{kindValue, 4, "gamma", "a longer value than the others"},
	}
	w := writeWAL(t, path, want)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, res := replayInto(t, path)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("replayed %v, want %v", got, want)
	}
	if res.Records != len(want) || res.DroppedBytes != 0 || res.Reason != "" {
		t.Fatalf("clean replay reported %+v", res)
	}
}

func TestWALReplayOfAnEmptyFileIsClean(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got, res := replayInto(t, path)
	if len(got) != 0 || res.DroppedBytes != 0 || res.Reason != "" {
		t.Fatalf("empty file replayed as %v / %+v", got, res)
	}
}

// Flipping a byte inside a record's payload must be caught: this is the
// whole reason the CRC is there.
func TestWALChecksumDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w := writeWAL(t, path, []walRecord{
		{kindValue, 1, "alpha", "one"},
		{kindValue, 2, "beta", "two"},
		{kindValue, 3, "gamma", "three"},
	})
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt a byte inside the second record's payload.
	firstLen := int(data[4]) // little endian length of record 0, small enough
	victim := walHeaderLen + firstLen + walHeaderLen + 2
	data[victim] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	got, res := replayInto(t, path)
	if len(got) != 1 || got[0].key != "alpha" {
		t.Fatalf("replay returned %v, want only the record before the damage", got)
	}
	if res.Reason == "" || res.DroppedBytes <= 0 {
		t.Fatalf("corruption was not reported: %+v", res)
	}
	t.Logf("stopped at offset %d, dropped %d bytes: %s", res.LastGoodOffset, res.DroppedBytes, res.Reason)
}

func TestWALTornTailIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w := writeWAL(t, path, []walRecord{
		{kindValue, 1, "alpha", "one"},
		{kindValue, 2, "beta", "two"},
	})
	// The next record is cut off halfway, exactly as an interrupted write
	// would leave it.
	w.tornTail = 6
	if err := w.Append(encodeWALPayload(nil, kindValue, 3, []byte("gamma"), []byte("three"))); err != nil {
		t.Fatal(err)
	}
	w.Close()

	got, res := replayInto(t, path)
	if len(got) != 2 {
		t.Fatalf("replayed %d records, want the 2 complete ones", len(got))
	}
	if res.DroppedBytes != 6 {
		t.Fatalf("dropped %d bytes, want the 6 torn ones", res.DroppedBytes)
	}
	if res.Reason == "" {
		t.Fatal("a torn tail should be reported")
	}
}

func TestWALTornTailInsideThePayloadIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w := writeWAL(t, path, []walRecord{{kindValue, 1, "alpha", "one"}})
	// Keep the whole header but only part of the payload: the length field
	// promises more bytes than the file holds.
	w.tornTail = walHeaderLen + 3
	if err := w.Append(encodeWALPayload(nil, kindValue, 2, []byte("beta"), []byte("a value long enough to matter"))); err != nil {
		t.Fatal(err)
	}
	w.Close()

	got, res := replayInto(t, path)
	if len(got) != 1 || got[0].key != "alpha" {
		t.Fatalf("replayed %v, want only the complete record", got)
	}
	if res.DroppedBytes != int64(walHeaderLen+3) {
		t.Fatalf("dropped %d bytes, want %d", res.DroppedBytes, walHeaderLen+3)
	}
}

func TestWALTrailingGarbageShorterThanAHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w := writeWAL(t, path, []walRecord{{kindValue, 1, "alpha", "one"}})
	w.Close()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{1, 2, 3}) // three bytes: not even a header
	f.Close()

	got, res := replayInto(t, path)
	if len(got) != 1 {
		t.Fatalf("replayed %d records, want 1", len(got))
	}
	if res.DroppedBytes != 3 {
		t.Fatalf("dropped %d bytes, want 3", res.DroppedBytes)
	}
}

func TestWALReaderReportsEOFThenStops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w := writeWAL(t, path, []walRecord{{kindValue, 1, "a", "b"}})
	w.Close()

	r, err := newWALReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second call = %v, want io.EOF", err)
	}
}

func TestWALPayloadEncodeDecodeRoundTrip(t *testing.T) {
	cases := []walRecord{
		{kindValue, 1, "k", "v"},
		{kindDelete, maxSequence, "tombstone", ""},
		{kindValue, 12345, "binary\x00key", "binary\x00value"},
		{kindValue, 7, "empty-value", ""},
	}
	for _, c := range cases {
		kind, seq, k, v, err := decodeWALPayload(encodeWALPayload(nil, c.kind, c.seq, []byte(c.key), []byte(c.value)))
		if err != nil {
			t.Fatalf("decode(%q): %v", c.key, err)
		}
		if kind != c.kind || seq != c.seq || string(k) != c.key || string(v) != c.value {
			t.Fatalf("round trip changed %+v into %d/%d/%q/%q", c, kind, seq, k, v)
		}
	}
}

func TestWALPayloadDecodeRejectsGarbage(t *testing.T) {
	good := encodeWALPayload(nil, kindValue, 1, []byte("key"), []byte("value"))
	cases := map[string][]byte{
		"too short":     good[:4],
		"unknown kind":  append([]byte{99}, good[1:]...),
		"truncated key": good[:len(good)-6],
		"trailing junk": append(append([]byte(nil), good...), 0xff),
	}
	for name, buf := range cases {
		if _, _, _, _, err := decodeWALPayload(buf); err == nil {
			t.Errorf("%s: decode accepted a malformed payload", name)
		}
	}
}

func TestWALSizeTracksAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w, err := newWALWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.Size() != 0 {
		t.Fatalf("new log reports size %d", w.Size())
	}
	payload := encodeWALPayload(nil, kindValue, 1, []byte("k"), []byte("v"))
	if err := w.Append(payload); err != nil {
		t.Fatal(err)
	}
	if want := int64(walHeaderLen + len(payload)); w.Size() != want {
		t.Fatalf("size = %d, want %d", w.Size(), want)
	}
}

func TestWALSyncSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	w, err := newWALWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(encodeWALPayload(nil, kindValue, 1, []byte("k"), []byte("v"))); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}
