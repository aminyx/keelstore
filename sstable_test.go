package keelstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// buildTable writes n entries (key-000000..) into a table and returns the
// path, the metadata and an open reader.
func buildTable(t *testing.T, opts Options, n int) (string, *tableMeta, *sstReader) {
	t.Helper()
	opts = opts.withDefaults()
	path := filepath.Join(t.TempDir(), "000007.sst")
	w, err := newSSTWriter(path, 7, opts)
	if err != nil {
		t.Fatalf("newSSTWriter: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := w.Add(makeInternalKey([]byte(key(i)), uint64(i+1), kindValue), []byte(value(i))); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := openSSTable(path, 7)
	if err != nil {
		t.Fatalf("openSSTable: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return path, meta, r
}

func TestSSTableWriteReadRoundTrip(t *testing.T) {
	const n = 500
	_, meta, r := buildTable(t, smallOptions(), n)

	if meta.Entries != n {
		t.Fatalf("meta says %d entries, want %d", meta.Entries, n)
	}
	if string(userKeyOf(meta.Smallest)) != key(0) || string(userKeyOf(meta.Largest)) != key(n-1) {
		t.Fatalf("key range is %q..%q", userKeyOf(meta.Smallest), userKeyOf(meta.Largest))
	}
	if meta.MinSeq != 1 || meta.MaxSeq != n {
		t.Fatalf("sequence range %d..%d, want 1..%d", meta.MinSeq, meta.MaxSeq, n)
	}

	for i := 0; i < n; i++ {
		v, kind, found, filtered, err := r.Get([]byte(key(i)), maxSequence)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !found || filtered {
			t.Fatalf("Get(%d): found=%v filtered=%v", i, found, filtered)
		}
		if kind != kindValue || string(v) != value(i) {
			t.Fatalf("Get(%d) = %q/%d", i, v, kind)
		}
	}
}

func TestSSTableSparseIndexHasOneEntryPerBlock(t *testing.T) {
	opts := smallOptions()
	opts.BlockSize = 256
	_, _, r := buildTable(t, opts, 500)

	if len(r.index) < 2 {
		t.Fatalf("500 entries into 256 byte blocks produced %d index entries", len(r.index))
	}
	if len(r.index) >= 500 {
		t.Fatalf("index has %d entries: it is not sparse", len(r.index))
	}
	// Index entries must be sorted and must match the block they name.
	for i := 1; i < len(r.index); i++ {
		if compareInternalKeys(r.index[i-1].firstKey, r.index[i].firstKey) >= 0 {
			t.Fatalf("index entry %d is out of order", i)
		}
	}
	for i := range r.index {
		block, err := r.readBlock(i)
		if err != nil {
			t.Fatalf("readBlock(%d): %v", i, err)
		}
		it := &sstIter{r: r, blockIdx: i, block: block}
		if !it.decodeAt() {
			t.Fatalf("block %d decoded to nothing", i)
		}
		if !bytes.Equal(it.Key(), r.index[i].firstKey) {
			t.Fatalf("block %d starts with %q but the index says %q", i, it.Key(), r.index[i].firstKey)
		}
	}
}

func TestSSTableFindBlockBinarySearch(t *testing.T) {
	opts := smallOptions()
	opts.BlockSize = 128
	_, _, r := buildTable(t, opts, 300)

	for i := 0; i < 300; i++ {
		target := seekKey([]byte(key(i)), maxSequence)
		b := r.findBlock(target)
		if b < 0 || b >= len(r.index) {
			t.Fatalf("findBlock returned %d of %d blocks", b, len(r.index))
		}
		if compareInternalKeys(r.index[b].firstKey, target) > 0 && b != 0 {
			t.Fatalf("block %d starts after the target for key %d", b, i)
		}
		if b+1 < len(r.index) && compareInternalKeys(r.index[b+1].firstKey, target) <= 0 {
			t.Fatalf("block %d is not the last one at or before key %d", b, i)
		}
	}
	// A key below everything in the file must land on the first block.
	if got := r.findBlock(seekKey([]byte("!!!"), maxSequence)); got != 0 {
		t.Fatalf("findBlock for a key below the file = %d, want 0", got)
	}
}

func TestSSTableIteratorCrossesBlockBoundaries(t *testing.T) {
	opts := smallOptions()
	opts.BlockSize = 64 // force many small blocks
	const n = 400
	_, _, r := buildTable(t, opts, n)

	if len(r.index) < 20 {
		t.Fatalf("expected many blocks, got %d", len(r.index))
	}
	it := r.NewIter()
	defer it.Close()

	count := 0
	var prev []byte
	for ok := it.First(); ok; ok = it.Next() {
		if prev != nil && compareInternalKeys(prev, it.Key()) >= 0 {
			t.Fatalf("iteration went backwards at entry %d", count)
		}
		prev = append(prev[:0], it.Key()...)
		if string(userKeyOf(it.Key())) != key(count) {
			t.Fatalf("entry %d is %q", count, userKeyOf(it.Key()))
		}
		count++
	}
	if it.Error() != nil {
		t.Fatalf("iterator error: %v", it.Error())
	}
	if count != n {
		t.Fatalf("iterated %d entries, want %d", count, n)
	}
}

func TestSSTableSeekAcrossAndPastBlocks(t *testing.T) {
	opts := smallOptions()
	opts.BlockSize = 64
	_, _, r := buildTable(t, opts, 200)
	it := r.NewIter()
	defer it.Close()

	for _, i := range []int{0, 1, 63, 64, 100, 199} {
		if !it.Seek(seekKey([]byte(key(i)), maxSequence)) {
			t.Fatalf("Seek(%d) failed", i)
		}
		if got := string(userKeyOf(it.Key())); got != key(i) {
			t.Fatalf("Seek(%d) landed on %q", i, got)
		}
	}
	if it.Seek(seekKey([]byte("zzzz"), maxSequence)) {
		t.Fatal("seek past the last key reported a value")
	}
	if !it.Seek(seekKey([]byte("!!!"), maxSequence)) {
		t.Fatal("seek before the first key should land on the first key")
	}
	if got := string(userKeyOf(it.Key())); got != key(0) {
		t.Fatalf("seek before the file landed on %q", got)
	}
}

func TestSSTableRespectsSequenceNumberOnGet(t *testing.T) {
	opts := smallOptions().withDefaults()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := newSSTWriter(path, 1, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Three versions of one key, newest first as the ordering demands.
	for _, v := range []struct {
		seq  uint64
		val  string
		kind uint8
	}{{30, "v30", kindValue}, {20, "", kindDelete}, {10, "v10", kindValue}} {
		if err := w.Add(makeInternalKey([]byte("k"), v.seq, v.kind), []byte(v.val)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := openSSTable(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	cases := []struct {
		seq      uint64
		wantVal  string
		wantKind uint8
	}{
		{40, "v30", kindValue},
		{30, "v30", kindValue},
		{25, "", kindDelete},
		{15, "v10", kindValue},
	}
	for _, c := range cases {
		v, kind, found, _, err := r.Get([]byte("k"), c.seq)
		if err != nil || !found {
			t.Fatalf("seq %d: found=%v err=%v", c.seq, found, err)
		}
		if kind != c.wantKind || string(v) != c.wantVal {
			t.Fatalf("seq %d = %q/%d, want %q/%d", c.seq, v, kind, c.wantVal, c.wantKind)
		}
	}
	if _, _, found, _, _ := r.Get([]byte("k"), 5); found {
		t.Fatal("a sequence number older than every version should find nothing")
	}
}

func TestSSTableBloomFilterSkipsAbsentKeys(t *testing.T) {
	_, _, r := buildTable(t, smallOptions(), 2000)

	skipped := 0
	for i := 10000; i < 11000; i++ {
		_, _, found, filtered, err := r.Get([]byte(key(i)), maxSequence)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("found a key that was never written: %d", i)
		}
		if filtered {
			skipped++
		}
	}
	if skipped < 900 {
		t.Fatalf("filter only rejected %d of 1000 absent keys", skipped)
	}
}

func TestSSTableFooterValidation(t *testing.T) {
	path, _, r := buildTable(t, smallOptions(), 100)
	r.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("bad magic", func(t *testing.T) {
		broken := append([]byte(nil), data...)
		broken[len(broken)-1] ^= 0xff
		p := filepath.Join(t.TempDir(), "000001.sst")
		os.WriteFile(p, broken, 0o644)
		if _, err := openSSTable(p, 1); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("opened a file with the wrong magic: %v", err)
		}
	})

	t.Run("bad footer checksum", func(t *testing.T) {
		broken := append([]byte(nil), data...)
		broken[len(broken)-sstFooterLen] ^= 0xff // corrupt the index offset
		p := filepath.Join(t.TempDir(), "000001.sst")
		os.WriteFile(p, broken, 0o644)
		if _, err := openSSTable(p, 1); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("opened a file with a bad footer CRC: %v", err)
		}
	})

	t.Run("truncated file", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "000001.sst")
		os.WriteFile(p, data[:20], 0o644)
		if _, err := openSSTable(p, 1); err == nil {
			t.Fatal("opened a file shorter than a footer")
		}
	})
}

func TestSSTableIndexChecksumDetectsCorruption(t *testing.T) {
	path, _, r := buildTable(t, smallOptions(), 300)
	r.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The index block sits just before the footer; damage its last byte of
	// payload, leaving the footer intact.
	data[len(data)-sstFooterLen-8] ^= 0xff
	p := filepath.Join(t.TempDir(), "000001.sst")
	os.WriteFile(p, data, 0o644)
	if _, err := openSSTable(p, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a damaged index block was accepted: %v", err)
	}
}

func TestSSTableDataBlockChecksumDetectsCorruption(t *testing.T) {
	path, _, r := buildTable(t, smallOptions(), 300)
	r.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[40] ^= 0xff // inside the first data block
	p := filepath.Join(t.TempDir(), "000001.sst")
	os.WriteFile(p, data, 0o644)

	broken, err := openSSTable(p, 1)
	if err != nil {
		t.Fatalf("the footer and index should still be readable: %v", err)
	}
	defer broken.Close()
	if _, err := broken.readBlock(0); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a damaged data block was accepted: %v", err)
	}
}

func TestSSTableEmptyWriterProducesNoIndexEntries(t *testing.T) {
	opts := smallOptions().withDefaults()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := newSSTWriter(path, 1, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !w.Empty() {
		t.Fatal("a fresh writer is not empty")
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish on an empty table: %v", err)
	}
	if meta.Entries != 0 {
		t.Fatalf("empty table claims %d entries", meta.Entries)
	}
	r, err := openSSTable(path, 1)
	if err != nil {
		t.Fatalf("open empty table: %v", err)
	}
	defer r.Close()
	if it := r.NewIter(); it.First() {
		t.Fatal("empty table iterated a value")
	}
}

func TestSSTableAbandonRemovesThePartialFile(t *testing.T) {
	opts := smallOptions().withDefaults()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := newSSTWriter(path, 1, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(makeInternalKey([]byte("a"), 1, kindValue), []byte("v")); err != nil {
		t.Fatal(err)
	}
	w.Abandon()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("abandoned file still exists: %v", err)
	}
}

func TestSSTableHandlesEmptyValuesAndBinaryKeys(t *testing.T) {
	opts := smallOptions().withDefaults()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := newSSTWriter(path, 1, opts)
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{{0x00, 0x01}, {0x00, 0x02}, []byte("\xff\xfe"), []byte("normal")}
	// Insert in sorted order, which is what the writer requires.
	sorted := [][]byte{{0x00, 0x01}, {0x00, 0x02}, []byte("normal"), []byte("\xff\xfe")}
	for i, k := range sorted {
		if err := w.Add(makeInternalKey(k, uint64(i+1), kindValue), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := openSSTable(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, k := range keys {
		v, kind, found, _, err := r.Get(k, maxSequence)
		if err != nil || !found {
			t.Fatalf("Get(%x): found=%v err=%v", k, found, err)
		}
		if kind != kindValue || len(v) != 0 {
			t.Fatalf("Get(%x) = %q/%d, want an empty value", k, v, kind)
		}
	}
}
