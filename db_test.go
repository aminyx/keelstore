package keelstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t, Options{})

	mustPut(t, db, "alpha", "one")
	mustPut(t, db, "beta", "two")
	mustGet(t, db, "alpha", "one")
	mustGet(t, db, "beta", "two")

	mustDelete(t, db, "alpha")
	mustMissing(t, db, "alpha")
	mustGet(t, db, "beta", "two")

	// Writing over a tombstone brings the key back.
	mustPut(t, db, "alpha", "three")
	mustGet(t, db, "alpha", "three")
}

func TestOverwriteKeepsOnlyTheNewestValue(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 200; i++ {
		mustPut(t, db, "hot", value(i))
	}
	mustGet(t, db, "hot", value(199))

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	mustGet(t, db, "hot", value(199))
}

func TestGetMissingKeyReturnsErrNotFound(t *testing.T) {
	db := openTestDB(t, Options{})
	mustMissing(t, db, "never-written")
	if ok, err := db.Has([]byte("never-written")); err != nil || ok {
		t.Fatalf("Has = %v/%v, want false/nil", ok, err)
	}
	mustPut(t, db, "written", "yes")
	if ok, err := db.Has([]byte("written")); err != nil || !ok {
		t.Fatalf("Has = %v/%v, want true/nil", ok, err)
	}
}

func TestEmptyKeyIsRejectedButEmptyValueIsNot(t *testing.T) {
	db := openTestDB(t, Options{})
	if err := db.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put with an empty key = %v, want ErrEmptyKey", err)
	}
	if err := db.Delete([]byte{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete with an empty key = %v, want ErrEmptyKey", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Get with an empty key = %v, want ErrEmptyKey", err)
	}

	mustPut(t, db, "empty", "")
	v, err := db.Get([]byte("empty"))
	if err != nil || len(v) != 0 {
		t.Fatalf("Get = %q/%v, want an empty value stored successfully", v, err)
	}
}

func TestOversizedKeyIsRejected(t *testing.T) {
	db := openTestDB(t, Options{})
	big := make([]byte, maxKeyLen+1)
	for i := range big {
		big[i] = 'k'
	}
	if err := db.Put(big, []byte("v")); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Put with an oversized key = %v, want ErrKeyTooLarge", err)
	}
}

func TestOperationsAfterCloseFail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "k", "v")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := db.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("Put after Close = %v", err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close = %v", err)
	}
	if err := db.Flush(); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush after Close = %v", err)
	}
	it := db.NewIterator(nil)
	defer it.Close()
	if it.Next() || !errors.Is(it.Error(), ErrClosed) {
		t.Errorf("iterator after Close = %v", it.Error())
	}
	if err := db.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
}

func TestReopenRecoversEverythingFromTheLog(t *testing.T) {
	opts := Options{}
	db := openTestDB(t, opts)
	for i := 0; i < 500; i++ {
		mustPut(t, db, key(i), value(i))
	}
	for i := 0; i < 500; i += 3 {
		mustDelete(t, db, key(i))
	}

	db = reopen(t, db, opts)
	for i := 0; i < 500; i++ {
		if i%3 == 0 {
			mustMissing(t, db, key(i))
		} else {
			mustGet(t, db, key(i), value(i))
		}
	}
}

func TestReopenAcrossFlushesAndCompactions(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)
	for i := 0; i < 1500; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	before := db.Stats()
	if before.SSTables == 0 {
		t.Fatal("no SSTables were produced")
	}

	db = reopen(t, db, opts)
	after := db.Stats()
	if after.SSTables != before.SSTables {
		t.Fatalf("reopen sees %d tables, want the %d that were written", after.SSTables, before.SSTables)
	}
	for i := 0; i < 1500; i++ {
		mustGet(t, db, key(i), value(i))
	}
}

func TestFlushMovesDataOutOfMemory(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 50; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	s := db.Stats()
	if s.MemtableEntries != 0 {
		t.Fatalf("memtable still holds %d entries after a flush", s.MemtableEntries)
	}
	if s.SSTablesPerLevel[0] == 0 && s.SSTables == 0 {
		t.Fatal("flush produced no table")
	}
	for i := 0; i < 50; i++ {
		mustGet(t, db, key(i), value(i))
	}
}

func TestFlushOnAnEmptyDatabaseIsANoop(t *testing.T) {
	db := openTestDB(t, smallOptions())
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if s := db.Stats(); s.SSTables != 0 {
		t.Fatalf("an empty flush created %d tables", s.SSTables)
	}
}

func TestManifestIsWrittenAndReadable(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	buf, err := os.ReadFile(filepath.Join(db.Path(), manifestName))
	if err != nil {
		t.Fatalf("reading the manifest: %v", err)
	}
	if !strings.Contains(string(buf), "\"next_file_number\"") {
		t.Fatalf("the manifest does not look like the expected JSON:\n%s", buf)
	}

	next, seq, v, err := readManifest(db.Path(), DefaultMaxLevels)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if next == 0 || seq == 0 || v.numFiles() == 0 {
		t.Fatalf("manifest round trip gave next=%d seq=%d files=%d", next, seq, v.numFiles())
	}
}

func TestCorruptManifestIsRefused(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "k", "v")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open on a broken manifest = %v, want ErrCorrupt", err)
	}
}

func TestOrphanSSTablesAreReclaimedOnOpen(t *testing.T) {
	opts := smallOptions()
	dir := t.TempDir()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "k", "v")
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a compaction that wrote its output and then died before it
	// could swap the manifest.
	orphan := sstPath(dir, 9999)
	if err := os.WriteFile(orphan, []byte("leftover"), 0o644); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dir, &opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("the orphaned table survived: %v", err)
	}
	mustGet(t, db, "k", "v")
}

func TestStatsReportTheShapeOfTheTree(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 400; i++ {
		mustPut(t, db, key(i), value(i))
	}
	for i := 0; i < 50; i++ {
		mustDelete(t, db, key(i))
	}
	for i := 0; i < 100; i++ {
		db.Get([]byte(key(i)))
	}

	s := db.Stats()
	if s.Puts != 400 || s.Deletes != 50 {
		t.Errorf("Puts=%d Deletes=%d, want 400/50", s.Puts, s.Deletes)
	}
	if s.Gets < 100 {
		t.Errorf("Gets=%d, want at least 100", s.Gets)
	}
	if s.LastSequence != 450 {
		t.Errorf("LastSequence=%d, want 450", s.LastSequence)
	}
	if s.WALBytes <= 0 {
		t.Errorf("WALBytes=%d, want a positive count", s.WALBytes)
	}
	if len(s.SSTablesPerLevel) != len(s.BytesPerLevel) {
		t.Errorf("per-level slices disagree: %d vs %d", len(s.SSTablesPerLevel), len(s.BytesPerLevel))
	}
	if out := s.String(); !strings.Contains(out, "keelstore stats") || !strings.Contains(out, "bloom.fp_rate") {
		t.Errorf("Stats.String is missing sections:\n%s", out)
	}
}

func TestStatsObserveBloomFilterBehaviour(t *testing.T) {
	db := openTestDB(t, smallOptions())
	// Only even keys exist. The odd ones sit inside every table's key
	// range, so the range check cannot reject them and the Bloom filter
	// has to do the work.
	for i := 0; i < 4000; i += 2 {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 4000; i += 2 {
		mustMissing(t, db, key(i))
	}

	s := db.Stats()
	if s.BloomChecks == 0 {
		t.Fatal("no Bloom filter was ever consulted")
	}
	if s.BloomSkips == 0 {
		t.Fatal("the filter never rejected an absent key")
	}
	if s.BloomFalsePositiveRate > 0.10 {
		t.Fatalf("observed false positive rate %.4f is far above the 1%% target", s.BloomFalsePositiveRate)
	}
	t.Logf("checks=%d skips=%d false positives=%d rate=%.4f",
		s.BloomChecks, s.BloomSkips, s.BloomFalsePositives, s.BloomFalsePositiveRate)
}

func TestBloomFiltersCanBeDisabled(t *testing.T) {
	opts := smallOptions()
	opts.BloomBitsPerKey = -1
	db := openTestDB(t, opts)
	for i := 0; i < 600; i += 2 {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 600; i += 2 {
		mustMissing(t, db, key(i))
	}
	s := db.Stats()
	if s.BloomChecks == 0 {
		t.Fatal("the tables were never consulted at all")
	}
	if s.BloomSkips != 0 {
		t.Fatalf("filters are disabled but %d lookups were skipped", s.BloomSkips)
	}
	for i := 0; i < 600; i += 2 {
		mustGet(t, db, key(i), value(i))
	}
}

func TestDefaultOptionsAreFilledIn(t *testing.T) {
	o := DefaultOptions()
	if o.MemtableSize != DefaultMemtableSize || o.BlockSize != DefaultBlockSize {
		t.Fatalf("defaults missing: %+v", o)
	}
	if o.BloomBitsPerKey != DefaultBloomBitsPerKey {
		t.Fatalf("BloomBitsPerKey = %d, want %d", o.BloomBitsPerKey, DefaultBloomBitsPerKey)
	}
	if got := o.maxBytesForLevel(1); got != o.LevelBaseSize {
		t.Fatalf("level 1 budget = %d, want %d", got, o.LevelBaseSize)
	}
	if got, want := o.maxBytesForLevel(3), o.LevelBaseSize*100; got != want {
		t.Fatalf("level 3 budget = %d, want %d", got, want)
	}
}

func TestOpenCreatesTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "store")
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		t.Fatalf("Open did not create %s", dir)
	}
	if db.Path() != dir {
		t.Fatalf("Path = %q, want %q", db.Path(), dir)
	}
}

func TestSyncWritesOptionStillRoundTrips(t *testing.T) {
	opts := smallOptions()
	opts.SyncWrites = true
	db := openTestDB(t, opts)
	for i := 0; i < 100; i++ {
		mustPut(t, db, key(i), value(i))
	}
	for i := 0; i < 100; i++ {
		mustGet(t, db, key(i), value(i))
	}
}

func TestLargeValuesSurviveFlushAndCompaction(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)

	big := strings.Repeat("payload", 900) // ~6 KiB, larger than a block
	for i := 0; i < 40; i++ {
		mustPut(t, db, key(i), big+value(i))
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		mustGet(t, db, key(i), big+value(i))
	}
}
