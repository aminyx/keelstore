package keelstore

import (
	"context"
	"os"
	"testing"
	"time"
)

// countEntriesOnDisk walks every SSTable and counts the raw internal keys,
// including versions and tombstones that a reader would never see. It is
// how these tests observe whether compaction actually threw anything away.
func countEntriesOnDisk(t *testing.T, db *DB) (entries int, tombstones int) {
	t.Helper()
	v := db.acquireVersion()
	defer db.releaseVersion(v)
	for _, level := range v.levels {
		for _, meta := range level {
			r, err := db.cache.get(meta.Number)
			if err != nil {
				t.Fatalf("opening table %d: %v", meta.Number, err)
			}
			it := r.NewIter()
			for ok := it.First(); ok; ok = it.Next() {
				entries++
				if kindOf(it.Key()) == kindDelete {
					tombstones++
				}
			}
			it.Close()
		}
	}
	return entries, tombstones
}

func TestCompactionDropsShadowedVersions(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	// Ten versions of every key. Nine of them are dead the moment the
	// tenth is written.
	for round := 0; round < 10; round++ {
		for i := 0; i < 100; i++ {
			mustPut(t, db, key(i), value(round))
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	before, _ := countEntriesOnDisk(t, db)
	if before < 1000 {
		t.Fatalf("only %d entries on disk before compaction, expected 1000", before)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after, _ := countEntriesOnDisk(t, db)
	if after != 100 {
		t.Fatalf("%d entries survived compaction, want exactly the 100 live keys", after)
	}
	for i := 0; i < 100; i++ {
		mustGet(t, db, key(i), value(9))
	}
}

func TestCompactionDropsTombstonesAtTheBottom(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		mustDelete(t, db, key(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	_, tombstonesBefore := countEntriesOnDisk(t, db)
	if tombstonesBefore != 200 {
		t.Fatalf("%d tombstones on disk, want 200", tombstonesBefore)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	entries, tombstones := countEntriesOnDisk(t, db)
	if tombstones != 0 {
		t.Fatalf("%d tombstones survived a full compaction", tombstones)
	}
	if entries != 0 {
		t.Fatalf("%d entries survived after every key was deleted", entries)
	}
	for i := 0; i < 200; i++ {
		mustMissing(t, db, key(i))
	}
}

// A tombstone may only be dropped when nothing older can be hiding beneath
// it. With a snapshot pinned to a sequence before the delete, the deleted
// value must still be on disk after compaction.
func TestCompactionKeepsTombstonesASnapshotStillNeeds(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), value(i))
	}
	snap := db.Snapshot()
	defer snap.Release()

	for i := 0; i < 200; i++ {
		mustDelete(t, db, key(i))
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	entries, tombstones := countEntriesOnDisk(t, db)
	if tombstones == 0 {
		t.Fatal("compaction dropped tombstones that a live snapshot still needs")
	}
	if entries < 400 {
		t.Fatalf("only %d entries survived; the snapshot's values were discarded", entries)
	}
	for i := 0; i < 200; i++ {
		if _, err := snap.Get([]byte(key(i))); err != nil {
			t.Fatalf("snapshot lost key %d: %v", i, err)
		}
		mustMissing(t, db, key(i))
	}

	// Once the snapshot goes away the same compaction is free to clean up.
	snap.Release()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if entries, tombstones := countEntriesOnDisk(t, db); entries != 0 || tombstones != 0 {
		t.Fatalf("after releasing the snapshot, %d entries / %d tombstones remain", entries, tombstones)
	}
}

func TestCompactionKeepsValuesASnapshotStillNeeds(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 100; i++ {
		mustPut(t, db, key(i), "v1")
	}
	snap := db.Snapshot()
	defer snap.Release()
	for i := 0; i < 100; i++ {
		mustPut(t, db, key(i), "v2")
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	if entries, _ := countEntriesOnDisk(t, db); entries != 200 {
		t.Fatalf("%d entries after compaction, want both versions of 100 keys", entries)
	}
	for i := 0; i < 100; i++ {
		got, err := snap.Get([]byte(key(i)))
		if err != nil || string(got) != "v1" {
			t.Fatalf("snapshot key %d = %q/%v, want v1", i, got, err)
		}
	}
}

func TestBackgroundCompactionRunsOnItsOwn(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)

	for i := 0; i < 4000; i++ {
		mustPut(t, db, key(i), value(i))
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if db.Stats().Compactions > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := db.Stats()
	if s.Compactions == 0 {
		t.Fatalf("no background compaction ran: %+v", s.SSTablesPerLevel)
	}
	if s.Flushes == 0 {
		t.Fatal("no background flush ran")
	}
	for i := 0; i < 4000; i++ {
		mustGet(t, db, key(i), value(i))
	}
}

func TestCompactionMovesFilesOutOfLevelZero(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 1200; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	if before.SSTablesPerLevel[0] < 2 {
		t.Fatalf("expected several level-0 files, got %v", before.SSTablesPerLevel)
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after := db.Stats()
	if after.SSTablesPerLevel[0] != 0 {
		t.Fatalf("level 0 still holds %d files after a full compaction", after.SSTablesPerLevel[0])
	}
	if after.SSTables == 0 {
		t.Fatal("compaction produced no output files")
	}
	t.Logf("levels before %v, after %v", before.SSTablesPerLevel, after.SSTablesPerLevel)
}

func TestCompactionKeepsDeeperLevelsDisjoint(t *testing.T) {
	opts := smallOptions()
	opts.TargetFileSize = 2 << 10 // force several output files
	db := openTestDB(t, opts)

	for i := 0; i < 3000; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	v := db.acquireVersion()
	defer db.releaseVersion(v)
	for level := 1; level < len(v.levels); level++ {
		files := v.levels[level]
		for i := 1; i < len(files); i++ {
			prev, cur := files[i-1], files[i]
			if string(cur.smallestUser()) <= string(prev.largestUser()) {
				t.Fatalf("level %d files %d and %d overlap: %q..%q then %q..%q",
					level, prev.Number, cur.Number,
					prev.smallestUser(), prev.largestUser(),
					cur.smallestUser(), cur.largestUser())
			}
		}
	}
}

func TestCompactionDeletesTheFilesItConsumed(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 1000; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	v := db.acquireVersion()
	consumed := make([]uint64, 0, len(v.levels[0]))
	for _, meta := range v.levels[0] {
		consumed = append(consumed, meta.Number)
	}
	db.releaseVersion(v)
	if len(consumed) == 0 {
		t.Fatal("nothing was flushed")
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, n := range consumed {
		if _, err := os.Stat(sstPath(db.Path(), n)); !os.IsNotExist(err) {
			t.Fatalf("input table %d was not deleted: %v", n, err)
		}
	}
}

func TestIterationIsCorrectDuringCompaction(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)
	const n = 2000
	for i := 0; i < n; i++ {
		mustPut(t, db, key(i), value(i))
	}

	// Start iterating, then compact underneath, then finish iterating.
	it := db.NewIterator(nil)
	defer it.Close()
	seen := 0
	for it.Next() {
		seen++
		if seen == 10 {
			if err := db.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
		}
		if got, want := string(it.Key()), key(seen-1); got != want {
			t.Fatalf("entry %d = %q, want %q", seen, got, want)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != n {
		t.Fatalf("iterated %d keys through a compaction, want %d", seen, n)
	}
}

func TestCompactionIsCancellable(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 4000; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	before := db.Stats()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the very first check must abort

	db.bgMu.Lock()
	v := db.acquireVersion()
	job := db.pickCompaction(v)
	if job == nil {
		db.releaseVersion(v)
		db.bgMu.Unlock()
		t.Fatal("no compaction to cancel")
	}
	err := db.runCompaction(ctx, v, job)
	db.releaseVersion(v)
	db.bgMu.Unlock()

	if err == nil {
		t.Fatal("a cancelled compaction reported success")
	}
	after := db.Stats()
	if after.Compactions != before.Compactions {
		t.Fatal("a cancelled compaction was counted as completed")
	}
	if after.CompactionsCancelled == 0 {
		t.Fatal("the cancellation was not recorded")
	}
	// The tree is untouched and every key is still readable.
	if after.SSTablesPerLevel[0] != before.SSTablesPerLevel[0] {
		t.Fatalf("level 0 changed from %d to %d files", before.SSTablesPerLevel[0], after.SSTablesPerLevel[0])
	}
	for i := 0; i < 4000; i += 97 {
		mustGet(t, db, key(i), value(i))
	}
}

func TestCloseStopsBackgroundWork(t *testing.T) {
	opts := smallOptions()
	dir := t.TempDir()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3000; i++ {
		mustPut(t, db, key(i), value(i))
	}

	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return: background work was not cancelled")
	}

	// Everything must still be there on the next Open.
	db2, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 3000; i += 13 {
		mustGet(t, db2, key(i), value(i))
	}
}

func TestPickCompactionPrefersLevelZero(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)

	for i := 0; i < 1000; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	v := db.acquireVersion()
	defer db.releaseVersion(v)

	job := db.pickCompaction(v)
	if job == nil {
		t.Fatal("no compaction picked with a full level 0")
	}
	if job.level != 0 || job.outputLevel != 1 {
		t.Fatalf("picked L%d -> L%d, want L0 -> L1", job.level, job.outputLevel)
	}
	if len(job.inputs[0]) != len(v.levels[0]) {
		t.Fatalf("a level-0 compaction took %d of %d files; it must take them all", len(job.inputs[0]), len(v.levels[0]))
	}
}

func TestPickCompactionReturnsNilWhenTheTreeIsSmall(t *testing.T) {
	opts := smallOptions()
	opts.disableAutoCompaction = true
	db := openTestDB(t, opts)
	mustPut(t, db, "only", "one")
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	v := db.acquireVersion()
	defer db.releaseVersion(v)
	if job := db.pickCompaction(v); job != nil {
		t.Fatalf("picked a compaction for a one-file tree: %+v", job)
	}
}
