package keelstore

import (
	"errors"
	"testing"
)

func TestSnapshotHidesLaterWrites(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 100; i++ {
		mustPut(t, db, key(i), "v1")
	}

	snap := db.Snapshot()
	defer snap.Release()

	for i := 0; i < 100; i++ {
		mustPut(t, db, key(i), "v2")
	}
	mustPut(t, db, "brand-new", "v2")

	for i := 0; i < 100; i++ {
		got, err := snap.Get([]byte(key(i)))
		if err != nil {
			t.Fatalf("snapshot Get(%d): %v", i, err)
		}
		if string(got) != "v1" {
			t.Fatalf("snapshot Get(%d) = %q, want v1", i, got)
		}
	}
	if _, err := snap.Get([]byte("brand-new")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot saw a key created after it: %v", err)
	}
	// The live database of course sees the new values.
	mustGet(t, db, key(0), "v2")
	mustGet(t, db, "brand-new", "v2")
}

func TestSnapshotHidesLaterDeletes(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 100; i++ {
		mustPut(t, db, key(i), value(i))
	}
	snap := db.Snapshot()
	defer snap.Release()

	for i := 0; i < 100; i++ {
		mustDelete(t, db, key(i))
	}
	for i := 0; i < 100; i++ {
		got, err := snap.Get([]byte(key(i)))
		if err != nil {
			t.Fatalf("snapshot lost key %d after it was deleted: %v", i, err)
		}
		if string(got) != value(i) {
			t.Fatalf("snapshot Get(%d) = %q", i, got)
		}
		mustMissing(t, db, key(i))
	}
}

func TestSnapshotSurvivesFlushAndCompaction(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 600; i++ {
		mustPut(t, db, key(i), "v1")
	}
	snap := db.Snapshot()
	defer snap.Release()

	for i := 0; i < 600; i++ {
		mustPut(t, db, key(i), "v2")
	}
	for i := 0; i < 600; i += 2 {
		mustDelete(t, db, key(i))
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The compaction ran with the snapshot held, so it was not allowed to
	// throw away anything the snapshot can see.
	for i := 0; i < 600; i++ {
		got, err := snap.Get([]byte(key(i)))
		if err != nil {
			t.Fatalf("compaction destroyed snapshot data for key %d: %v", i, err)
		}
		if string(got) != "v1" {
			t.Fatalf("snapshot Get(%d) = %q, want v1", i, got)
		}
	}
}

func TestSnapshotIteratorSeesTheSnapshotView(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), "v1")
	}
	snap := db.Snapshot()
	defer snap.Release()

	for i := 0; i < 200; i += 2 {
		mustDelete(t, db, key(i))
	}
	for i := 1000; i < 1100; i++ {
		mustPut(t, db, key(i), "later")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	it := snap.NewIterator(nil)
	defer it.Close()
	keys, values := collect(t, it)
	if len(keys) != 200 {
		t.Fatalf("snapshot iterator saw %d keys, want 200", len(keys))
	}
	for i := range values {
		if values[i] != "v1" {
			t.Fatalf("key %q = %q, want v1", keys[i], values[i])
		}
	}
}

func TestSnapshotSequenceIsPinned(t *testing.T) {
	db := openTestDB(t, Options{})
	mustPut(t, db, "a", "1")
	snap := db.Snapshot()
	defer snap.Release()

	pinned := snap.Sequence()
	for i := 0; i < 10; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if snap.Sequence() != pinned {
		t.Fatalf("snapshot sequence moved from %d to %d", pinned, snap.Sequence())
	}
	if db.Stats().LastSequence <= pinned {
		t.Fatal("the database sequence did not advance")
	}
}

func TestReleasedSnapshotIsRefused(t *testing.T) {
	db := openTestDB(t, Options{})
	mustPut(t, db, "a", "1")
	snap := db.Snapshot()
	snap.Release()
	snap.Release() // releasing twice must be harmless

	if _, err := snap.Get([]byte("a")); !errors.Is(err, ErrSnapshotReleased) {
		t.Fatalf("Get on a released snapshot = %v", err)
	}
	it := snap.NewIterator(nil)
	defer it.Close()
	if it.Next() || !errors.Is(it.Error(), ErrSnapshotReleased) {
		t.Fatalf("iterator on a released snapshot = %v", it.Error())
	}
}

func TestActiveSnapshotsAreCounted(t *testing.T) {
	db := openTestDB(t, Options{})
	mustPut(t, db, "a", "1")
	if got := db.Stats().ActiveSnapshots; got != 0 {
		t.Fatalf("ActiveSnapshots = %d before any snapshot", got)
	}

	a, b := db.Snapshot(), db.Snapshot()
	if got := db.Stats().ActiveSnapshots; got != 1 {
		// Both snapshots pin the same sequence number, so they share one
		// entry in the snapshot set.
		t.Fatalf("ActiveSnapshots = %d, want 1 shared entry", got)
	}
	mustPut(t, db, "b", "2")
	c := db.Snapshot()
	if got := db.Stats().ActiveSnapshots; got != 2 {
		t.Fatalf("ActiveSnapshots = %d, want 2 distinct sequences", got)
	}

	a.Release()
	b.Release()
	c.Release()
	if got := db.Stats().ActiveSnapshots; got != 0 {
		t.Fatalf("ActiveSnapshots = %d after releasing everything", got)
	}
}

func TestOldestSnapshotSequenceTracksTheEarliestReader(t *testing.T) {
	db := openTestDB(t, Options{})
	for i := 0; i < 10; i++ {
		mustPut(t, db, key(i), value(i))
	}
	early := db.Snapshot()
	earlySeq := early.Sequence()

	for i := 10; i < 20; i++ {
		mustPut(t, db, key(i), value(i))
	}
	late := db.Snapshot()

	if got := db.oldestSnapshotSeq(); got != earlySeq {
		t.Fatalf("oldest snapshot = %d, want %d", got, earlySeq)
	}
	early.Release()
	if got := db.oldestSnapshotSeq(); got != late.Sequence() {
		t.Fatalf("after releasing the early snapshot, oldest = %d, want %d", got, late.Sequence())
	}
	late.Release()
	if got, want := db.oldestSnapshotSeq(), db.Stats().LastSequence; got != want {
		t.Fatalf("with no snapshots, oldest = %d, want the latest sequence %d", got, want)
	}
}
