package keelstore

import (
	"fmt"
	"testing"
)

func TestIteratorWalksEveryKeyInOrder(t *testing.T) {
	db := openTestDB(t, smallOptions())
	const n = 1000
	for i := n - 1; i >= 0; i-- { // insert backwards; order must come from the tree
		mustPut(t, db, key(i), value(i))
	}

	it := db.NewIterator(nil)
	defer it.Close()
	keys, values := collect(t, it)

	if len(keys) != n {
		t.Fatalf("iterated %d keys, want %d", len(keys), n)
	}
	for i := range keys {
		if keys[i] != key(i) || values[i] != value(i) {
			t.Fatalf("entry %d = %q/%q", i, keys[i], values[i])
		}
	}
}

func TestIteratorRespectsRangeBounds(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 500; i++ {
		mustPut(t, db, key(i), value(i))
	}

	cases := []struct {
		name      string
		opts      IterOptions
		wantFirst string
		wantLast  string
		wantCount int
	}{
		{"unbounded", IterOptions{}, key(0), key(499), 500},
		{"lower bound only", IterOptions{Start: []byte(key(100))}, key(100), key(499), 400},
		{"upper bound only", IterOptions{End: []byte(key(100))}, key(0), key(99), 100},
		{"both bounds", IterOptions{Start: []byte(key(100)), End: []byte(key(200))}, key(100), key(199), 100},
		{"empty range", IterOptions{Start: []byte(key(200)), End: []byte(key(200))}, "", "", 0},
		{"bounds outside the data", IterOptions{Start: []byte("zzz")}, "", "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := c.opts
			it := db.NewIterator(&opts)
			defer it.Close()
			keys, _ := collect(t, it)
			if len(keys) != c.wantCount {
				t.Fatalf("got %d keys, want %d", len(keys), c.wantCount)
			}
			if c.wantCount == 0 {
				return
			}
			if keys[0] != c.wantFirst || keys[len(keys)-1] != c.wantLast {
				t.Fatalf("range is %q..%q, want %q..%q", keys[0], keys[len(keys)-1], c.wantFirst, c.wantLast)
			}
		})
	}
}

func TestIteratorStartBoundIsInclusiveAndEndExclusive(t *testing.T) {
	db := openTestDB(t, Options{})
	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, k, k)
	}
	it := db.NewIterator(&IterOptions{Start: []byte("b"), End: []byte("d")})
	defer it.Close()
	keys, _ := collect(t, it)
	if fmt.Sprint(keys) != "[b c]" {
		t.Fatalf("got %v, want [b c]", keys)
	}
}

func TestIteratorHidesTombstones(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 300; i++ {
		mustPut(t, db, key(i), value(i))
	}
	for i := 0; i < 300; i += 2 {
		mustDelete(t, db, key(i))
	}

	it := db.NewIterator(nil)
	defer it.Close()
	keys, _ := collect(t, it)

	if len(keys) != 150 {
		t.Fatalf("iterated %d keys, want the 150 that survive", len(keys))
	}
	for _, k := range keys {
		var i int
		fmt.Sscanf(k, "key-%06d", &i)
		if i%2 == 0 {
			t.Fatalf("deleted key %q was returned", k)
		}
	}
}

func TestIteratorShowsOnlyTheNewestVersionOfAKey(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for round := 0; round < 20; round++ {
		for i := 0; i < 50; i++ {
			mustPut(t, db, key(i), fmt.Sprintf("round-%d", round))
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	it := db.NewIterator(nil)
	defer it.Close()
	keys, values := collect(t, it)
	if len(keys) != 50 {
		t.Fatalf("iterated %d keys, want 50 distinct ones", len(keys))
	}
	for i, v := range values {
		if v != "round-19" {
			t.Fatalf("key %q shows %q, want the newest write", keys[i], v)
		}
	}
}

// The interesting case: the same key lives in the memtable, in a frozen
// memtable and in several SSTables at once, and the merge has to pick the
// right one.
func TestIteratorMergesAcrossMemtablesAndLevels(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)

	for i := 0; i < 400; i++ {
		mustPut(t, db, key(i), "old")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i += 2 {
		mustPut(t, db, key(i), "middle")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i += 4 {
		mustPut(t, db, key(i), "new")
	}

	s := db.Stats()
	if s.SSTables < 2 {
		t.Fatalf("only %d tables: the merge is not being exercised", s.SSTables)
	}

	it := db.NewIterator(nil)
	defer it.Close()
	keys, values := collect(t, it)
	if len(keys) != 400 {
		t.Fatalf("iterated %d keys, want 400", len(keys))
	}
	for i := range keys {
		want := "old"
		switch {
		case i%4 == 0:
			want = "new"
		case i%2 == 0:
			want = "middle"
		}
		if values[i] != want {
			t.Fatalf("key %d = %q, want %q", i, values[i], want)
		}
	}
}

func TestIteratorIsUnaffectedByConcurrentWrites(t *testing.T) {
	db := openTestDB(t, smallOptions())
	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), "before")
	}

	it := db.NewIterator(nil)
	defer it.Close()

	if !it.Next() {
		t.Fatal("iterator started empty")
	}
	// Rewrite everything while the iterator is halfway through.
	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), "after")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	seen := 1
	if string(it.Value()) != "before" {
		t.Fatalf("first value = %q, want \"before\"", it.Value())
	}
	for it.Next() {
		if string(it.Value()) != "before" {
			t.Fatalf("key %q shows %q: a later write leaked into the iteration", it.Key(), it.Value())
		}
		seen++
	}
	if seen != 200 {
		t.Fatalf("saw %d keys, want 200", seen)
	}
}

func TestIteratorOnAnEmptyDatabase(t *testing.T) {
	db := openTestDB(t, Options{})
	it := db.NewIterator(nil)
	defer it.Close()
	if it.Next() {
		t.Fatal("an empty database iterated a key")
	}
	if it.Error() != nil {
		t.Fatalf("iterator error: %v", it.Error())
	}
	if it.Key() != nil || it.Value() != nil {
		t.Fatal("an exhausted iterator should hold nothing")
	}
}

func TestIteratorCloseIsIdempotent(t *testing.T) {
	db := openTestDB(t, smallOptions())
	mustPut(t, db, "k", "v")
	it := db.NewIterator(nil)
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if it.Next() {
		t.Fatal("a closed iterator produced a key")
	}
}

func TestIteratorPinsTablesUntilClosed(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)
	for i := 0; i < 800; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	it := db.NewIterator(nil)
	// Compacting now makes every table the iterator holds obsolete. The
	// files must stay on disk until the iterator lets go of them.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	keys, _ := collect(t, it)
	if len(keys) != 800 {
		t.Fatalf("iterated %d keys through a compaction, want 800", len(keys))
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 800; i++ {
		mustGet(t, db, key(i), value(i))
	}
}

func TestMergeIterProducesASingleSortedStream(t *testing.T) {
	// Three overlapping sources, merged.
	lists := make([]*skiplist, 3)
	for i := range lists {
		lists[i] = newSkiplist()
	}
	for i := 0; i < 90; i++ {
		lists[i%3].Insert(makeInternalKey([]byte(key(i)), uint64(i+1), kindValue), []byte(value(i)))
	}
	sources := []internalIterator{lists[0].NewIter(), lists[1].NewIter(), lists[2].NewIter()}
	m := newMergeIter(sources)
	defer m.Close()

	n := 0
	var prev []byte
	for ok := m.First(); ok; ok = m.Next() {
		if prev != nil && compareInternalKeys(prev, m.Key()) >= 0 {
			t.Fatalf("merge produced %q after %q", m.Key(), prev)
		}
		prev = append(prev[:0], m.Key()...)
		n++
	}
	if n != 90 {
		t.Fatalf("merged %d entries, want 90", n)
	}
	if err := m.Error(); err != nil {
		t.Fatalf("merge error: %v", err)
	}
}

func TestMergeIterSeekPositionsEverySource(t *testing.T) {
	a, b := newSkiplist(), newSkiplist()
	for i := 0; i < 100; i += 2 {
		a.Insert(makeInternalKey([]byte(key(i)), 1, kindValue), []byte("a"))
	}
	for i := 1; i < 100; i += 2 {
		b.Insert(makeInternalKey([]byte(key(i)), 1, kindValue), []byte("b"))
	}
	m := newMergeIter([]internalIterator{a.NewIter(), b.NewIter()})
	defer m.Close()

	if !m.Seek(firstKeyFor([]byte(key(50)))) {
		t.Fatal("seek found nothing")
	}
	if got := string(userKeyOf(m.Key())); got != key(50) {
		t.Fatalf("seek landed on %q, want %q", got, key(50))
	}
	n := 1
	for m.Next() {
		n++
	}
	if n != 50 {
		t.Fatalf("%d entries from key 50 onwards, want 50", n)
	}
}

func TestMergeIterWithNoSources(t *testing.T) {
	m := newMergeIter(nil)
	defer m.Close()
	if m.First() || m.Next() {
		t.Fatal("a merge over nothing produced a key")
	}
	if m.Key() != nil || m.Value() != nil || m.Error() != nil {
		t.Fatal("empty merge is not in a clean state")
	}
}
