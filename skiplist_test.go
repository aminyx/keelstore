package keelstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func slKey(k string, seq uint64) []byte { return makeInternalKey([]byte(k), seq, kindValue) }

func TestSkiplistInsertAndGet(t *testing.T) {
	s := newSkiplist()
	for i := 0; i < 100; i++ {
		s.Insert(slKey(key(i), uint64(i+1)), []byte(value(i)))
	}
	for i := 0; i < 100; i++ {
		v, ok := s.Get(slKey(key(i), uint64(i+1)))
		if !ok {
			t.Fatalf("missing key %d", i)
		}
		if string(v) != value(i) {
			t.Fatalf("key %d = %q, want %q", i, v, value(i))
		}
	}
	if _, ok := s.Get(slKey("absent", 1)); ok {
		t.Fatal("found a key that was never inserted")
	}
}

func TestSkiplistIteratesInOrderRegardlessOfInsertOrder(t *testing.T) {
	s := newSkiplist()
	order := rand.New(rand.NewSource(7)).Perm(500)
	for _, i := range order {
		s.Insert(slKey(key(i), uint64(i+1)), []byte(value(i)))
	}

	it := s.NewIter()
	n := 0
	var prev []byte
	for ok := it.First(); ok; ok = it.Next() {
		if prev != nil && compareInternalKeys(prev, it.Key()) >= 0 {
			t.Fatalf("out of order at %d: %q then %q", n, prev, it.Key())
		}
		prev = append([]byte(nil), it.Key()...)
		n++
	}
	if n != 500 {
		t.Fatalf("iterated %d entries, want 500", n)
	}
}

func TestSkiplistOrdersNewestSequenceFirst(t *testing.T) {
	s := newSkiplist()
	// Insert oldest first so that ordering, not insertion order, decides.
	for seq := uint64(1); seq <= 5; seq++ {
		s.Insert(slKey("k", seq), []byte(fmt.Sprintf("v%d", seq)))
	}
	it := s.NewIter()
	var seqs []uint64
	for ok := it.First(); ok; ok = it.Next() {
		seqs = append(seqs, seqOf(it.Key()))
	}
	want := []uint64{5, 4, 3, 2, 1}
	if fmt.Sprint(seqs) != fmt.Sprint(want) {
		t.Fatalf("sequence order = %v, want %v", seqs, want)
	}
}

func TestSkiplistDuplicateKeysKeepNewestFirst(t *testing.T) {
	s := newSkiplist()
	k := slKey("dup", 9)
	s.Insert(k, []byte("first"))
	s.Insert(k, []byte("second"))

	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 (duplicates are kept, not replaced)", got)
	}
	v, ok := s.Get(k)
	if !ok || string(v) != "second" {
		t.Fatalf("Get = %q/%v, want the most recent insertion", v, ok)
	}
}

func TestSkiplistSeekLandsOnFirstGreaterOrEqual(t *testing.T) {
	s := newSkiplist()
	for i := 0; i < 50; i += 2 {
		s.Insert(slKey(key(i), 1), []byte(value(i)))
	}
	it := s.NewIter()

	if !it.Seek(slKey(key(11), 1)) {
		t.Fatal("seek past a gap should land on the next key")
	}
	if got := string(userKeyOf(it.Key())); got != key(12) {
		t.Fatalf("Seek(key 11) landed on %q, want %q", got, key(12))
	}
	if it.Seek(slKey("zzzz", 1)) {
		t.Fatal("seek past the end should not be valid")
	}
}

func TestSkiplistSizeAccountingGrows(t *testing.T) {
	s := newSkiplist()
	if s.Bytes() != 0 {
		t.Fatalf("fresh list reports %d bytes", s.Bytes())
	}
	s.Insert(slKey("a", 1), bytes.Repeat([]byte("x"), 100))
	first := s.Bytes()
	if first < 100 {
		t.Fatalf("size %d does not account for a 100 byte value", first)
	}
	s.Insert(slKey("b", 2), bytes.Repeat([]byte("x"), 100))
	if s.Bytes() <= first {
		t.Fatalf("size did not grow: %d then %d", first, s.Bytes())
	}
}

func TestSkiplistConcurrentReadsDuringWrites(t *testing.T) {
	const n = 3000
	s := newSkiplist()
	// Seed a prefix so readers have something to find from the start.
	for i := 0; i < 100; i++ {
		s.Insert(slKey(key(i), uint64(i+1)), []byte(value(i)))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < 100; i++ {
					v, ok := s.Get(slKey(key(i), uint64(i+1)))
					if !ok || string(v) != value(i) {
						panic("reader saw a torn skiplist")
					}
				}
				it := s.NewIter()
				var prev []byte
				for ok := it.First(); ok; ok = it.Next() {
					if prev != nil && compareInternalKeys(prev, it.Key()) > 0 {
						panic("reader saw an out-of-order skiplist")
					}
					prev = append(prev[:0], it.Key()...)
				}
			}
		}()
	}

	for i := 100; i < n; i++ {
		s.Insert(slKey(key(i), uint64(i+1)), []byte(value(i)))
	}
	close(stop)
	wg.Wait()

	if got := s.Len(); got != n {
		t.Fatalf("Len = %d, want %d", got, n)
	}
}

func TestSkiplistRandomHeightStaysInRange(t *testing.T) {
	s := newSkiplist()
	seen := map[int]bool{}
	for i := 0; i < 20000; i++ {
		h := s.randomHeight()
		if h < 1 || h > slMaxHeight {
			t.Fatalf("height %d out of range", h)
		}
		seen[h] = true
	}
	if len(seen) < 3 {
		t.Fatalf("heights are not varying: %v", seen)
	}
}

func TestSkiplistIteratorOnEmptyList(t *testing.T) {
	s := newSkiplist()
	it := s.NewIter()
	if it.First() {
		t.Fatal("First on an empty list reported a value")
	}
	if it.Valid() || it.Key() != nil || it.Value() != nil {
		t.Fatal("empty iterator is not in a clean state")
	}
	if it.Next() {
		t.Fatal("Next on an empty list reported a value")
	}
}
