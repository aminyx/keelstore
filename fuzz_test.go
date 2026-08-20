package keelstore

import (
	"bytes"
	"errors"
	"testing"
)

// FuzzPutGet throws arbitrary bytes at the write path and demands that a
// key written is a key readable, and a key deleted is a key gone. Keys and
// values in keelstore are opaque byte slices, so the fuzzer is free to
// produce embedded NULs, invalid UTF-8, very long keys and empty values.
//
// Run it with:
//
//	go test -run=xxx -fuzz=FuzzPutGet -fuzztime=60s
func FuzzPutGet(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte("k"), []byte(""))
	f.Add([]byte{0x00}, []byte{0xff, 0x00, 0xfe})
	f.Add([]byte("a very long key that spans more than one block boundary in a tiny store"), []byte("v"))
	f.Add([]byte("\xf0\x9f\x94\x91"), []byte("\xff\xfe\xfd"))

	f.Fuzz(func(t *testing.T, k, v []byte) {
		if len(k) == 0 || len(k) > maxKeyLen {
			t.Skip()
		}
		opts := smallOptions()
		dir := t.TempDir()
		db, err := Open(dir, &opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) after Put: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("Get(%q) = %q, want %q", k, got, v)
		}

		// The same key must survive a flush unchanged.
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		got, err = db.Get(k)
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("after Flush, Get(%q) = %q/%v", k, got, err)
		}

		// And it must be visible to an iterator, exactly once.
		it := db.NewIterator(nil)
		count := 0
		for it.Next() {
			if !bytes.Equal(it.Key(), k) {
				t.Fatalf("iterator produced %q, want %q", it.Key(), k)
			}
			if !bytes.Equal(it.Value(), v) {
				t.Fatalf("iterator value = %q, want %q", it.Value(), v)
			}
			count++
		}
		iterErr := it.Error()
		it.Close()
		if iterErr != nil {
			t.Fatalf("iterator: %v", iterErr)
		}
		if count != 1 {
			t.Fatalf("iterator produced %d entries for one key", count)
		}

		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete(%q): %v", k, err)
		}
		if _, err := db.Get(k); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) after Delete = %v, want ErrNotFound", k, err)
		}
	})
}

// FuzzInternalKeyOrder checks the comparator that everything else depends
// on: it must be a total order, and it must be consistent with the way
// user keys and sequence numbers are supposed to sort.
func FuzzInternalKeyOrder(f *testing.F) {
	f.Add([]byte("a"), uint64(1), []byte("b"), uint64(2))
	f.Add([]byte(""), uint64(0), []byte(""), uint64(0))
	f.Add([]byte("same"), uint64(9), []byte("same"), uint64(3))

	f.Fuzz(func(t *testing.T, ka []byte, sa uint64, kb []byte, sb uint64) {
		sa, sb = sa%(maxSequence+1), sb%(maxSequence+1)
		a := makeInternalKey(ka, sa, kindValue)
		b := makeInternalKey(kb, sb, kindValue)

		ab := compareInternalKeys(a, b)
		ba := compareInternalKeys(b, a)
		if ab != -ba {
			t.Fatalf("comparator is not antisymmetric: %d and %d", ab, ba)
		}
		if compareInternalKeys(a, a) != 0 {
			t.Fatal("a key does not equal itself")
		}

		switch c := bytes.Compare(ka, kb); {
		case c != 0 && ab != c:
			t.Fatalf("user keys compare %d but internal keys compare %d", c, ab)
		case c == 0 && sa > sb && ab >= 0:
			t.Fatal("the newer sequence number did not sort first")
		case c == 0 && sa == sb && ab != 0:
			t.Fatal("identical keys did not compare equal")
		}
	})
}
