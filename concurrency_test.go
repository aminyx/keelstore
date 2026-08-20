package keelstore

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// keelstore's contract is one writer and any number of readers. These tests
// exist mostly to be run under -race, where they cover the memtable
// handover, the version refcounting and the table cache all at once.

func TestConcurrentReadersDuringWrites(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)

	const seeded = 300
	for i := 0; i < seeded; i++ {
		mustPut(t, db, key(i), value(i))
	}

	var (
		wg    sync.WaitGroup
		stop  = make(chan struct{})
		reads atomic.Int64
		fail  atomic.Value
	)
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < seeded; i++ {
					got, err := db.Get([]byte(key(i)))
					if err != nil {
						fail.Store(fmt.Sprintf("Get(%d): %v", i, err))
						return
					}
					if string(got) != value(i) {
						fail.Store(fmt.Sprintf("Get(%d) = %q, want %q", i, got, value(i)))
						return
					}
					reads.Add(1)
				}
			}
		}()
	}

	// The single writer only ever appends new keys, so the seeded ones the
	// readers check must never change.
	for i := seeded; i < seeded+2000; i++ {
		mustPut(t, db, key(i), value(i))
	}
	close(stop)
	wg.Wait()

	if msg, ok := fail.Load().(string); ok {
		t.Fatal(msg)
	}
	if reads.Load() == 0 {
		t.Fatal("the readers never ran")
	}
	t.Logf("%d concurrent reads alongside 2000 writes", reads.Load())
}

func TestConcurrentIteratorsDuringWritesAndCompaction(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)
	const seeded = 400
	for i := 0; i < seeded; i++ {
		mustPut(t, db, key(i), value(i))
	}

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		fail atomic.Value
	)
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
				it := db.NewIterator(&IterOptions{End: []byte(key(seeded))})
				n := 0
				for it.Next() {
					if string(it.Value()) != value(n) {
						fail.Store(fmt.Sprintf("entry %d = %q", n, it.Value()))
						it.Close()
						return
					}
					n++
				}
				err := it.Error()
				it.Close()
				if err != nil {
					fail.Store(fmt.Sprintf("iterator: %v", err))
					return
				}
				if n != seeded {
					fail.Store(fmt.Sprintf("iterated %d keys, want %d", n, seeded))
					return
				}
			}
		}()
	}

	for i := seeded; i < seeded+1500; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	close(stop)
	wg.Wait()

	if msg, ok := fail.Load().(string); ok {
		t.Fatal(msg)
	}
}

func TestConcurrentSnapshotsAreIndependent(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)
	for i := 0; i < 200; i++ {
		mustPut(t, db, key(i), "v0")
	}

	var wg sync.WaitGroup
	fail := make(chan string, 8)
	for round := 1; round <= 8; round++ {
		snap := db.Snapshot()
		want := fmt.Sprintf("v%d", round-1)
		wg.Add(1)
		go func(s *Snapshot, want string) {
			defer wg.Done()
			defer s.Release()
			for pass := 0; pass < 20; pass++ {
				for i := 0; i < 200; i++ {
					got, err := s.Get([]byte(key(i)))
					if err != nil || string(got) != want {
						fail <- fmt.Sprintf("snapshot pinned at %q saw %q/%v for key %d", want, got, err, i)
						return
					}
				}
			}
		}(snap, want)

		for i := 0; i < 200; i++ {
			mustPut(t, db, key(i), fmt.Sprintf("v%d", round))
		}
	}
	wg.Wait()
	close(fail)
	if msg, ok := <-fail; ok {
		t.Fatal(msg)
	}
}

func TestConcurrentReadsWhileClosing(t *testing.T) {
	opts := smallOptions()
	dir := t.TempDir()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		mustPut(t, db, key(i), value(i))
	}

	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				for i := 0; i < 500; i++ {
					// After Close the only acceptable answers are the
					// right value or ErrClosed. A wrong value or a panic
					// is a bug.
					got, err := db.Get([]byte(key(i)))
					switch {
					case errors.Is(err, ErrClosed):
						return
					case err != nil:
						t.Errorf("Get(%d): %v", i, err)
						return
					case string(got) != value(i):
						t.Errorf("Get(%d) = %q", i, got)
						return
					}
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
}

func TestManyIteratorsDoNotLeakVersions(t *testing.T) {
	opts := smallOptions()
	db := openTestDB(t, opts)
	for i := 0; i < 500; i++ {
		mustPut(t, db, key(i), value(i))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		it := db.NewIterator(nil)
		it.Next()
		if err := it.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	db.vmu.Lock()
	live, pending := len(db.live), len(db.obsolete)
	db.vmu.Unlock()
	if live != 1 {
		t.Fatalf("%d versions are still referenced, want just the current one", live)
	}
	if pending != 0 {
		t.Fatalf("%d obsolete files were never deleted", pending)
	}
}
