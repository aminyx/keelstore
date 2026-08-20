package keelstore

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// A model-based test: run a long stream of random operations against
// keelstore and against a plain map at the same time, and require them to
// agree at every step. The map is obviously correct, so any disagreement is
// a bug in the engine — and because the operations are random, the test
// reaches interleavings of flush, compaction and tombstone handling that no
// hand-written case would think to try.
type oracle map[string]string

func (o oracle) sortedKeysInRange(start, end string) []string {
	keys := make([]string, 0, len(o))
	for k := range o {
		if start != "" && k < start {
			continue
		}
		if end != "" && k >= end {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func runModelTest(t *testing.T, seed int64, ops int, opts Options) {
	t.Helper()
	rnd := rand.New(rand.NewSource(seed))
	db := openTestDB(t, opts)
	model := oracle{}

	// A small key space guarantees collisions: overwrites, deletes of live
	// keys and deletes of dead ones all happen constantly.
	const keySpace = 400
	randKey := func() string { return fmt.Sprintf("k%04d", rnd.Intn(keySpace)) }

	checkGet := func(k string) {
		got, err := db.Get([]byte(k))
		want, present := model[k]
		switch {
		case present && err != nil:
			t.Fatalf("seed %d: Get(%q) = %v, want %q", seed, k, err, want)
		case present && string(got) != want:
			t.Fatalf("seed %d: Get(%q) = %q, want %q", seed, k, got, want)
		case !present && !errors.Is(err, ErrNotFound):
			t.Fatalf("seed %d: Get(%q) = %q/%v, want ErrNotFound", seed, k, got, err)
		}
	}

	for i := 0; i < ops; i++ {
		switch n := rnd.Intn(1000); {
		case n < 450: // put
			k, v := randKey(), fmt.Sprintf("v%d-%d", i, rnd.Int31())
			mustPut(t, db, k, v)
			model[k] = v

		case n < 600: // delete
			k := randKey()
			mustDelete(t, db, k)
			delete(model, k)

		case n < 880: // point read
			checkGet(randKey())

		case n < 940: // range scan
			a, b := randKey(), randKey()
			if a > b {
				a, b = b, a
			}
			it := db.NewIterator(&IterOptions{Start: []byte(a), End: []byte(b)})
			got, _ := collect(t, it)
			it.Close()
			want := model.sortedKeysInRange(a, b)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("seed %d op %d: scan [%s,%s) returned %d keys, want %d\n got: %v\nwant: %v",
					seed, i, a, b, len(got), len(want), got, want)
			}

		case n < 970: // snapshot isolation over a burst of writes
			snap := db.Snapshot()
			frozen := make(oracle, len(model))
			for k, v := range model {
				frozen[k] = v
			}
			for j := 0; j < 20; j++ {
				k, v := randKey(), fmt.Sprintf("later-%d-%d", i, j)
				mustPut(t, db, k, v)
				model[k] = v
			}
			for j := 0; j < 20; j++ {
				k := randKey()
				got, err := snap.Get([]byte(k))
				want, present := frozen[k]
				if present && (err != nil || string(got) != want) {
					t.Fatalf("seed %d: snapshot Get(%q) = %q/%v, want %q", seed, k, got, err, want)
				}
				if !present && !errors.Is(err, ErrNotFound) {
					t.Fatalf("seed %d: snapshot Get(%q) = %q/%v, want ErrNotFound", seed, k, got, err)
				}
			}
			snap.Release()

		case n < 996: // flush
			if err := db.Flush(); err != nil {
				t.Fatalf("seed %d: Flush: %v", seed, err)
			}

		default: // compact
			if err := db.Compact(); err != nil {
				t.Fatalf("seed %d: Compact: %v", seed, err)
			}
		}
	}

	// Final agreement: every key, and a full scan.
	for k := range model {
		checkGet(k)
	}
	for i := 0; i < keySpace; i++ {
		checkGet(fmt.Sprintf("k%04d", i))
	}

	it := db.NewIterator(nil)
	defer it.Close()
	gotKeys, gotValues := collect(t, it)
	wantKeys := model.sortedKeysInRange("", "")
	if fmt.Sprint(gotKeys) != fmt.Sprint(wantKeys) {
		t.Fatalf("seed %d: full scan returned %d keys, want %d", seed, len(gotKeys), len(wantKeys))
	}
	for i, k := range gotKeys {
		if gotValues[i] != model[k] {
			t.Fatalf("seed %d: scan value for %q = %q, want %q", seed, k, gotValues[i], model[k])
		}
	}
	t.Logf("seed %d: %d ops, %d live keys, stats %v", seed, ops, len(model), db.Stats().SSTablesPerLevel)
}

func TestModelAgreesWithAMapOracle(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runModelTest(t, seed, 4000, smallOptions())
		})
	}
}

func TestModelAgreesWithDefaultOptions(t *testing.T) {
	// The same workload against a store that mostly stays in memory,
	// which exercises a completely different set of code paths.
	runModelTest(t, 42, 3000, Options{})
}

func TestModelAgreesWithoutBloomFilters(t *testing.T) {
	opts := smallOptions()
	opts.BloomBitsPerKey = -1
	runModelTest(t, 7, 3000, opts)
}

func TestModelAgreesWithTinyBlocks(t *testing.T) {
	opts := smallOptions()
	opts.BlockSize = 64
	opts.TargetFileSize = 1 << 10
	runModelTest(t, 99, 3000, opts)
}
