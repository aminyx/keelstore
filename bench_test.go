package keelstore

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

// Benchmarks use a real directory on the real filesystem, because the
// interesting costs in an LSM tree are I/O and the merge, not the map
// lookup. The numbers in the README come from `go test -bench`.

const benchValueSize = 100

func benchOptions() Options {
	// Closer to production than smallOptions: real memtables, real block
	// sizes. The store still has to flush and compact during a long run.
	return Options{
		MemtableSize:   1 << 20,
		BlockSize:      4 << 10,
		TargetFileSize: 2 << 20,
		LevelBaseSize:  4 << 20,
	}
}

func benchDB(b *testing.B, opts Options) *DB {
	b.Helper()
	db, err := Open(b.TempDir(), &opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func benchValue() []byte {
	v := make([]byte, benchValueSize)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

// populate fills a store with n keys and returns it, ready to be read from.
func populate(b *testing.B, opts Options, n int) *DB {
	b.Helper()
	db := benchDB(b, opts)
	v := benchValue()
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(key(i)), v); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := db.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	return db
}

func BenchmarkPutSequential(b *testing.B) {
	db := benchDB(b, benchOptions())
	v := benchValue()
	b.SetBytes(int64(len(key(0)) + benchValueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put([]byte(key(i)), v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutRandom(b *testing.B) {
	db := benchDB(b, benchOptions())
	v := benchValue()
	rnd := rand.New(rand.NewSource(1))
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = []byte(key(rnd.Intn(1 << 20)))
	}
	b.SetBytes(int64(len(key(0)) + benchValueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i], v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSync(b *testing.B) {
	opts := benchOptions()
	opts.SyncWrites = true
	db := benchDB(b, opts)
	v := benchValue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put([]byte(key(i)), v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetHit(b *testing.B) {
	const n = 100000
	db := populate(b, benchOptions(), n)
	rnd := rand.New(rand.NewSource(2))
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = []byte(key(rnd.Intn(n)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// The Bloom filter's whole reason for existing: a lookup for a key that is
// not there should not touch the disk.
func BenchmarkGetMissBloom(b *testing.B) {
	const n = 100000
	db := populate(b, benchOptions(), n)
	rnd := rand.New(rand.NewSource(3))
	keys := make([][]byte, 4096)
	for i := range keys {
		// Absent, but inside the key range of every table, so only the
		// filter can reject it.
		keys[i] = []byte(fmt.Sprintf("key-%06d-absent", rnd.Intn(n)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%len(keys)]); !errors.Is(err, ErrNotFound) {
			b.Fatalf("Get: %v", err)
		}
	}
	b.StopTimer()
	s := db.Stats()
	b.ReportMetric(s.BloomFalsePositiveRate, "bloom-fp-rate")
}

func BenchmarkGetMissNoBloom(b *testing.B) {
	const n = 100000
	opts := benchOptions()
	opts.BloomBitsPerKey = -1
	db := populate(b, opts, n)
	rnd := rand.New(rand.NewSource(3))
	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%06d-absent", rnd.Intn(n)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%len(keys)]); !errors.Is(err, ErrNotFound) {
			b.Fatalf("Get: %v", err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	const n = 100000
	db := populate(b, benchOptions(), n)
	b.SetBytes(int64(len(key(0)) + benchValueSize))
	b.ResetTimer()

	it := db.NewIterator(nil)
	for i := 0; i < b.N; i++ {
		if !it.Next() {
			it.Close()
			it = db.NewIterator(nil)
			if !it.Next() {
				b.Fatal("the store is empty")
			}
		}
		_ = it.Key()
		_ = it.Value()
	}
	it.Close()
}

func BenchmarkSkiplistInsert(b *testing.B) {
	s := newSkiplist()
	v := benchValue()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Insert(makeInternalKey([]byte(key(i)), uint64(i+1), kindValue), v)
	}
}

func BenchmarkSkiplistGet(b *testing.B) {
	const n = 200000
	s := newSkiplist()
	v := benchValue()
	for i := 0; i < n; i++ {
		s.Insert(makeInternalKey([]byte(key(i)), uint64(i+1), kindValue), v)
	}
	rnd := rand.New(rand.NewSource(4))
	keys := make([][]byte, 4096)
	for i := range keys {
		j := rnd.Intn(n)
		keys[i] = makeInternalKey([]byte(key(j)), uint64(j+1), kindValue)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := s.Get(keys[i%len(keys)]); !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkBloomMayContain(b *testing.B) {
	hashes := make([]uint64, 100000)
	for i := range hashes {
		hashes[i] = bloomHash([]byte(key(i)))
	}
	f := buildBloom(hashes, DefaultBloomBitsPerKey)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.MayContain(hashes[i%len(hashes)])
	}
}
