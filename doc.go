// Package keelstore implements an embedded, crash-safe, log-structured
// merge-tree (LSM) key/value store in pure Go, with no third-party
// dependencies.
//
// keelstore is the kind of engine that sits underneath databases such as
// RocksDB, Pebble or LevelDB, written small enough to read end to end and
// complete enough to survive a kill -9 in the middle of a write.
//
// # Data model
//
// Keys and values are arbitrary byte slices. Keys must be non-empty. A key
// maps to at most one value; there are no column families and no
// multi-key transactions.
//
// Every mutation is stamped with a monotonically increasing sequence
// number. A [Snapshot] pins a sequence number and gives a stable read view
// that later writes cannot disturb.
//
// # Write path
//
// Put and Delete append a checksummed record to a write-ahead log and then
// insert the record into an in-memory skiplist (the memtable). When the
// memtable exceeds Options.MemtableSize it is frozen, a fresh WAL is
// started, and a background goroutine flushes the frozen memtable into a
// sorted, immutable SSTable at level 0. Deletes are tombstones: they are
// written like any other record and only physically disappear during
// compaction, once no snapshot can observe the value they hide.
//
// # Read path
//
//	Get(key) -> memtable -> immutable memtables -> L0 (newest file first)
//	                     -> L1 .. Ln (at most one file per level)
//
// The first version of the key that is visible at the read's sequence
// number wins. Each SSTable carries a Bloom filter, so a lookup for an
// absent key usually costs no disk reads at all.
//
// # Durability
//
// Each WAL record is framed with a CRC-32C checksum over its payload. A
// record that was only partially written when the process died fails its
// length or checksum test during replay and is discarded, together with
// everything that follows it. Acknowledged writes — those for which Put
// returned nil — are always recovered.
//
// # Concurrency
//
// One writer, many readers. Put and Delete serialise against each
// other on an internal mutex; Get, Snapshot and Iterator are safe to call
// from any number of goroutines at the same time, concurrently with the
// writer. See the README for the full memory model discussion.
//
// # Example
//
//	db, err := keelstore.Open("data", nil)
//	if err != nil {
//	        log.Fatal(err)
//	}
//	defer db.Close()
//
//	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
//	        log.Fatal(err)
//	}
//	v, err := db.Get([]byte("hello"))
//	fmt.Printf("%s %v\n", v, err)
//
//	snap := db.Snapshot()
//	defer snap.Release()
//
//	it := snap.NewIterator(&keelstore.IterOptions{Start: []byte("a"), End: []byte("z")})
//	defer it.Close()
//	for it.Next() {
//	        fmt.Printf("%s=%s\n", it.Key(), it.Value())
//	}
package keelstore
