# Changelog

All notable changes to keelstore are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-21

First release: a complete, crash-safe LSM-tree storage engine.

### Added

- **Write path.** `Put` and `Delete` append a CRC-32C checksummed record to
  a write-ahead log and then insert it into an in-memory skiplist. A full
  memtable is frozen, handed to a background goroutine and written out as a
  level-0 SSTable.
- **Skiplist memtable.** Written from scratch, single-writer, lock-free for
  readers: forward pointers are atomic and new nodes are linked bottom-up,
  so a reader never observes a half-linked node.
- **SSTable format.** Sorted data blocks with per-block checksums, a sparse
  index holding the first key of each block, a Bloom filter block, and a
  64-byte footer carrying offsets, an entry count, a sequence range, a CRC
  and the `KEEL` magic number. The layout is documented byte by byte in the
  README.
- **Bloom filters.** Written from scratch: a bit array plus k probes
  derived from one 64-bit hash by double hashing. 10 bits per key by
  default, for roughly a 1% false positive rate.
- **Read path.** Memtable, then frozen memtables, then level 0 newest file
  first, then one file per deeper level found by binary search. Bloom
  filters short-circuit absent keys before any disk read.
- **Levelled compaction.** Runs in a background goroutine, merges
  overlapping runs, drops shadowed values and tombstones only when no
  snapshot can still observe them, and aborts cleanly on `Close`.
- **Crash recovery.** `Open` replays every write-ahead log in the
  directory. A record that was only partially written fails its length or
  checksum test and is discarded along with everything after it; recovered
  data is folded into an SSTable before the logs are removed.
- **Snapshots and iterators.** Every write gets a monotonic sequence
  number. `Snapshot` pins a consistent read view; `Iterator` merges the
  memtable and every level into one sorted stream over a key range,
  respecting the snapshot and hiding tombstones.
- **Introspection.** `Stats` reports memtable size, files and bytes per
  level, sequence numbers, active snapshots, flush and compaction counts,
  bytes written and read, log bytes discarded, and the *observed* Bloom
  filter false positive rate.
- **`cmd/keelctl`.** A dependency-free CLI with `put`, `get`, `delete`,
  `scan`, `stats` and `compact`.
- **Verification.** `make check` runs formatting, vet, lint, the test
  suite, the build and a benchmark smoke test. `make race` runs the suite
  under the race detector inside a pinned Docker image. `.githooks/pre-commit`
  refuses a commit that does not pass.

### Known limitations

See the README: single-process access only, no transactions spanning
several keys, no column families, no block cache, and no compression.

[0.1.0]: https://github.com/aminyx/keelstore/releases/tag/v0.1.0
