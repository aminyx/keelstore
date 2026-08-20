package keelstore

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// counters are the raw atomic tallies behind Stats. They are incremented on
// hot paths, so every field is a lock-free atomic.
type counters struct {
	puts    atomic.Uint64
	deletes atomic.Uint64
	gets    atomic.Uint64

	flushes              atomic.Uint64
	compactions          atomic.Uint64
	compactionsCancelled atomic.Uint64

	bytesWritten atomic.Int64 // bytes handed to SSTable files
	bytesRead    atomic.Int64 // bytes read out of SSTable data blocks
	walBytes     atomic.Int64 // bytes appended to write-ahead logs
	walDropped   atomic.Int64 // bytes discarded from damaged log tails

	bloomChecks         atomic.Uint64 // Bloom filters consulted
	bloomSkips          atomic.Uint64 // filters that said "definitely absent"
	bloomFalsePositives atomic.Uint64 // filters that said "maybe" and were wrong
}

// Stats is a point-in-time view of what the engine is doing. It is cheap to
// call: everything is either an atomic load or a walk over the level
// metadata, which never involves disk.
//
// The tree fields describe what is on disk and survive a reopen. The
// counters — operations, flushes, compactions, bytes, Bloom filter
// behaviour — are per-open and start at zero every time the directory is
// opened.
type Stats struct {
	// Memory
	MemtableBytes      int64
	MemtableEntries    int64
	ImmutableMemtables int

	// Tree shape
	SSTables         int
	SSTablesPerLevel []int
	BytesPerLevel    []int64
	TotalTableBytes  int64

	// Sequencing
	LastSequence    uint64
	ActiveSnapshots int

	// Work done
	Puts                 uint64
	Deletes              uint64
	Gets                 uint64
	Flushes              uint64
	Compactions          uint64
	CompactionsCancelled uint64

	// I/O
	BytesWritten    int64
	BytesRead       int64
	WALBytes        int64
	WALBytesDropped int64

	// Bloom filter behaviour, measured rather than estimated.
	BloomChecks         uint64
	BloomSkips          uint64
	BloomFalsePositives uint64
	// BloomFalsePositiveRate is falsePositives / (skips + falsePositives):
	// the fraction of lookups for absent keys that the filter failed to
	// reject. It is 0 until a lookup misses.
	BloomFalsePositiveRate float64
}

// String renders the stats as an aligned report, which is what keelctl
// prints.
func (s Stats) String() string {
	var b strings.Builder
	w := func(k string, format string, args ...any) {
		fmt.Fprintf(&b, "  %-24s %s\n", k, fmt.Sprintf(format, args...))
	}
	b.WriteString("keelstore stats\n")
	b.WriteString(" memory\n")
	w("memtable.bytes", "%d", s.MemtableBytes)
	w("memtable.entries", "%d", s.MemtableEntries)
	w("memtable.immutable", "%d", s.ImmutableMemtables)
	b.WriteString(" tree\n")
	w("sstables.total", "%d", s.SSTables)
	for i, n := range s.SSTablesPerLevel {
		if n == 0 && s.BytesPerLevel[i] == 0 {
			continue
		}
		w(fmt.Sprintf("sstables.L%d", i), "%d file(s), %d bytes", n, s.BytesPerLevel[i])
	}
	w("sstables.bytes", "%d", s.TotalTableBytes)
	b.WriteString(" sequence\n")
	w("sequence.last", "%d", s.LastSequence)
	w("snapshots.active", "%d", s.ActiveSnapshots)
	b.WriteString(" work\n")
	w("ops.put", "%d", s.Puts)
	w("ops.delete", "%d", s.Deletes)
	w("ops.get", "%d", s.Gets)
	w("flushes", "%d", s.Flushes)
	w("compactions", "%d", s.Compactions)
	w("compactions.cancelled", "%d", s.CompactionsCancelled)
	b.WriteString(" io\n")
	w("bytes.written", "%d", s.BytesWritten)
	w("bytes.read", "%d", s.BytesRead)
	w("wal.bytes", "%d", s.WALBytes)
	w("wal.bytes_dropped", "%d", s.WALBytesDropped)
	b.WriteString(" bloom\n")
	w("bloom.checks", "%d", s.BloomChecks)
	w("bloom.skips", "%d", s.BloomSkips)
	w("bloom.false_positives", "%d", s.BloomFalsePositives)
	w("bloom.fp_rate", "%.4f", s.BloomFalsePositiveRate)
	return b.String()
}
