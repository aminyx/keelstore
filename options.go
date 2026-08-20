package keelstore

// Default tuning constants. They are deliberately small: keelstore is an
// embedded store for single-process applications, not a server engine.
const (
	DefaultMemtableSize        = 4 << 20 // 4 MiB
	DefaultBlockSize           = 4 << 10 // 4 KiB
	DefaultTargetFileSize      = 2 << 20 // 2 MiB
	DefaultL0CompactionTrigger = 4
	DefaultMaxLevels           = 7
	DefaultLevelBaseSize       = 8 << 20 // 8 MiB budget for L1
	DefaultLevelMultiplier     = 10
	DefaultBloomBitsPerKey     = 10 // ~1% false positive rate
	DefaultMaxImmutable        = 2

	// maxKeyLen bounds a single user key. The limit exists so that an
	// internal key always fits comfortably in a data block.
	maxKeyLen = 1 << 20
)

// Options configures a database. The zero value is valid and is filled in
// with the Default* constants above, so `Open(dir, nil)` is the normal way
// to open a store.
type Options struct {
	// MemtableSize is the approximate number of bytes a memtable may hold
	// before it is frozen and flushed to an SSTable at level 0.
	MemtableSize int64

	// MaxImmutableMemtables bounds how many frozen memtables may be
	// waiting for the flusher before writers are throttled.
	MaxImmutableMemtables int

	// BlockSize is the target size of an SSTable data block. The sparse
	// index holds one entry per block, so smaller blocks mean a larger
	// index and fewer bytes read per point lookup.
	BlockSize int

	// TargetFileSize is the size at which a compaction rolls over to a new
	// output SSTable.
	TargetFileSize int64

	// L0CompactionTrigger is the number of level-0 files that schedules an
	// L0 -> L1 compaction. Level 0 files may overlap each other, so every
	// one of them has to be consulted on a read miss; keeping the count
	// low is what keeps reads fast.
	L0CompactionTrigger int

	// MaxLevels is the number of levels in the tree, including level 0.
	MaxLevels int

	// LevelBaseSize is the byte budget for level 1. Level n's budget is
	// LevelBaseSize * LevelSizeMultiplier^(n-1).
	LevelBaseSize int64

	// LevelSizeMultiplier is the growth factor between levels.
	LevelSizeMultiplier int64

	// BloomBitsPerKey controls the Bloom filter attached to every SSTable.
	// 10 bits per key, the default, gives roughly a 1% false positive
	// rate. A negative value disables Bloom filters entirely.
	BloomBitsPerKey int

	// SyncWrites makes every Put and Delete fsync the write-ahead log
	// before returning. Off by default: without it a write survives a
	// process crash (the bytes are in the OS page cache) but not a machine
	// power loss.
	SyncWrites bool

	// disableAutoCompaction stops the background worker from starting
	// compactions on its own. Flushes still happen, and an explicit call
	// to Compact still works. Tests use it to keep level layouts stable
	// while they assert on them.
	disableAutoCompaction bool
}

// DefaultOptions returns the options used when Open is passed nil.
func DefaultOptions() Options {
	return Options{}.withDefaults()
}

func (o Options) withDefaults() Options {
	if o.MemtableSize <= 0 {
		o.MemtableSize = DefaultMemtableSize
	}
	if o.MaxImmutableMemtables <= 0 {
		o.MaxImmutableMemtables = DefaultMaxImmutable
	}
	if o.BlockSize <= 0 {
		o.BlockSize = DefaultBlockSize
	}
	if o.TargetFileSize <= 0 {
		o.TargetFileSize = DefaultTargetFileSize
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = DefaultL0CompactionTrigger
	}
	if o.MaxLevels <= 1 {
		o.MaxLevels = DefaultMaxLevels
	}
	if o.LevelBaseSize <= 0 {
		o.LevelBaseSize = DefaultLevelBaseSize
	}
	if o.LevelSizeMultiplier <= 1 {
		o.LevelSizeMultiplier = DefaultLevelMultiplier
	}
	switch {
	case o.BloomBitsPerKey == 0:
		o.BloomBitsPerKey = DefaultBloomBitsPerKey
	case o.BloomBitsPerKey < 0:
		o.BloomBitsPerKey = 0 // explicitly disabled
	}
	return o
}

// maxBytesForLevel returns the size budget of a level with data, in bytes.
// Level 0 is governed by a file count rather than a byte budget.
func (o Options) maxBytesForLevel(level int) int64 {
	if level <= 1 {
		return o.LevelBaseSize
	}
	n := o.LevelBaseSize
	for i := 1; i < level; i++ {
		n *= o.LevelSizeMultiplier
	}
	return n
}
