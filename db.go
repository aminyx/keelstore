package keelstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// DB is an embedded LSM-tree key/value store backed by a directory.
//
// A directory may be opened by one DB at a time. Within a process the DB is
// safe for one writing goroutine and any number of readers; see the package
// documentation for the memory model.
type DB struct {
	dir  string
	opts Options

	// wmu serialises writers. keelstore is a single-writer engine: this is
	// the mutex that makes that true.
	wmu sync.Mutex

	// mu guards the mutable pointers below. Critical sections are short —
	// swapping a pointer, appending to a slice — never I/O.
	mu        sync.RWMutex
	mem       *memtable
	imm       []*memtable // frozen, waiting for the flusher; copy-on-write
	cur       *version
	snapshots map[uint64]int
	flushCond *sync.Cond
	closing   bool // set by Close before waking blocked writers
	bgFailed  bool // set when the background worker gave up

	// vmu guards the set of live versions and the files waiting to be
	// deleted once no live version references them.
	vmu      sync.Mutex
	live     []*version
	obsolete map[uint64]struct{}

	// bgMu is held for the duration of a flush or a compaction, so that
	// the background worker and an explicit Flush/Compact never overlap.
	bgMu sync.Mutex

	wal      *walWriter
	nextFile atomic.Uint64
	seq      atomic.Uint64 // sequence numbers handed out by the writer
	visible  atomic.Uint64 // sequence numbers readers are allowed to see

	cache *tableCache
	stats counters

	compactPtr [][]byte // per level, where the next compaction should start

	bgSignal chan struct{}
	bgCancel context.CancelFunc
	bgWG     sync.WaitGroup

	errMu  sync.Mutex
	bgErr  error
	closed atomic.Bool
}

// Open opens or creates the store rooted at dir. Passing nil options uses
// DefaultOptions.
//
// Open replays any write-ahead log left behind by a previous run. Records
// that were only partially written are detected by their checksum and
// dropped; every acknowledged write is recovered.
func Open(dir string, opts *Options) (*DB, error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	o = o.withDefaults()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:       dir,
		opts:      o,
		snapshots: make(map[uint64]int),
		obsolete:  make(map[uint64]struct{}),
		bgSignal:  make(chan struct{}, 1),
	}
	db.cache = newTableCache(dir, &db.stats)
	db.flushCond = sync.NewCond(&db.mu)
	db.compactPtr = make([][]byte, o.MaxLevels)

	var (
		nextFile uint64 = 1
		lastSeq  uint64
		ver      *version
	)
	if manifestExists(dir) {
		var err error
		nextFile, lastSeq, ver, err = readManifest(dir, o.MaxLevels)
		if err != nil {
			return nil, err
		}
		if len(ver.levels) < o.MaxLevels {
			grown := newVersion(o.MaxLevels)
			copy(grown.levels, ver.levels)
			ver = grown
		}
		db.compactPtr = make([][]byte, len(ver.levels))
	} else {
		ver = newVersion(o.MaxLevels)
	}

	db.seq.Store(lastSeq)
	db.visible.Store(lastSeq)
	db.nextFile.Store(nextFile)
	db.installFirstVersion(ver)

	if err := db.reclaimOrphans(); err != nil {
		db.cache.close()
		return nil, err
	}
	if err := db.recoverLogs(); err != nil {
		db.cache.close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	db.bgCancel = cancel
	db.bgWG.Add(1)
	go db.backgroundLoop(ctx)

	return db, nil
}

// installFirstVersion seeds the version list at Open.
func (db *DB) installFirstVersion(v *version) {
	v.refs.Store(1)
	db.vmu.Lock()
	db.live = append(db.live, v)
	db.vmu.Unlock()
	db.mu.Lock()
	db.cur = v
	db.mu.Unlock()
}

// reclaimOrphans deletes SSTables that no manifest references. They are the
// output of a flush or compaction that was interrupted before its manifest
// swap, so nothing has ever read them and nothing ever will.
func (db *DB) reclaimOrphans() error {
	live := db.currentVersionNoRef().allNumbers()

	sstNums, err := listNumberedFiles(db.dir, sstExt)
	if err != nil {
		return err
	}
	for _, n := range sstNums {
		if n >= db.nextFile.Load() {
			db.nextFile.Store(n + 1)
		}
		if _, ok := live[n]; !ok {
			if err := os.Remove(sstPath(db.dir, n)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	walNums, err := listNumberedFiles(db.dir, walExt)
	if err != nil {
		return err
	}
	for _, n := range walNums {
		if n >= db.nextFile.Load() {
			db.nextFile.Store(n + 1)
		}
	}
	return nil
}

// recoverLogs replays every write-ahead log in the directory, in file
// number order, flushes the result to a level-0 table and starts a fresh
// log. Rebuilding into a table rather than keeping the recovered memtable
// in memory keeps the log lifecycle trivial: at most one log per memtable,
// deleted as soon as its memtable is on disk.
func (db *DB) recoverLogs() error {
	walNums, err := listNumberedFiles(db.dir, walExt)
	if err != nil {
		return err
	}

	rec := newMemtable(0)
	maxSeq := db.seq.Load()
	for _, n := range walNums {
		res, err := replayWAL(walPath(db.dir, n), func(kind uint8, seq uint64, key, value []byte) error {
			rec.Add(key, value, seq, kind)
			if seq > maxSeq {
				maxSeq = seq
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("replaying %s: %w", walPath(db.dir, n), err)
		}
		db.stats.walDropped.Add(res.DroppedBytes)
	}
	db.seq.Store(maxSeq)
	db.visible.Store(maxSeq)

	if !rec.Empty() {
		meta, err := db.writeTableFromMemtable(rec)
		if err != nil {
			return err
		}
		v := db.currentVersionNoRef().clone()
		v.levels[0] = append(v.levels[0], meta)
		v.sortLevel(0)
		if err := db.syncManifest(v); err != nil {
			return err
		}
		db.installVersion(v)
		db.stats.flushes.Add(1)
	}

	// Only now, with the data durably in an SSTable named by the manifest,
	// may the logs go away.
	for _, n := range walNums {
		if err := os.Remove(walPath(db.dir, n)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	walNum := db.nextFile.Add(1) - 1
	w, err := newWALWriter(walPath(db.dir, walNum))
	if err != nil {
		return err
	}
	db.wal = w
	db.mu.Lock()
	db.mem = newMemtable(walNum)
	db.mu.Unlock()

	return db.syncManifest(db.currentVersionNoRef())
}

func (db *DB) syncManifest(v *version) error {
	return writeManifest(db.dir, db.nextFile.Load(), db.visible.Load(), v)
}

// -------------------------------------------------------------------------
// version bookkeeping
// -------------------------------------------------------------------------

func (db *DB) currentVersionNoRef() *version {
	db.mu.RLock()
	v := db.cur
	db.mu.RUnlock()
	return v
}

// acquireVersion returns the current version with a reference held. The
// caller must call releaseVersion. Holding a reference guarantees that the
// SSTables the version names stay on disk.
func (db *DB) acquireVersion() *version {
	db.mu.RLock()
	v := db.cur
	v.ref()
	db.mu.RUnlock()
	return v
}

func (db *DB) releaseVersion(v *version) {
	if v.unref() != 0 {
		return
	}
	db.vmu.Lock()
	for i, lv := range db.live {
		if lv == v {
			db.live = append(db.live[:i], db.live[i+1:]...)
			break
		}
	}
	db.deleteObsoleteLocked()
	db.vmu.Unlock()
}

// installVersion makes v the current version and drops the reference the
// old one held.
func (db *DB) installVersion(v *version) {
	v.refs.Store(1)
	db.vmu.Lock()
	db.live = append(db.live, v)
	db.vmu.Unlock()

	db.mu.Lock()
	old := db.cur
	db.cur = v
	db.mu.Unlock()

	if old != nil {
		db.releaseVersion(old)
	}
}

// markObsolete records tables that the newest version dropped. They are
// removed once every version that still names them is gone.
func (db *DB) markObsolete(numbers []uint64) {
	db.vmu.Lock()
	for _, n := range numbers {
		db.obsolete[n] = struct{}{}
	}
	db.deleteObsoleteLocked()
	db.vmu.Unlock()
}

func (db *DB) deleteObsoleteLocked() {
	if len(db.obsolete) == 0 {
		return
	}
	stillLive := make(map[uint64]struct{})
	for _, v := range db.live {
		for n := range v.allNumbers() {
			stillLive[n] = struct{}{}
		}
	}
	for n := range db.obsolete {
		if _, ok := stillLive[n]; ok {
			continue
		}
		// The reader must be closed before the file is unlinked: Windows
		// refuses to delete a file that is still open.
		db.cache.evict(n)
		if err := os.Remove(sstPath(db.dir, n)); err == nil || os.IsNotExist(err) {
			delete(db.obsolete, n)
		}
	}
}

// -------------------------------------------------------------------------
// write path
// -------------------------------------------------------------------------

// Put stores value under key. An empty key is rejected; an empty value is
// fine and is not the same thing as a delete.
func (db *DB) Put(key, value []byte) error {
	if err := db.write(key, value, kindValue); err != nil {
		return err
	}
	db.stats.puts.Add(1)
	return nil
}

// Delete writes a tombstone for key. Deleting an absent key is not an
// error: the tombstone is recorded either way and disappears during a later
// compaction.
func (db *DB) Delete(key []byte) error {
	if err := db.write(key, nil, kindDelete); err != nil {
		return err
	}
	db.stats.deletes.Add(1)
	return nil
}

func (db *DB) write(key, value []byte, kind uint8) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > maxKeyLen {
		return ErrKeyTooLarge
	}
	if db.closed.Load() {
		return ErrClosed
	}

	db.wmu.Lock()
	defer db.wmu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}
	if err := db.backgroundError(); err != nil {
		return err
	}
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	seq := db.seq.Add(1)

	payload := encodeWALPayload(nil, kind, seq, key, value)
	if err := db.wal.Append(payload); err != nil {
		return err
	}
	db.stats.walBytes.Add(int64(walHeaderLen + len(payload)))
	if db.opts.SyncWrites {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}

	// db.mem is only ever reassigned by the writer, and the writer is us:
	// no lock is needed to read it here, only to change it.
	db.mem.Add(key, value, seq, kind)

	// Publishing the sequence number only after the memtable insert means
	// a reader never sees a sequence number whose record it cannot find.
	db.visible.Store(seq)
	return nil
}

// makeRoomForWrite freezes the memtable when it is full and throttles the
// writer if the flusher has fallen too far behind. Called with wmu held.
//
// closing and bgFailed are read here under mu and written under mu by Close
// and setBackgroundError, so a writer can never miss the wake-up and park
// on the condition variable forever.
func (db *DB) makeRoomForWrite() error {
	// Fast path: no lock at all. Only the writer replaces db.mem, and the
	// skiplist's size counter is atomic.
	if db.mem.ApproxBytes() < db.opts.MemtableSize {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for len(db.imm) >= db.opts.MaxImmutableMemtables {
		if db.closing {
			return ErrClosed
		}
		if db.bgFailed {
			return db.backgroundError()
		}
		db.signalBackground()
		db.flushCond.Wait()
	}
	return db.rotateMemtableLocked()
}

// rotateMemtable freezes the active memtable and starts a fresh log for its
// replacement. Called with wmu held.
func (db *DB) rotateMemtable() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.rotateMemtableLocked()
}

// rotateMemtableLocked is rotateMemtable with wmu and mu already held.
func (db *DB) rotateMemtableLocked() error {
	walNum := db.nextFile.Add(1) - 1
	w, err := newWALWriter(walPath(db.dir, walNum))
	if err != nil {
		return err
	}
	if err := db.wal.Close(); err != nil {
		w.Close()
		return err
	}
	db.wal = w

	// imm is copy-on-write so that readers can capture the slice header
	// under a read lock and use it after releasing the lock.
	db.imm = append(append([]*memtable(nil), db.imm...), db.mem)
	db.mem = newMemtable(walNum)

	db.signalBackground()
	return nil
}

// -------------------------------------------------------------------------
// read path
// -------------------------------------------------------------------------

// Get returns the value stored under key, or ErrNotFound if the key is
// absent or its newest visible version is a tombstone.
//
// The returned slice is a fresh copy owned by the caller; it never aliases
// engine memory, so it is always safe to keep or modify.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.getAt(key, db.visible.Load())
}

func (db *DB) getAt(key []byte, seq uint64) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	db.stats.gets.Add(1)

	db.mu.RLock()
	mem, imm, v := db.mem, db.imm, db.cur
	v.ref()
	db.mu.RUnlock()
	defer db.releaseVersion(v)

	if value, kind, found := mem.Get(key, seq); found {
		return decodeLookup(value, kind)
	}
	for i := len(imm) - 1; i >= 0; i-- {
		if value, kind, found := imm[i].Get(key, seq); found {
			return decodeLookup(value, kind)
		}
	}

	// Level 0 files overlap, so every one of them has to be checked,
	// newest first. Deeper levels are disjoint: at most one file per level
	// can hold the key, and a binary search finds it.
	for _, t := range v.levels[0] {
		if !inRange(t, key) {
			continue
		}
		value, kind, found, err := db.getFromTable(t, key, seq)
		if err != nil {
			return nil, err
		}
		if found {
			return decodeLookup(value, kind)
		}
	}
	for level := 1; level < len(v.levels); level++ {
		t := v.findFile(level, key)
		if t == nil {
			continue
		}
		value, kind, found, err := db.getFromTable(t, key, seq)
		if err != nil {
			return nil, err
		}
		if found {
			return decodeLookup(value, kind)
		}
	}
	return nil, ErrNotFound
}

func inRange(t *tableMeta, userKey []byte) bool {
	return bytes.Compare(userKey, t.smallestUser()) >= 0 &&
		bytes.Compare(userKey, t.largestUser()) <= 0
}

func (db *DB) getFromTable(t *tableMeta, key []byte, seq uint64) (value []byte, kind uint8, found bool, err error) {
	r, err := db.cache.get(t.Number)
	if err != nil {
		return nil, 0, false, err
	}
	db.stats.bloomChecks.Add(1)
	value, kind, found, filtered, err := r.Get(key, seq)
	if err != nil {
		return nil, 0, false, err
	}
	switch {
	case filtered:
		db.stats.bloomSkips.Add(1)
	case !found:
		// The filter said "maybe" and the table did not have the key.
		db.stats.bloomFalsePositives.Add(1)
	}
	return value, kind, found, nil
}

func decodeLookup(value []byte, kind uint8) ([]byte, error) {
	if kind == kindDelete {
		return nil, ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

// Has reports whether key currently resolves to a value.
func (db *DB) Has(key []byte) (bool, error) {
	_, err := db.Get(key)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// -------------------------------------------------------------------------
// snapshots
// -------------------------------------------------------------------------

// A Snapshot is a read view pinned to a sequence number. Writes made after
// it was taken are invisible through it, and compaction will not discard
// data it can still see, until it is released.
//
// Every Snapshot must be released; a leaked snapshot pins garbage forever.
type Snapshot struct {
	db       *DB
	seq      uint64
	released atomic.Bool
}

// Snapshot pins the current sequence number.
func (db *DB) Snapshot() *Snapshot {
	seq := db.visible.Load()
	db.mu.Lock()
	db.snapshots[seq]++
	db.mu.Unlock()
	return &Snapshot{db: db, seq: seq}
}

// Sequence is the sequence number the snapshot is pinned to.
func (s *Snapshot) Sequence() uint64 { return s.seq }

// Get reads through the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.released.Load() {
		return nil, ErrSnapshotReleased
	}
	return s.db.getAt(key, s.seq)
}

// Release drops the snapshot. Calling it twice is harmless.
func (s *Snapshot) Release() {
	if s.released.Swap(true) {
		return
	}
	db := s.db
	db.mu.Lock()
	if n := db.snapshots[s.seq]; n <= 1 {
		delete(db.snapshots, s.seq)
	} else {
		db.snapshots[s.seq] = n - 1
	}
	db.mu.Unlock()
}

// oldestSnapshotSeq is the sequence number below which no reader can
// observe anything, which is the line compaction uses to decide whether a
// shadowed value or a tombstone may be dropped.
func (db *DB) oldestSnapshotSeq() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	oldest := db.visible.Load()
	for seq := range db.snapshots {
		if seq < oldest {
			oldest = seq
		}
	}
	return oldest
}

// -------------------------------------------------------------------------
// maintenance
// -------------------------------------------------------------------------

// Flush freezes the active memtable and writes every frozen memtable to a
// level-0 SSTable before returning.
func (db *DB) Flush() error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.wmu.Lock()
	db.mu.RLock()
	empty := db.mem.Empty()
	db.mu.RUnlock()
	if !empty {
		if err := db.rotateMemtable(); err != nil {
			db.wmu.Unlock()
			return err
		}
	}
	db.wmu.Unlock()

	for {
		db.mu.RLock()
		pending := len(db.imm)
		db.mu.RUnlock()
		if pending == 0 {
			return nil
		}
		db.bgMu.Lock()
		err := db.flushOneMemtable(context.Background())
		db.bgMu.Unlock()
		if err != nil {
			return err
		}
	}
}

// Compact flushes memory and rewrites the tree from the top down: every
// level that holds files is merged into the level below it, and then the
// ordinary size-based rules are applied until no level is over budget.
//
// Merging a level is what actually reclaims space, because that is when
// shadowed values and tombstones are dropped. Data can only move one level
// per call, and never past the last level, so a store that has been
// hammered may want more than one Compact.
func (db *DB) Compact() error {
	if err := db.Flush(); err != nil {
		return err
	}
	ctx := context.Background()

	v := db.acquireVersion()
	deepest := -1
	for level := range v.levels {
		if len(v.levels[level]) > 0 {
			deepest = level
		}
	}
	last := len(v.levels) - 1
	db.releaseVersion(v)

	for level := 0; level <= deepest && level < last; level++ {
		db.bgMu.Lock()
		v := db.acquireVersion()
		job := forcedCompaction(v, level)
		var err error
		if job != nil {
			err = db.runCompaction(ctx, v, job)
		}
		db.releaseVersion(v)
		db.bgMu.Unlock()
		if err != nil {
			return err
		}
	}

	for {
		db.bgMu.Lock()
		v := db.acquireVersion()
		job := db.pickCompaction(v)
		if job == nil {
			db.releaseVersion(v)
			db.bgMu.Unlock()
			return nil
		}
		err := db.runCompaction(ctx, v, job)
		db.releaseVersion(v)
		db.bgMu.Unlock()
		if err != nil {
			return err
		}
	}
}

// Stats returns a point-in-time report. See [Stats].
func (db *DB) Stats() Stats {
	db.mu.RLock()
	mem, imm, v, snaps := db.mem, db.imm, db.cur, len(db.snapshots)
	db.mu.RUnlock()

	s := Stats{
		MemtableBytes:      mem.ApproxBytes(),
		MemtableEntries:    mem.Entries(),
		ImmutableMemtables: len(imm),
		SSTablesPerLevel:   make([]int, len(v.levels)),
		BytesPerLevel:      make([]int64, len(v.levels)),
		LastSequence:       db.visible.Load(),
		ActiveSnapshots:    snaps,

		Puts:                 db.stats.puts.Load(),
		Deletes:              db.stats.deletes.Load(),
		Gets:                 db.stats.gets.Load(),
		Flushes:              db.stats.flushes.Load(),
		Compactions:          db.stats.compactions.Load(),
		CompactionsCancelled: db.stats.compactionsCancelled.Load(),

		BytesWritten:    db.stats.bytesWritten.Load(),
		BytesRead:       db.stats.bytesRead.Load(),
		WALBytes:        db.stats.walBytes.Load(),
		WALBytesDropped: db.stats.walDropped.Load(),

		BloomChecks:         db.stats.bloomChecks.Load(),
		BloomSkips:          db.stats.bloomSkips.Load(),
		BloomFalsePositives: db.stats.bloomFalsePositives.Load(),
	}
	for i, level := range v.levels {
		s.SSTablesPerLevel[i] = len(level)
		for _, t := range level {
			s.BytesPerLevel[i] += t.Size
		}
		s.SSTables += len(level)
		s.TotalTableBytes += s.BytesPerLevel[i]
	}
	if neg := s.BloomSkips + s.BloomFalsePositives; neg > 0 {
		s.BloomFalsePositiveRate = float64(s.BloomFalsePositives) / float64(neg)
	}
	return s
}

// Path is the directory the store lives in.
func (db *DB) Path() string { return db.dir }

// injectTornTail is test-only fault injection: the next record appended to
// the write-ahead log is truncated to n bytes, reproducing the on-disk
// state a machine that lost power mid-write leaves behind. It is
// unexported, so no caller outside this package can reach it.
func (db *DB) injectTornTail(n int) {
	db.wmu.Lock()
	db.wal.tornTail = n
	db.wmu.Unlock()
}

// Close stops background work, flushes the log and releases every file
// handle. Data still sitting in the memtable is not written to an SSTable;
// it stays in the write-ahead log and is replayed by the next Open.
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		return nil
	}

	db.bgCancel()
	db.mu.Lock()
	db.closing = true
	db.flushCond.Broadcast()
	db.mu.Unlock()
	db.bgWG.Wait()

	db.wmu.Lock()
	var err error
	if db.wal != nil {
		err = db.wal.Close()
	}
	db.wmu.Unlock()

	if merr := db.syncManifest(db.currentVersionNoRef()); merr != nil && err == nil {
		err = merr
	}
	db.cache.close()
	return err
}

// -------------------------------------------------------------------------
// background worker
// -------------------------------------------------------------------------

func (db *DB) signalBackground() {
	select {
	case db.bgSignal <- struct{}{}:
	default:
	}
}

func (db *DB) backgroundError() error {
	db.errMu.Lock()
	defer db.errMu.Unlock()
	return db.bgErr
}

func (db *DB) setBackgroundError(err error) {
	db.errMu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.errMu.Unlock()

	// Wake any writer parked waiting for a flush that is never coming.
	db.mu.Lock()
	db.bgFailed = true
	db.flushCond.Broadcast()
	db.mu.Unlock()
}

// backgroundLoop flushes frozen memtables and runs compactions until the
// context is cancelled by Close.
func (db *DB) backgroundLoop(ctx context.Context) {
	defer db.bgWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-db.bgSignal:
		}
		for {
			if ctx.Err() != nil {
				return
			}
			did, err := db.doBackgroundWork(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				db.setBackgroundError(err)
				break
			}
			if !did {
				break
			}
		}
	}
}

func (db *DB) doBackgroundWork(ctx context.Context) (bool, error) {
	db.bgMu.Lock()
	defer db.bgMu.Unlock()

	db.mu.RLock()
	pending := len(db.imm)
	db.mu.RUnlock()
	if pending > 0 {
		return true, db.flushOneMemtable(ctx)
	}

	if db.opts.disableAutoCompaction {
		return false, nil
	}
	v := db.acquireVersion()
	defer db.releaseVersion(v)
	job := db.pickCompaction(v)
	if job == nil {
		return false, nil
	}
	return true, db.runCompaction(ctx, v, job)
}

// flushOneMemtable turns the oldest frozen memtable into a level-0 table.
// Called with bgMu held.
func (db *DB) flushOneMemtable(ctx context.Context) error {
	db.mu.RLock()
	if len(db.imm) == 0 {
		db.mu.RUnlock()
		return nil
	}
	m := db.imm[0]
	db.mu.RUnlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	meta, err := db.writeTableFromMemtable(m)
	if err != nil {
		return err
	}

	v := db.currentVersionNoRef().clone()
	v.levels[0] = append(v.levels[0], meta)
	v.sortLevel(0)
	if err := db.syncManifest(v); err != nil {
		os.Remove(sstPath(db.dir, meta.Number))
		return err
	}
	db.installVersion(v)

	db.mu.Lock()
	if len(db.imm) > 0 && db.imm[0] == m {
		db.imm = append([]*memtable(nil), db.imm[1:]...)
	}
	db.flushCond.Broadcast()
	db.mu.Unlock()

	if m.walNum != 0 {
		os.Remove(walPath(db.dir, m.walNum))
	}
	db.stats.flushes.Add(1)
	db.signalBackground()
	return nil
}

// writeTableFromMemtable serialises a memtable, in internal key order, into
// a fresh SSTable.
func (db *DB) writeTableFromMemtable(m *memtable) (*tableMeta, error) {
	number := db.nextFile.Add(1) - 1
	w, err := newSSTWriter(sstPath(db.dir, number), number, db.opts)
	if err != nil {
		return nil, err
	}
	it := m.NewIter()
	for ok := it.First(); ok; ok = it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			w.Abandon()
			return nil, err
		}
	}
	meta, err := w.Finish()
	if err != nil {
		w.Abandon()
		return nil, err
	}
	db.stats.bytesWritten.Add(meta.Size)
	return meta, nil
}
