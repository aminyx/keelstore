package keelstore

import (
	"bytes"
	"container/heap"
)

// internalIterator is the shape every level of the tree exposes: a forward
// cursor over internal keys in sorted order. The memtable's skiplist and an
// SSTable's block reader both satisfy it, which is what lets one merge
// routine serve reads, range scans and compaction alike.
type internalIterator interface {
	First() bool
	Seek(target []byte) bool
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

var (
	_ internalIterator = (*skiplistIter)(nil)
	_ internalIterator = (*sstIter)(nil)
)

// -------------------------------------------------------------------------
// k-way merge
// -------------------------------------------------------------------------

type mergeHeap []internalIterator

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	return compareInternalKeys(h[i].Key(), h[j].Key()) < 0
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(internalIterator)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// mergeIter merges several sorted sources into one sorted stream. Because
// internal keys sort by user key ascending and sequence number descending,
// the merged stream presents the newest version of each key first, which is
// exactly what both the read path and compaction want.
type mergeIter struct {
	h       mergeHeap
	sources []internalIterator
	started bool
	err     error
}

func newMergeIter(sources []internalIterator) *mergeIter {
	return &mergeIter{sources: sources}
}

func (m *mergeIter) init(seed func(internalIterator) bool) bool {
	m.started = true
	m.h = m.h[:0]
	for _, it := range m.sources {
		if seed(it) {
			m.h = append(m.h, it)
		} else if err := it.Error(); err != nil && m.err == nil {
			m.err = err
		}
	}
	heap.Init(&m.h)
	return len(m.h) > 0
}

func (m *mergeIter) First() bool {
	return m.init(func(it internalIterator) bool { return it.First() })
}

func (m *mergeIter) Seek(target []byte) bool {
	return m.init(func(it internalIterator) bool { return it.Seek(target) })
}

func (m *mergeIter) Next() bool {
	if !m.started {
		return m.First()
	}
	if len(m.h) == 0 {
		return false
	}
	it := m.h[0]
	if it.Next() {
		heap.Fix(&m.h, 0)
	} else {
		if err := it.Error(); err != nil && m.err == nil {
			m.err = err
		}
		heap.Pop(&m.h)
	}
	return len(m.h) > 0
}

func (m *mergeIter) Key() []byte {
	if len(m.h) == 0 {
		return nil
	}
	return m.h[0].Key()
}

func (m *mergeIter) Value() []byte {
	if len(m.h) == 0 {
		return nil
	}
	return m.h[0].Value()
}

func (m *mergeIter) Error() error {
	if m.err != nil {
		return m.err
	}
	for _, it := range m.sources {
		if err := it.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (m *mergeIter) Close() error {
	var first error
	for _, it := range m.sources {
		if err := it.Close(); err != nil && first == nil {
			first = err
		}
	}
	m.h = nil
	return first
}

// -------------------------------------------------------------------------
// public iterator
// -------------------------------------------------------------------------

// IterOptions bounds an iteration to the half-open user key range
// [Start, End). A nil bound is unbounded on that side.
type IterOptions struct {
	Start []byte
	End   []byte
}

// An Iterator walks user keys in ascending order, merging the memtable, any
// frozen memtables and every SSTable into a single stream. It shows exactly
// one version of each key — the newest one visible at its sequence number —
// and hides keys whose newest visible version is a tombstone.
//
// An Iterator is not safe for concurrent use, but any number of iterators
// may run at once, including while the writer is writing. It pins the
// SSTables it was built from, so it must be closed.
type Iterator struct {
	db    *DB
	ver   *version
	merge *mergeIter
	seq   uint64
	end   []byte

	key    []byte
	value  []byte
	last   []byte
	err    error
	closed bool
	first  bool
}

// NewIterator returns an iterator over the database as of now. Passing nil
// options iterates the whole key space.
func (db *DB) NewIterator(opts *IterOptions) *Iterator {
	return db.newIterator(db.visible.Load(), opts)
}

// NewIterator returns an iterator over the snapshot's view.
func (s *Snapshot) NewIterator(opts *IterOptions) *Iterator {
	if s.released.Load() {
		return &Iterator{err: ErrSnapshotReleased, closed: true}
	}
	return s.db.newIterator(s.seq, opts)
}

func (db *DB) newIterator(seq uint64, opts *IterOptions) *Iterator {
	if db.closed.Load() {
		return &Iterator{err: ErrClosed, closed: true}
	}
	var o IterOptions
	if opts != nil {
		o = *opts
	}

	db.mu.RLock()
	mem, imm, v := db.mem, db.imm, db.cur
	v.ref()
	db.mu.RUnlock()

	it := &Iterator{db: db, ver: v, seq: seq, first: true}
	if o.End != nil {
		it.end = append([]byte(nil), o.End...)
	}

	sources := make([]internalIterator, 0, 4+v.numFiles())
	sources = append(sources, mem.NewIter())
	for i := len(imm) - 1; i >= 0; i-- {
		sources = append(sources, imm[i].NewIter())
	}
levels:
	for _, level := range v.levels {
		for _, t := range level {
			r, err := db.cache.get(t.Number)
			if err != nil {
				it.err = err
				break levels
			}
			sources = append(sources, r.NewIter())
		}
	}
	it.merge = newMergeIter(sources)

	if o.Start != nil {
		it.merge.Seek(firstKeyFor(o.Start))
	} else {
		it.merge.First()
	}
	return it
}

// Next advances to the next key and reports whether one is available. It
// must be called before the first read, in the style of sql.Rows:
//
//	for it.Next() { use(it.Key(), it.Value()) }
func (it *Iterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for {
		if it.first {
			it.first = false
			if it.merge.Key() == nil {
				return it.finish()
			}
		} else if !it.merge.Next() {
			return it.finish()
		}

		ik := it.merge.Key()
		if ik == nil {
			return it.finish()
		}
		uk := userKeyOf(ik)

		// Versions the snapshot cannot see do not count as "the newest
		// version" of anything, so they must not mask an older one.
		if seqOf(ik) > it.seq {
			continue
		}
		if it.last != nil && bytes.Equal(uk, it.last) {
			continue // an older version of a key already emitted or deleted
		}
		it.last = append(it.last[:0], uk...)

		if it.end != nil && bytes.Compare(uk, it.end) >= 0 {
			return it.finish()
		}
		if kindOf(ik) == kindDelete {
			continue // tombstone: hide this key and everything under it
		}

		it.key = append(it.key[:0], uk...)
		it.value = append(it.value[:0], it.merge.Value()...)
		return true
	}
}

func (it *Iterator) finish() bool {
	if err := it.merge.Error(); err != nil && it.err == nil {
		it.err = err
	}
	it.key, it.value = nil, nil
	return false
}

// Key is the current key. It is only valid until the next call to Next.
func (it *Iterator) Key() []byte { return it.key }

// Value is the current value. It is only valid until the next call to Next.
func (it *Iterator) Value() []byte { return it.value }

// Error reports the first error the iteration hit, if any.
func (it *Iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.merge != nil {
		return it.merge.Error()
	}
	return nil
}

// Close releases the SSTables the iterator pinned. Every iterator must be
// closed, or compacted-away files will never be deleted.
func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	var err error
	if it.merge != nil {
		err = it.merge.Close()
	}
	if it.ver != nil && it.db != nil {
		it.db.releaseVersion(it.ver)
		it.ver = nil
	}
	it.key, it.value = nil, nil
	return err
}
