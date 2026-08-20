package keelstore

import "sync/atomic"

// A skiplist is the ordered in-memory index behind every memtable.
//
// Why a skiplist and not a map: the memtable has to be flushed to disk in
// sorted order and has to answer range scans, and a hash map can do
// neither. A skiplist also has a property a balanced tree does not: it can
// be written by one goroutine while other goroutines read it, without any
// locking. Every forward pointer is an atomic word, and a new node is
// linked bottom-up, so a reader either does not see the node yet or sees it
// completely.
//
// The list is single-writer. keelstore serialises writers above it.
const (
	slMaxHeight = 12
	slBranching = 4

	// slNodeOverhead approximates the Go heap cost of a node so that
	// memtable size accounting is not wildly optimistic.
	slNodeOverhead = 64
)

type slNode struct {
	key  []byte
	val  []byte
	next []atomic.Pointer[slNode]
}

func newSLNode(key, val []byte, height int) *slNode {
	return &slNode{key: key, val: val, next: make([]atomic.Pointer[slNode], height)}
}

type skiplist struct {
	head   *slNode
	height atomic.Int32
	bytes  atomic.Int64
	count  atomic.Int64

	// rnd and prev are writer-private scratch state. Only the single
	// writer goroutine touches them.
	rnd  uint32
	prev [slMaxHeight]*slNode
}

func newSkiplist() *skiplist {
	s := &skiplist{
		head: newSLNode(nil, nil, slMaxHeight),
		rnd:  0x9e3779b9,
	}
	s.height.Store(1)
	return s
}

// randomHeight returns a level in [1, slMaxHeight] where each extra level
// is 1/slBranching as likely as the one below it. xorshift keeps this
// allocation-free and out of the global rand mutex.
func (s *skiplist) randomHeight() int {
	s.rnd ^= s.rnd << 13
	s.rnd ^= s.rnd >> 17
	s.rnd ^= s.rnd << 5
	h := 1
	for h < slMaxHeight && s.rnd%slBranching == 0 {
		h++
		s.rnd ^= s.rnd << 7
		s.rnd ^= s.rnd >> 9
	}
	return h
}

// findGE returns the first node whose key is >= key, and optionally records
// the predecessor at every level so that Insert can splice a node in.
func (s *skiplist) findGE(key []byte, prev *[slMaxHeight]*slNode) *slNode {
	x := s.head
	level := int(s.height.Load()) - 1
	for {
		next := x.next[level].Load()
		if next != nil && compareInternalKeys(next.key, key) < 0 {
			x = next
			continue
		}
		if prev != nil {
			prev[level] = x
		}
		if level == 0 {
			return next
		}
		level--
	}
}

// Insert adds key/val. Equal keys are allowed: the newest insertion is
// placed first, so Get and forward iteration observe it before the older
// one. Only one goroutine may call Insert at a time.
func (s *skiplist) Insert(key, val []byte) {
	s.findGE(key, &s.prev)

	h := s.randomHeight()
	if cur := int(s.height.Load()); h > cur {
		for i := cur; i < h; i++ {
			s.prev[i] = s.head
		}
		// Publishing the taller height before the links exist is safe:
		// a reader that descends from a level with a nil pointer simply
		// drops to the level below and still finds every node.
		s.height.Store(int32(h))
	}

	x := newSLNode(key, val, h)
	for i := 0; i < h; i++ {
		x.next[i].Store(s.prev[i].next[i].Load())
		s.prev[i].next[i].Store(x)
	}

	s.bytes.Add(int64(len(key)+len(val)) + slNodeOverhead)
	s.count.Add(1)
}

// Get returns the value stored under an exactly equal key.
func (s *skiplist) Get(key []byte) ([]byte, bool) {
	n := s.findGE(key, nil)
	if n == nil || compareInternalKeys(n.key, key) != 0 {
		return nil, false
	}
	return n.val, true
}

// Bytes is the approximate heap footprint of the list.
func (s *skiplist) Bytes() int64 { return s.bytes.Load() }

// Len is the number of nodes, counting duplicate keys separately.
func (s *skiplist) Len() int64 { return s.count.Load() }

// skiplistIter walks the list in key order. It is a read-only view and is
// safe to use while the writer inserts, but it belongs to one goroutine.
type skiplistIter struct {
	list *skiplist
	node *slNode
}

func (s *skiplist) NewIter() *skiplistIter { return &skiplistIter{list: s} }

func (it *skiplistIter) First() bool {
	it.node = it.list.head.next[0].Load()
	return it.node != nil
}

// Seek positions on the first key >= target.
func (it *skiplistIter) Seek(target []byte) bool {
	it.node = it.list.findGE(target, nil)
	return it.node != nil
}

func (it *skiplistIter) Next() bool {
	if it.node == nil {
		return false
	}
	it.node = it.node.next[0].Load()
	return it.node != nil
}

func (it *skiplistIter) Valid() bool { return it.node != nil }

func (it *skiplistIter) Key() []byte {
	if it.node == nil {
		return nil
	}
	return it.node.key
}

func (it *skiplistIter) Value() []byte {
	if it.node == nil {
		return nil
	}
	return it.node.val
}

func (it *skiplistIter) Error() error { return nil }

func (it *skiplistIter) Close() error {
	it.node = nil
	return nil
}
