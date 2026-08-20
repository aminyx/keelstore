package keelstore

import "bytes"

// A memtable is a skiplist of internal keys plus the number of the
// write-ahead log that backs it. Until the memtable has been flushed into
// an SSTable, that log file is the only durable copy of its contents.
type memtable struct {
	list   *skiplist
	walNum uint64
}

func newMemtable(walNum uint64) *memtable {
	return &memtable{list: newSkiplist(), walNum: walNum}
}

// Add records one mutation. Callers must supply strictly increasing
// sequence numbers; keelstore's single writer guarantees that.
func (m *memtable) Add(userKey, value []byte, seq uint64, kind uint8) {
	m.list.Insert(makeInternalKey(userKey, seq, kind), value)
}

// Get returns the newest version of userKey with a sequence number at or
// below seq. found is false when the memtable knows nothing about the key;
// when found is true the caller must still check kind for a tombstone.
func (m *memtable) Get(userKey []byte, seq uint64) (value []byte, kind uint8, found bool) {
	n := m.list.findGE(seekKey(userKey, seq), nil)
	if n == nil || !bytes.Equal(userKeyOf(n.key), userKey) {
		return nil, 0, false
	}
	return n.val, kindOf(n.key), true
}

func (m *memtable) ApproxBytes() int64 { return m.list.Bytes() }
func (m *memtable) Entries() int64     { return m.list.Len() }
func (m *memtable) Empty() bool        { return m.list.Len() == 0 }

func (m *memtable) NewIter() *skiplistIter { return m.list.NewIter() }
