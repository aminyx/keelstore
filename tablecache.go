package keelstore

import (
	"sync"
)

// tableCache keeps one open sstReader per live SSTable. Reopening a file
// per lookup would dominate read latency, and on Windows an open handle
// also blocks deletion, so eviction has to happen before a file is removed.
type tableCache struct {
	dir   string
	stats *counters

	mu     sync.Mutex
	open   map[uint64]*sstReader
	closed bool
}

func newTableCache(dir string, stats *counters) *tableCache {
	return &tableCache{dir: dir, stats: stats, open: make(map[uint64]*sstReader)}
}

func (c *tableCache) get(number uint64) (*sstReader, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if r, ok := c.open[number]; ok {
		c.mu.Unlock()
		return r, nil
	}
	c.mu.Unlock()

	r, err := openSSTable(sstPath(c.dir, number), number)
	if err != nil {
		return nil, err
	}
	r.stats = c.stats

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		r.Close()
		return nil, ErrClosed
	}
	// Another goroutine may have won the race; keep one reader per file.
	if existing, ok := c.open[number]; ok {
		r.Close()
		return existing, nil
	}
	c.open[number] = r
	return r, nil
}

// evict closes the reader for a table that is about to be deleted.
func (c *tableCache) evict(number uint64) {
	c.mu.Lock()
	r, ok := c.open[number]
	delete(c.open, number)
	c.mu.Unlock()
	if ok {
		r.Close()
	}
}

func (c *tableCache) close() {
	c.mu.Lock()
	readers := make([]*sstReader, 0, len(c.open))
	for _, r := range c.open {
		readers = append(readers, r)
	}
	c.open = make(map[uint64]*sstReader)
	c.closed = true
	c.mu.Unlock()
	for _, r := range readers {
		r.Close()
	}
}
