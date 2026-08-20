package keelstore

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
)

// An SSTable ("sorted string table") is an immutable file of internal keys
// in ascending order. Once written it is never modified; the only way it
// changes is by being merged into a new file and deleted.
//
// File layout, in write order:
//
//	+---------------------------+
//	| data block 0              |  entries, then a CRC-32C over them
//	| data block 1              |
//	| ...                       |
//	+---------------------------+
//	| Bloom filter block        |  bit count, probe count, bit array
//	+---------------------------+
//	| index block               |  one entry per data block + largest key
//	+---------------------------+
//	| footer (64 bytes, fixed)  |  offsets, counts, CRC, magic
//	+---------------------------+
//
// The index is sparse: it holds only the first key of each block, so a
// point lookup binary-searches the index (in memory) and then scans one
// block (one read). The README carries the byte-level tables.

const (
	sstFooterLen  = 64
	sstMagic      = 0x4C45454B // "KEEL" little endian
	sstBlockTrail = 4          // per-block CRC-32C
)

type indexEntry struct {
	firstKey []byte // first internal key in the block
	offset   uint64
	length   uint64 // includes the block's trailing CRC
}

// tableMeta is everything the version set needs to know about a table
// without opening it.
type tableMeta struct {
	Number   uint64
	Size     int64
	Smallest []byte // smallest internal key
	Largest  []byte // largest internal key
	MinSeq   uint64
	MaxSeq   uint64
	Entries  uint64
}

func (t *tableMeta) smallestUser() []byte { return userKeyOf(t.Smallest) }
func (t *tableMeta) largestUser() []byte  { return userKeyOf(t.Largest) }

// -------------------------------------------------------------------------
// writer
// -------------------------------------------------------------------------

type sstWriter struct {
	path   string
	number uint64
	f      *os.File
	w      *bufio.Writer
	opts   Options

	off      uint64
	block    []byte
	blockKey []byte // first key of the block being built

	index   []indexEntry
	hashes  []uint64
	lastKey []byte

	entries        uint64
	smallest       []byte
	minSeq, maxSeq uint64
	seenAnySeq     bool
	closed         bool
}

func newSSTWriter(path string, number uint64, opts Options) (*sstWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &sstWriter{
		path:   path,
		number: number,
		f:      f,
		w:      bufio.NewWriterSize(f, 64<<10),
		opts:   opts,
		block:  make([]byte, 0, opts.BlockSize+256),
	}, nil
}

// Add appends one entry. Keys must arrive in strictly increasing internal
// key order; every caller inside keelstore feeds it from a sorted source.
func (w *sstWriter) Add(ik, value []byte) error {
	if w.blockKey == nil {
		w.blockKey = append([]byte(nil), ik...)
	}
	if w.smallest == nil {
		w.smallest = append([]byte(nil), ik...)
	}

	var n [binary.MaxVarintLen64]byte
	m := binary.PutUvarint(n[:], uint64(len(ik)))
	w.block = append(w.block, n[:m]...)
	w.block = append(w.block, ik...)
	m = binary.PutUvarint(n[:], uint64(len(value)))
	w.block = append(w.block, n[:m]...)
	w.block = append(w.block, value...)

	uk := userKeyOf(ik)
	if len(w.hashes) == 0 || !equalBytes(userKeyOf(w.lastKey), uk) {
		w.hashes = append(w.hashes, bloomHash(uk))
	}
	w.lastKey = append(w.lastKey[:0], ik...)

	seq := seqOf(ik)
	if !w.seenAnySeq {
		w.minSeq, w.maxSeq, w.seenAnySeq = seq, seq, true
	} else {
		if seq < w.minSeq {
			w.minSeq = seq
		}
		if seq > w.maxSeq {
			w.maxSeq = seq
		}
	}
	w.entries++

	if len(w.block) >= w.opts.BlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *sstWriter) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.Checksum(w.block, crcTable))
	if _, err := w.w.Write(w.block); err != nil {
		return err
	}
	if _, err := w.w.Write(crc[:]); err != nil {
		return err
	}
	length := uint64(len(w.block) + sstBlockTrail)
	w.index = append(w.index, indexEntry{firstKey: w.blockKey, offset: w.off, length: length})
	w.off += length
	w.block = w.block[:0]
	w.blockKey = nil
	return nil
}

// EstimatedSize is the number of bytes written so far, used by compaction
// to decide when to roll over to a new output file.
func (w *sstWriter) EstimatedSize() int64 { return int64(w.off) + int64(len(w.block)) }

func (w *sstWriter) Empty() bool { return w.entries == 0 }

// Finish writes the Bloom filter, the index and the footer, fsyncs and
// closes the file, and returns the table's metadata.
func (w *sstWriter) Finish() (*tableMeta, error) {
	if err := w.flushBlock(); err != nil {
		return nil, err
	}

	bloomOff := w.off
	bloomBuf := buildBloom(w.hashes, w.opts.BloomBitsPerKey).Encode()
	if len(bloomBuf) > 0 {
		if _, err := w.w.Write(bloomBuf); err != nil {
			return nil, err
		}
		w.off += uint64(len(bloomBuf))
	}

	indexOff := w.off
	indexBuf := w.encodeIndex()
	if _, err := w.w.Write(indexBuf); err != nil {
		return nil, err
	}
	w.off += uint64(len(indexBuf))

	footer := make([]byte, sstFooterLen)
	binary.LittleEndian.PutUint64(footer[0:8], indexOff)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(indexBuf)))
	binary.LittleEndian.PutUint64(footer[16:24], bloomOff)
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(bloomBuf)))
	binary.LittleEndian.PutUint64(footer[32:40], w.entries)
	binary.LittleEndian.PutUint64(footer[40:48], w.minSeq)
	binary.LittleEndian.PutUint64(footer[48:56], w.maxSeq)
	binary.LittleEndian.PutUint32(footer[56:60], crc32.Checksum(footer[:56], crcTable))
	binary.LittleEndian.PutUint32(footer[60:64], sstMagic)
	if _, err := w.w.Write(footer); err != nil {
		return nil, err
	}
	w.off += sstFooterLen

	if err := w.w.Flush(); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}
	w.closed = true

	return &tableMeta{
		Number:   w.number,
		Size:     int64(w.off),
		Smallest: w.smallest,
		Largest:  append([]byte(nil), w.lastKey...),
		MinSeq:   w.minSeq,
		MaxSeq:   w.maxSeq,
		Entries:  w.entries,
	}, nil
}

// encodeIndex lays out the index block:
//
//	uvarint  number of blocks
//	repeat:  uvarint keyLen | first internal key | uvarint offset | uvarint length
//	uvarint  largest key length
//	         largest internal key in the file
//	4 bytes  CRC-32C over everything above
func (w *sstWriter) encodeIndex() []byte {
	buf := make([]byte, 0, 64+len(w.index)*48)
	var n [binary.MaxVarintLen64]byte

	m := binary.PutUvarint(n[:], uint64(len(w.index)))
	buf = append(buf, n[:m]...)
	for _, e := range w.index {
		m = binary.PutUvarint(n[:], uint64(len(e.firstKey)))
		buf = append(buf, n[:m]...)
		buf = append(buf, e.firstKey...)
		m = binary.PutUvarint(n[:], e.offset)
		buf = append(buf, n[:m]...)
		m = binary.PutUvarint(n[:], e.length)
		buf = append(buf, n[:m]...)
	}
	m = binary.PutUvarint(n[:], uint64(len(w.lastKey)))
	buf = append(buf, n[:m]...)
	buf = append(buf, w.lastKey...)

	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.Checksum(buf, crcTable))
	return append(buf, crc[:]...)
}

// Abandon closes and removes a half-written file, used when a compaction is
// cancelled.
func (w *sstWriter) Abandon() {
	if !w.closed {
		w.f.Close()
		w.closed = true
	}
	os.Remove(w.path)
}

// -------------------------------------------------------------------------
// reader
// -------------------------------------------------------------------------

type sstReader struct {
	f      *os.File
	number uint64
	size   int64
	stats  *counters // optional; nil when a table is opened outside a DB

	index   []indexEntry
	largest []byte
	bloom   *bloomFilter
	entries uint64
	minSeq  uint64
	maxSeq  uint64
}

func openSSTable(path string, number uint64) (*sstReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r := &sstReader{f: f, number: number, size: st.Size()}
	if err := r.readFooter(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *sstReader) readFooter() error {
	if r.size < sstFooterLen {
		return ErrCorrupt
	}
	footer := make([]byte, sstFooterLen)
	if _, err := r.f.ReadAt(footer, r.size-sstFooterLen); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(footer[60:64]) != sstMagic {
		return ErrCorrupt
	}
	if binary.LittleEndian.Uint32(footer[56:60]) != crc32.Checksum(footer[:56], crcTable) {
		return ErrCorrupt
	}

	indexOff := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	bloomOff := binary.LittleEndian.Uint64(footer[16:24])
	bloomLen := binary.LittleEndian.Uint64(footer[24:32])
	r.entries = binary.LittleEndian.Uint64(footer[32:40])
	r.minSeq = binary.LittleEndian.Uint64(footer[40:48])
	r.maxSeq = binary.LittleEndian.Uint64(footer[48:56])

	limit := uint64(r.size - sstFooterLen)
	if indexOff+indexLen > limit || bloomOff+bloomLen > limit {
		return ErrCorrupt
	}

	if bloomLen > 0 {
		buf := make([]byte, bloomLen)
		if _, err := r.f.ReadAt(buf, int64(bloomOff)); err != nil {
			return err
		}
		bf, err := decodeBloom(buf)
		if err != nil {
			return err
		}
		r.bloom = bf
	}

	buf := make([]byte, indexLen)
	if _, err := r.f.ReadAt(buf, int64(indexOff)); err != nil {
		return err
	}
	return r.decodeIndex(buf)
}

func (r *sstReader) decodeIndex(buf []byte) error {
	if len(buf) < 4 {
		return ErrCorrupt
	}
	body := buf[:len(buf)-4]
	want := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32.Checksum(body, crcTable) != want {
		return ErrCorrupt
	}

	count, n := binary.Uvarint(body)
	if n <= 0 {
		return ErrCorrupt
	}
	body = body[n:]

	r.index = make([]indexEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		klen, n := binary.Uvarint(body)
		if n <= 0 || uint64(len(body)-n) < klen {
			return ErrCorrupt
		}
		body = body[n:]
		key := body[:klen]
		body = body[klen:]

		off, n := binary.Uvarint(body)
		if n <= 0 {
			return ErrCorrupt
		}
		body = body[n:]
		length, n := binary.Uvarint(body)
		if n <= 0 {
			return ErrCorrupt
		}
		body = body[n:]
		r.index = append(r.index, indexEntry{firstKey: key, offset: off, length: length})
	}

	klen, n := binary.Uvarint(body)
	if n <= 0 || uint64(len(body)-n) < klen {
		return ErrCorrupt
	}
	body = body[n:]
	r.largest = body[:klen]
	if len(body[klen:]) != 0 {
		return ErrCorrupt
	}
	return nil
}

func (r *sstReader) Close() error { return r.f.Close() }

// mayContain consults the Bloom filter. A false answer means the key is
// definitely absent from this table.
func (r *sstReader) mayContain(userKey []byte) bool {
	if r.bloom == nil {
		return true
	}
	return r.bloom.MayContain(bloomHash(userKey))
}

// findBlock returns the index of the last block whose first key is <= ik,
// or 0 when ik sorts before everything in the file.
func (r *sstReader) findBlock(ik []byte) int {
	lo, hi, res := 0, len(r.index)-1, 0
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		if compareInternalKeys(r.index[mid].firstKey, ik) <= 0 {
			res = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return res
}

func (r *sstReader) readBlock(i int) ([]byte, error) {
	e := r.index[i]
	if e.length < sstBlockTrail {
		return nil, ErrCorrupt
	}
	buf := make([]byte, e.length)
	if _, err := r.f.ReadAt(buf, int64(e.offset)); err != nil {
		return nil, err
	}
	body := buf[:len(buf)-sstBlockTrail]
	want := binary.LittleEndian.Uint32(buf[len(buf)-sstBlockTrail:])
	if crc32.Checksum(body, crcTable) != want {
		return nil, ErrCorrupt
	}
	if r.stats != nil {
		r.stats.bytesRead.Add(int64(len(buf)))
	}
	return body, nil
}

// Get returns the newest version of userKey at or below seq that this table
// holds. found is false when the table has nothing for the key; filtered is
// true when the Bloom filter answered without any disk read.
func (r *sstReader) Get(userKey []byte, seq uint64) (value []byte, kind uint8, found, filtered bool, err error) {
	if !r.mayContain(userKey) {
		return nil, 0, false, true, nil
	}
	it := r.NewIter()
	defer it.Close()
	if !it.Seek(seekKey(userKey, seq)) {
		return nil, 0, false, false, it.Error()
	}
	if !equalBytes(userKeyOf(it.Key()), userKey) {
		return nil, 0, false, false, nil
	}
	return it.Value(), kindOf(it.Key()), true, false, nil
}

// -------------------------------------------------------------------------
// reader iterator
// -------------------------------------------------------------------------

type sstIter struct {
	r        *sstReader
	blockIdx int
	block    []byte
	off      int
	key, val []byte
	valid    bool
	err      error
}

func (r *sstReader) NewIter() *sstIter {
	return &sstIter{r: r, blockIdx: -1}
}

func (it *sstIter) loadBlock(i int) bool {
	if i < 0 || i >= len(it.r.index) {
		it.valid = false
		return false
	}
	if it.blockIdx == i && it.block != nil {
		it.off = 0
		return true
	}
	b, err := it.r.readBlock(i)
	if err != nil {
		it.err, it.valid = err, false
		return false
	}
	it.block, it.blockIdx, it.off = b, i, 0
	return true
}

// decodeAt decodes the entry at the current offset of the loaded block.
func (it *sstIter) decodeAt() bool {
	if it.off >= len(it.block) {
		return false
	}
	buf := it.block[it.off:]
	klen, n := binary.Uvarint(buf)
	if n <= 0 || uint64(len(buf)-n) < klen {
		it.err, it.valid = ErrCorrupt, false
		return false
	}
	buf = buf[n:]
	key := buf[:klen]
	buf = buf[klen:]

	vlen, n := binary.Uvarint(buf)
	if n <= 0 || uint64(len(buf)-n) < vlen {
		it.err, it.valid = ErrCorrupt, false
		return false
	}
	buf = buf[n:]
	val := buf[:vlen]

	consumed := len(it.block[it.off:]) - len(buf[vlen:])
	it.off += consumed
	it.key, it.val, it.valid = key, val, true
	return true
}

func (it *sstIter) First() bool {
	if len(it.r.index) == 0 {
		it.valid = false
		return false
	}
	if !it.loadBlock(0) {
		return false
	}
	return it.decodeAt()
}

// Seek positions on the first entry whose internal key is >= target.
func (it *sstIter) Seek(target []byte) bool {
	if len(it.r.index) == 0 {
		it.valid = false
		return false
	}
	for i := it.r.findBlock(target); i < len(it.r.index); i++ {
		if !it.loadBlock(i) {
			return false
		}
		for it.decodeAt() {
			if compareInternalKeys(it.key, target) >= 0 {
				return true
			}
		}
		if it.err != nil {
			return false
		}
	}
	it.valid = false
	return false
}

func (it *sstIter) Next() bool {
	if !it.valid {
		return false
	}
	if it.decodeAt() {
		return true
	}
	if it.err != nil {
		return false
	}
	for i := it.blockIdx + 1; i < len(it.r.index); i++ {
		if !it.loadBlock(i) {
			return false
		}
		if it.decodeAt() {
			return true
		}
		if it.err != nil {
			return false
		}
	}
	it.valid = false
	return false
}

func (it *sstIter) Valid() bool   { return it.valid }
func (it *sstIter) Key() []byte   { return it.key }
func (it *sstIter) Value() []byte { return it.val }
func (it *sstIter) Error() error  { return it.err }

func (it *sstIter) Close() error {
	it.block, it.key, it.val, it.valid = nil, nil, nil, false
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
