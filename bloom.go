package keelstore

import (
	"encoding/binary"
	"math"
)

// A Bloom filter is what makes an LSM tree tolerable to read from. Without
// one, a lookup for a key that does not exist has to touch every SSTable in
// the tree. With one, each table answers "definitely not here" from a few
// hundred bytes of memory and the disk is never touched.
//
// The filter is a bit array of m bits and k hash functions. keelstore
// computes a single 64-bit hash per key and derives the k probes from it by
// double hashing (Kirsch-Mitzenmacher): h_i = h1 + i*h2. The technique costs
// one hash instead of k and, for the sizes used here, has the same false
// positive behaviour as k independent hashes.

const bloomHeaderLen = 5 // uint32 bit count + uint8 probe count

// bloomHash hashes a key to 64 bits: FNV-1a for the mixing loop, then the
// splitmix64 finaliser to spread the entropy across all 64 bits. FNV alone
// leaves short keys clustered in the low bits, which shows up as a
// measurably worse false positive rate.
func bloomHash(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range key {
		h ^= uint64(c)
		h *= prime64
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

type bloomFilter struct {
	bits   []byte
	nbits  uint32
	probes uint8
}

// bloomProbes converts a bits-per-key budget into the optimal number of
// probes, k = (m/n) * ln2.
func bloomProbes(bitsPerKey int) uint8 {
	k := int(float64(bitsPerKey)*0.69314718056 + 0.5)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return uint8(k)
}

// buildBloom builds a filter over the given key hashes. Passing the hashes
// rather than the keys lets the SSTable writer hash each user key once,
// while it is already holding it.
func buildBloom(hashes []uint64, bitsPerKey int) *bloomFilter {
	if bitsPerKey <= 0 {
		return nil
	}
	n := len(hashes)
	nbits := n * bitsPerKey
	if nbits < 64 {
		nbits = 64 // tiny filters are all false positives; round up
	}
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8

	f := &bloomFilter{
		bits:   make([]byte, nbytes),
		nbits:  uint32(nbits),
		probes: bloomProbes(bitsPerKey),
	}
	for _, h := range hashes {
		f.add(h)
	}
	return f
}

func (f *bloomFilter) add(h uint64) {
	h1 := uint32(h)
	// The second hash must be odd so that repeated addition walks the
	// whole bit array instead of a short cycle when nbits is a power of 2.
	h2 := uint32(h>>32) | 1
	for i := uint8(0); i < f.probes; i++ {
		bit := h1 % f.nbits
		f.bits[bit/8] |= 1 << (bit % 8)
		h1 += h2
	}
}

// MayContain reports whether the key might be present. False is definitive:
// a key that was added always returns true, so there are never false
// negatives. True may be wrong with probability roughly (1-e^(-kn/m))^k.
func (f *bloomFilter) MayContain(h uint64) bool {
	if f == nil || f.nbits == 0 {
		return true
	}
	h1 := uint32(h)
	h2 := uint32(h>>32) | 1
	for i := uint8(0); i < f.probes; i++ {
		bit := h1 % f.nbits
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		h1 += h2
	}
	return true
}

// Encode serialises the filter into the layout stored in an SSTable:
//
//	offset  size            field
//	0       4               bit count      uint32 little endian
//	4       1               probe count    uint8
//	5       ceil(nbits/8)   bit array
func (f *bloomFilter) Encode() []byte {
	if f == nil {
		return nil
	}
	buf := make([]byte, bloomHeaderLen+len(f.bits))
	binary.LittleEndian.PutUint32(buf[0:4], f.nbits)
	buf[4] = f.probes
	copy(buf[bloomHeaderLen:], f.bits)
	return buf
}

func decodeBloom(buf []byte) (*bloomFilter, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	if len(buf) < bloomHeaderLen {
		return nil, ErrCorrupt
	}
	nbits := binary.LittleEndian.Uint32(buf[0:4])
	probes := buf[4]
	bits := buf[bloomHeaderLen:]
	if nbits == 0 || probes == 0 || int(nbits+7)/8 != len(bits) {
		return nil, ErrCorrupt
	}
	return &bloomFilter{bits: bits, nbits: nbits, probes: probes}, nil
}

// bloomFalsePositiveRate is the textbook estimate (1-e^(-kn/m))^k, used by
// the tests to check that the implementation lands near theory.
func bloomFalsePositiveRate(n int, bitsPerKey int) float64 {
	if n == 0 || bitsPerKey <= 0 {
		return 1
	}
	k := float64(bloomProbes(bitsPerKey))
	m := float64(n * bitsPerKey)
	return math.Pow(1-math.Exp(-k*float64(n)/m), k)
}
