package keelstore

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// A Bloom filter is only allowed to be wrong in one direction. This test is
// the property that matters: everything that was added must be reported
// present, for every filter size, over thousands of keys.
func TestBloomNeverReportsAFalseNegative(t *testing.T) {
	for _, bitsPerKey := range []int{1, 4, 10, 16, 24} {
		for _, n := range []int{1, 10, 1000, 20000} {
			hashes := make([]uint64, n)
			keys := make([][]byte, n)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("member-%d-%d", bitsPerKey, i))
				hashes[i] = bloomHash(keys[i])
			}
			f := buildBloom(hashes, bitsPerKey)
			for i, k := range keys {
				if !f.MayContain(bloomHash(k)) {
					t.Fatalf("false negative: bits=%d n=%d key %d (%q)", bitsPerKey, n, i, k)
				}
			}
		}
	}
}

func TestBloomFalsePositiveRateIsNearTheory(t *testing.T) {
	const n = 20000
	for _, bitsPerKey := range []int{6, 10, 16} {
		hashes := make([]uint64, n)
		for i := range hashes {
			hashes[i] = bloomHash([]byte(fmt.Sprintf("present-%d", i)))
		}
		f := buildBloom(hashes, bitsPerKey)

		hits := 0
		const probes = 100000
		for i := 0; i < probes; i++ {
			if f.MayContain(bloomHash([]byte(fmt.Sprintf("absent-%d", i)))) {
				hits++
			}
		}
		measured := float64(hits) / probes
		theory := bloomFalsePositiveRate(n, bitsPerKey)

		// Double hashing and a finite bit array will not hit theory
		// exactly; a factor of two either way is the usual tolerance.
		if measured > theory*2+0.005 {
			t.Errorf("bits=%d: measured fp rate %.4f, theory %.4f (too high)", bitsPerKey, measured, theory)
		}
		t.Logf("bits/key=%2d measured fp=%.4f theory=%.4f", bitsPerKey, measured, theory)
	}
}

func TestBloomProbeCountMatchesOptimum(t *testing.T) {
	cases := map[int]uint8{1: 1, 2: 1, 10: 7, 16: 11, 100: 30}
	for bits, want := range cases {
		if got := bloomProbes(bits); got != want {
			t.Errorf("bloomProbes(%d) = %d, want %d", bits, got, want)
		}
	}
}

func TestBloomEncodeDecodeRoundTrip(t *testing.T) {
	hashes := make([]uint64, 500)
	for i := range hashes {
		hashes[i] = bloomHash([]byte(fmt.Sprintf("k%d", i)))
	}
	f := buildBloom(hashes, 10)

	decoded, err := decodeBloom(f.Encode())
	if err != nil {
		t.Fatalf("decodeBloom: %v", err)
	}
	if decoded.nbits != f.nbits || decoded.probes != f.probes {
		t.Fatalf("header changed: %d/%d became %d/%d", f.nbits, f.probes, decoded.nbits, decoded.probes)
	}
	for i := range hashes {
		if !decoded.MayContain(hashes[i]) {
			t.Fatalf("decoded filter lost key %d", i)
		}
	}
}

func TestBloomDecodeRejectsTruncatedAndInconsistentBuffers(t *testing.T) {
	f := buildBloom([]uint64{1, 2, 3}, 10)
	enc := f.Encode()

	if _, err := decodeBloom(enc[:3]); err == nil {
		t.Error("a buffer shorter than the header was accepted")
	}
	if _, err := decodeBloom(enc[:len(enc)-1]); err == nil {
		t.Error("a bit array shorter than its declared bit count was accepted")
	}
	if got, err := decodeBloom(nil); got != nil || err != nil {
		t.Errorf("decodeBloom(nil) = %v/%v, want nil/nil (filters are optional)", got, err)
	}
}

func TestBloomDisabledByZeroBitsPerKey(t *testing.T) {
	if f := buildBloom([]uint64{1, 2, 3}, 0); f != nil {
		t.Fatal("zero bits per key should build no filter at all")
	}
	var nilFilter *bloomFilter
	if !nilFilter.MayContain(123) {
		t.Fatal("a missing filter must answer \"maybe\", never \"no\"")
	}
	if nilFilter.Encode() != nil {
		t.Fatal("a missing filter should encode to nothing")
	}
}

func TestBloomTinyInputStillGetsAUsableFilter(t *testing.T) {
	f := buildBloom([]uint64{bloomHash([]byte("only"))}, 10)
	if f.nbits < 64 {
		t.Fatalf("filter rounded down to %d bits; tiny filters must be padded", f.nbits)
	}
	if !f.MayContain(bloomHash([]byte("only"))) {
		t.Fatal("false negative on a one-key filter")
	}
}

func TestBloomHashSpreadsBitsAcrossTheWord(t *testing.T) {
	// A weak hash clusters its entropy and quietly ruins the filter, so
	// check that every bit position varies across a sample of keys.
	var ones, zeros uint64
	for i := 0; i < 4096; i++ {
		h := bloomHash([]byte(fmt.Sprintf("k%d", i)))
		ones |= h
		zeros |= ^h
	}
	if ones != math.MaxUint64 || zeros != math.MaxUint64 {
		t.Fatalf("hash never varies some bits: ones=%016x zeros=%016x", ones, zeros)
	}
}

func TestBloomHandlesRandomBinaryKeys(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	keys := make([][]byte, 5000)
	hashes := make([]uint64, len(keys))
	for i := range keys {
		b := make([]byte, 1+rnd.Intn(64))
		rnd.Read(b)
		keys[i] = b
		hashes[i] = bloomHash(b)
	}
	f := buildBloom(hashes, 10)
	for i, k := range keys {
		if !f.MayContain(bloomHash(k)) {
			t.Fatalf("false negative on random key %d", i)
		}
	}
}
