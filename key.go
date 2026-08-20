package keelstore

import (
	"bytes"
	"encoding/binary"
)

// Record kinds. The kind lives in the low byte of an internal key's
// trailer, so it participates in ordering: for one sequence number a value
// sorts before a tombstone. In practice a sequence number is used once, so
// the tie never happens outside of tests.
const (
	kindDelete uint8 = 0
	kindValue  uint8 = 1
)

const (
	// trailerLen is the number of bytes appended to a user key to make an
	// internal key: eight big-endian bytes holding seq<<8 | kind.
	trailerLen = 8

	// maxSequence is the largest sequence number a trailer can hold.
	maxSequence = uint64(1)<<56 - 1
)

func makeTrailer(seq uint64, kind uint8) uint64 {
	return seq<<8 | uint64(kind)
}

// appendInternalKey appends userKey||trailer to dst and returns it.
func appendInternalKey(dst, userKey []byte, seq uint64, kind uint8) []byte {
	dst = append(dst, userKey...)
	var t [trailerLen]byte
	binary.BigEndian.PutUint64(t[:], makeTrailer(seq, kind))
	return append(dst, t[:]...)
}

func makeInternalKey(userKey []byte, seq uint64, kind uint8) []byte {
	return appendInternalKey(make([]byte, 0, len(userKey)+trailerLen), userKey, seq, kind)
}

// userKeyOf returns the user portion of an internal key. It aliases ik.
func userKeyOf(ik []byte) []byte {
	if len(ik) < trailerLen {
		return nil
	}
	return ik[:len(ik)-trailerLen]
}

func trailerOf(ik []byte) uint64 {
	if len(ik) < trailerLen {
		return 0
	}
	return binary.BigEndian.Uint64(ik[len(ik)-trailerLen:])
}

func seqOf(ik []byte) uint64          { return trailerOf(ik) >> 8 }
func kindOf(ik []byte) uint8          { return uint8(trailerOf(ik) & 0xff) }
func validInternalKey(ik []byte) bool { return len(ik) > trailerLen }

// compareInternalKeys orders internal keys the way every sorted structure
// in keelstore expects them:
//
//	user key ascending, then sequence number descending.
//
// Descending sequence numbers put the newest version of a key first, so a
// seek for (key, snapshotSeq) lands directly on the newest version that the
// snapshot is allowed to see.
func compareInternalKeys(a, b []byte) int {
	if c := bytes.Compare(userKeyOf(a), userKeyOf(b)); c != 0 {
		return c
	}
	ta, tb := trailerOf(a), trailerOf(b)
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	default:
		return 0
	}
}

// seekKey builds the internal key to seek for when reading userKey at
// sequence number seq: the first position whose sequence number is at most
// seq.
func seekKey(userKey []byte, seq uint64) []byte {
	return makeInternalKey(userKey, seq, kindValue)
}

// firstKeyFor builds the internal key that sorts before every version of
// userKey, used to position range iterators on a lower bound.
func firstKeyFor(userKey []byte) []byte {
	dst := make([]byte, 0, len(userKey)+trailerLen)
	dst = append(dst, userKey...)
	for i := 0; i < trailerLen; i++ {
		dst = append(dst, 0xff)
	}
	return dst
}
