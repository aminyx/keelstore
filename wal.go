package keelstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// The write-ahead log is the durability contract. A mutation is only
// acknowledged after its record has reached the log; everything else in the
// engine — memtable, SSTables, levels — can be rebuilt from it.
//
// Record framing:
//
//	offset  size    field
//	0       4       CRC-32C of the payload, little endian
//	4       4       payload length in bytes, little endian
//	8       length  payload
//
// Payload framing (one mutation):
//
//	offset  size     field
//	0       1        kind: 0 tombstone, 1 value
//	1       8        sequence number, big endian
//	9       varint   key length
//	..      keyLen   key bytes
//	..      varint   value length
//	..      valLen   value bytes
//
// A record whose header runs past the end of the file, whose length points
// past the end of the file, or whose payload fails the checksum is treated
// as a torn tail: the writer died in the middle of it. Replay stops there
// and everything from that point on is discarded.

const walHeaderLen = 8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type walWriter struct {
	f    *os.File
	w    *bufio.Writer
	size int64
	hdr  [walHeaderLen]byte

	// tornTail, when positive, truncates the next record to that many
	// bytes. Fault injection for the crash-recovery test; never set in
	// production because Options.tornTailBytes is unexported.
	tornTail int
}

func newWALWriter(path string) (*walWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &walWriter{f: f, w: bufio.NewWriterSize(f, 64<<10), size: st.Size()}, nil
}

// Append frames and writes one payload.
func (w *walWriter) Append(payload []byte) error {
	binary.LittleEndian.PutUint32(w.hdr[0:4], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(w.hdr[4:8], uint32(len(payload)))

	if w.tornTail > 0 {
		// Simulate a write that the machine only partially persisted.
		rec := append(append([]byte{}, w.hdr[:]...), payload...)
		n := w.tornTail
		if n > len(rec) {
			n = len(rec)
		}
		w.tornTail = 0
		if _, err := w.w.Write(rec[:n]); err != nil {
			return err
		}
		w.size += int64(n)
		return w.w.Flush()
	}

	if _, err := w.w.Write(w.hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(payload); err != nil {
		return err
	}
	w.size += int64(walHeaderLen + len(payload))
	// Flushing the bufio layer on every append hands the bytes to the
	// kernel, which is what makes a write survive a process crash. fsync
	// (Sync below) is what makes it survive a power cut.
	return w.w.Flush()
}

func (w *walWriter) Sync() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *walWriter) Size() int64 { return w.size }

func (w *walWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// encodeWALPayload appends one mutation's payload to dst.
func encodeWALPayload(dst []byte, kind uint8, seq uint64, key, value []byte) []byte {
	dst = append(dst, kind)
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], seq)
	dst = append(dst, s[:]...)
	var n [binary.MaxVarintLen64]byte
	m := binary.PutUvarint(n[:], uint64(len(key)))
	dst = append(dst, n[:m]...)
	dst = append(dst, key...)
	m = binary.PutUvarint(n[:], uint64(len(value)))
	dst = append(dst, n[:m]...)
	dst = append(dst, value...)
	return dst
}

func decodeWALPayload(p []byte) (kind uint8, seq uint64, key, value []byte, err error) {
	if len(p) < 9 {
		return 0, 0, nil, nil, ErrCorrupt
	}
	kind = p[0]
	if kind != kindValue && kind != kindDelete {
		return 0, 0, nil, nil, ErrCorrupt
	}
	seq = binary.BigEndian.Uint64(p[1:9])
	rest := p[9:]

	klen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < klen {
		return 0, 0, nil, nil, ErrCorrupt
	}
	rest = rest[n:]
	key = rest[:klen]
	rest = rest[klen:]

	vlen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < vlen {
		return 0, 0, nil, nil, ErrCorrupt
	}
	rest = rest[n:]
	value = rest[:vlen]
	if len(rest[vlen:]) != 0 {
		return 0, 0, nil, nil, ErrCorrupt
	}
	return kind, seq, key, value, nil
}

// walReader reads framed records and reports exactly why it stopped.
type walReader struct {
	f    *os.File
	r    *bufio.Reader
	size int64
	off  int64
}

func newWALReader(path string) (*walReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &walReader{f: f, r: bufio.NewReaderSize(f, 64<<10), size: st.Size()}, nil
}

func (r *walReader) Close() error { return r.f.Close() }

// Offset is the byte offset just past the last record Next returned
// successfully — the point a recovering writer would truncate to.
func (r *walReader) Offset() int64 { return r.off }

// Next returns the next payload, io.EOF at a clean end of file,
// errTornRecord for an incomplete tail, or errBadChecksum for bytes that do
// not match their CRC.
func (r *walReader) Next() ([]byte, error) {
	remaining := r.size - r.off
	if remaining == 0 {
		return nil, io.EOF
	}
	if remaining < walHeaderLen {
		return nil, errTornRecord
	}

	var hdr [walHeaderLen]byte
	if _, err := io.ReadFull(r.r, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, errTornRecord
		}
		return nil, err
	}
	want := binary.LittleEndian.Uint32(hdr[0:4])
	length := binary.LittleEndian.Uint32(hdr[4:8])

	if int64(length) > remaining-walHeaderLen {
		// The length field itself says the record never finished.
		return nil, errTornRecord
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, errTornRecord
		}
		return nil, err
	}
	if crc32.Checksum(payload, crcTable) != want {
		return nil, errBadChecksum
	}

	r.off += walHeaderLen + int64(length)
	return payload, nil
}

// walReplayResult describes what a replay found.
type walReplayResult struct {
	Records        int    // records applied
	LastGoodOffset int64  // bytes of the file that are trustworthy
	DroppedBytes   int64  // bytes after the last good record
	Reason         string // why replay stopped, "" for a clean end of file
}

// replayWAL feeds every intact record to apply, in write order, and stops
// at the first damaged one.
func replayWAL(path string, apply func(kind uint8, seq uint64, key, value []byte) error) (walReplayResult, error) {
	r, err := newWALReader(path)
	if err != nil {
		return walReplayResult{}, err
	}
	defer r.Close()

	var res walReplayResult
	for {
		payload, err := r.Next()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// Clean end of file.
			case errors.Is(err, errTornRecord):
				res.Reason = "torn tail: final record was never completed"
			case errors.Is(err, errBadChecksum):
				res.Reason = "checksum mismatch: record does not match its CRC-32C"
			default:
				return res, err
			}
			res.LastGoodOffset = r.Offset()
			res.DroppedBytes = r.size - r.Offset()
			return res, nil
		}
		kind, seq, key, value, derr := decodeWALPayload(payload)
		if derr != nil {
			// The framing checked out but the contents did not parse.
			res.Reason = "malformed record payload"
			res.LastGoodOffset = r.Offset()
			res.DroppedBytes = r.size - r.Offset()
			return res, nil
		}
		if err := apply(kind, seq, key, value); err != nil {
			return res, err
		}
		res.Records++
	}
}
