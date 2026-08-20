package keelstore

import "errors"

var (
	// ErrNotFound is returned by Get when the key is absent, or when the
	// newest visible version of the key is a tombstone.
	ErrNotFound = errors.New("keelstore: key not found")

	// ErrClosed is returned by every operation on a closed database.
	ErrClosed = errors.New("keelstore: database is closed")

	// ErrEmptyKey is returned when a zero-length key is written.
	ErrEmptyKey = errors.New("keelstore: key must not be empty")

	// ErrKeyTooLarge is returned when a key exceeds maxKeyLen bytes.
	ErrKeyTooLarge = errors.New("keelstore: key too large")

	// ErrCorrupt reports damaged on-disk state that keelstore refuses to
	// interpret: a bad SSTable footer, a truncated index block, a manifest
	// that does not parse.
	ErrCorrupt = errors.New("keelstore: corrupt data")

	// ErrSnapshotReleased is returned when a released snapshot is reused.
	ErrSnapshotReleased = errors.New("keelstore: snapshot already released")

	// ErrIteratorClosed is returned by Iterator.Error after Close.
	ErrIteratorClosed = errors.New("keelstore: iterator closed")
)

// errTornRecord marks a WAL record whose framing ran past the end of the
// file: the writer was interrupted before the record was complete.
var errTornRecord = errors.New("keelstore: torn write-ahead log record")

// errBadChecksum marks a WAL record whose payload does not match its
// CRC-32C. The bytes are present but they are not what the writer wrote.
var errBadChecksum = errors.New("keelstore: write-ahead log checksum mismatch")
