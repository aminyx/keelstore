package keelstore

import (
	"errors"
	"fmt"
	"testing"
)

// smallOptions makes the tree misbehave on purpose: tiny memtables, tiny
// blocks and tiny files mean a few hundred writes produce real flushes,
// real level-0 pressure and real compactions, which is what most of these
// tests are actually about.
func smallOptions() Options {
	return Options{
		MemtableSize:          2 << 10,
		BlockSize:             256,
		TargetFileSize:        4 << 10,
		L0CompactionTrigger:   3,
		LevelBaseSize:         16 << 10,
		MaxImmutableMemtables: 2,
	}
}

func openTestDB(t *testing.T, opts Options) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, &opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return db
}

// reopen closes a database and opens the same directory again, which is the
// only way to prove that everything needed survived on disk.
func reopen(t *testing.T, db *DB, opts Options) *DB {
	t.Helper()
	dir := db.Path()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	next, err := Open(dir, &opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { next.Close() })
	return next
}

func mustPut(t *testing.T, db *DB, key, value string) {
	t.Helper()
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func mustDelete(t *testing.T, db *DB, key string) {
	t.Helper()
	if err := db.Delete([]byte(key)); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	_, err := db.Get([]byte(key))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) = %v, want ErrNotFound", key, err)
	}
}

func key(i int) string   { return fmt.Sprintf("key-%06d", i) }
func value(i int) string { return fmt.Sprintf("value-%06d", i) }

// collect drains an iterator into parallel key and value slices.
func collect(t *testing.T, it *Iterator) ([]string, []string) {
	t.Helper()
	var keys, values []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return keys, values
}
