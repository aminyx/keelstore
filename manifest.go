package keelstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The manifest is the root of the on-disk state: which SSTables exist, at
// which level, and how far the sequence counter got. It is written whole,
// to a temporary file, fsynced, and then renamed over the old one, so a
// crash leaves either the complete old manifest or the complete new one.
//
// JSON is a deliberate choice over a packed binary log. It costs a few
// hundred bytes and a rewrite per flush, which is nothing next to the
// SSTable it accompanies, and it means the state of a store can be read
// with `cat`.

const (
	manifestName    = "MANIFEST"
	manifestTmpName = "MANIFEST.tmp"
	manifestVersion = 1

	sstExt = ".sst"
	walExt = ".wal"
)

type manifestTable struct {
	Number   uint64 `json:"number"`
	Size     int64  `json:"size"`
	Smallest []byte `json:"smallest"` // base64 internal key
	Largest  []byte `json:"largest"`  // base64 internal key
	MinSeq   uint64 `json:"min_seq"`
	MaxSeq   uint64 `json:"max_seq"`
	Entries  uint64 `json:"entries"`
}

type manifestFile struct {
	Version        int               `json:"version"`
	NextFileNumber uint64            `json:"next_file_number"`
	LastSequence   uint64            `json:"last_sequence"`
	Levels         [][]manifestTable `json:"levels"`
}

func writeManifest(dir string, nextFile, lastSeq uint64, v *version) error {
	mf := manifestFile{
		Version:        manifestVersion,
		NextFileNumber: nextFile,
		LastSequence:   lastSeq,
		Levels:         make([][]manifestTable, len(v.levels)),
	}
	for i, level := range v.levels {
		mf.Levels[i] = make([]manifestTable, 0, len(level))
		for _, t := range level {
			mf.Levels[i] = append(mf.Levels[i], manifestTable{
				Number:   t.Number,
				Size:     t.Size,
				Smallest: t.Smallest,
				Largest:  t.Largest,
				MinSeq:   t.MinSeq,
				MaxSeq:   t.MaxSeq,
				Entries:  t.Entries,
			})
		}
	}

	buf, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')

	tmp := filepath.Join(dir, manifestTmpName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// os.Rename replaces the destination atomically on both POSIX and
	// Windows, which is what makes the swap crash-safe.
	return os.Rename(tmp, filepath.Join(dir, manifestName))
}

func readManifest(dir string, maxLevels int) (nextFile, lastSeq uint64, v *version, err error) {
	buf, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return 0, 0, nil, err
	}
	var mf manifestFile
	if err := json.Unmarshal(buf, &mf); err != nil {
		return 0, 0, nil, fmt.Errorf("%w: manifest: %v", ErrCorrupt, err)
	}
	if mf.Version != manifestVersion {
		return 0, 0, nil, fmt.Errorf("%w: manifest version %d not supported", ErrCorrupt, mf.Version)
	}

	if len(mf.Levels) > maxLevels {
		maxLevels = len(mf.Levels)
	}
	v = newVersion(maxLevels)
	for i, level := range mf.Levels {
		for _, t := range level {
			if !validInternalKey(t.Smallest) || !validInternalKey(t.Largest) {
				return 0, 0, nil, fmt.Errorf("%w: manifest table %d has an invalid key range", ErrCorrupt, t.Number)
			}
			v.levels[i] = append(v.levels[i], &tableMeta{
				Number:   t.Number,
				Size:     t.Size,
				Smallest: t.Smallest,
				Largest:  t.Largest,
				MinSeq:   t.MinSeq,
				MaxSeq:   t.MaxSeq,
				Entries:  t.Entries,
			})
		}
		v.sortLevel(i)
	}
	return mf.NextFileNumber, mf.LastSequence, v, nil
}

func manifestExists(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, manifestName))
	return err == nil
}

// File naming: zero-padded numbers keep a directory listing in creation
// order, which makes a store directory readable at a glance.

func sstPath(dir string, number uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", number, sstExt))
}

func walPath(dir string, number uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", number, walExt))
}

// listNumberedFiles returns the file numbers of every dir entry with the
// given extension, in ascending order.
func listNumberedFiles(dir, ext string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ext) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ext)
		n, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	for i := 1; i < len(nums); i++ {
		for j := i; j > 0 && nums[j] < nums[j-1]; j-- {
			nums[j], nums[j-1] = nums[j-1], nums[j]
		}
	}
	return nums, nil
}
