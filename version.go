package keelstore

import (
	"bytes"
	"sort"
	"sync/atomic"
)

// A version is an immutable snapshot of which SSTables make up the tree.
// Every flush and every compaction produces a new version rather than
// editing the old one, so a reader that captured a version keeps a coherent
// view of the file set even while a compaction rewrites it underneath.
//
// Level invariants:
//
//	level 0   files may overlap; sorted newest file number first
//	level 1+  files never overlap; sorted by smallest user key ascending
type version struct {
	levels [][]*tableMeta
	refs   atomic.Int32
}

func newVersion(maxLevels int) *version {
	return &version{levels: make([][]*tableMeta, maxLevels)}
}

func (v *version) clone() *version {
	nv := &version{levels: make([][]*tableMeta, len(v.levels))}
	for i, l := range v.levels {
		nv.levels[i] = append([]*tableMeta(nil), l...)
	}
	return nv
}

func (v *version) ref()         { v.refs.Add(1) }
func (v *version) unref() int32 { return v.refs.Add(-1) }

func (v *version) numFiles() int {
	n := 0
	for _, l := range v.levels {
		n += len(l)
	}
	return n
}

func (v *version) levelBytes(level int) int64 {
	var n int64
	for _, t := range v.levels[level] {
		n += t.Size
	}
	return n
}

func (v *version) allNumbers() map[uint64]struct{} {
	m := make(map[uint64]struct{})
	for _, l := range v.levels {
		for _, t := range l {
			m[t.Number] = struct{}{}
		}
	}
	return m
}

// sortLevel restores the invariant for a level after files were added.
func (v *version) sortLevel(level int) {
	l := v.levels[level]
	if level == 0 {
		sort.Slice(l, func(i, j int) bool { return l[i].Number > l[j].Number })
		return
	}
	sort.Slice(l, func(i, j int) bool {
		if c := bytes.Compare(l[i].smallestUser(), l[j].smallestUser()); c != 0 {
			return c < 0
		}
		return l[i].Number < l[j].Number
	})
}

// overlapping returns the files in a level whose user key range intersects
// [smallest, largest].
func (v *version) overlapping(level int, smallest, largest []byte) []*tableMeta {
	if level < 0 || level >= len(v.levels) {
		return nil
	}
	var out []*tableMeta
	for _, t := range v.levels[level] {
		if bytes.Compare(t.largestUser(), smallest) < 0 {
			continue
		}
		if bytes.Compare(t.smallestUser(), largest) > 0 {
			continue
		}
		out = append(out, t)
	}
	return out
}

// findFile returns the single level-1-or-deeper file that can hold userKey,
// found by binary search over the level's disjoint ranges.
func (v *version) findFile(level int, userKey []byte) *tableMeta {
	l := v.levels[level]
	lo, hi := 0, len(l)-1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		t := l[mid]
		switch {
		case bytes.Compare(userKey, t.smallestUser()) < 0:
			hi = mid - 1
		case bytes.Compare(userKey, t.largestUser()) > 0:
			lo = mid + 1
		default:
			return t
		}
	}
	return nil
}

// overlapsDeeperLevels reports whether any level below `level` could still
// hold an older version of userKey. Compaction may only drop a tombstone
// when the answer is no; otherwise the delete would uncover a value that
// was supposed to be gone.
func (v *version) overlapsDeeperLevels(level int, userKey []byte) bool {
	for l := level + 1; l < len(v.levels); l++ {
		for _, t := range v.levels[l] {
			if bytes.Compare(userKey, t.smallestUser()) >= 0 &&
				bytes.Compare(userKey, t.largestUser()) <= 0 {
				return true
			}
		}
	}
	return false
}

// keyRange returns the smallest and largest user key across the given
// tables.
func keyRange(tables ...[]*tableMeta) (smallest, largest []byte) {
	for _, group := range tables {
		for _, t := range group {
			if smallest == nil || bytes.Compare(t.smallestUser(), smallest) < 0 {
				smallest = t.smallestUser()
			}
			if largest == nil || bytes.Compare(t.largestUser(), largest) > 0 {
				largest = t.largestUser()
			}
		}
	}
	return smallest, largest
}
