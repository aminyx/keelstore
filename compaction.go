package keelstore

import (
	"bytes"
	"context"
	"os"
)

// keelstore uses levelled compaction.
//
// Level 0 holds whatever the flusher produced, so its files overlap each
// other freely. Every level below it is a single sorted run: the files are
// disjoint, sorted by key, and a lookup consults at most one of them. Each
// level is allowed ten times the bytes of the one above, so a store of N
// bytes has about log10(N) levels.
//
// The alternative, size-tiered compaction, merges same-sized files into
// bigger ones and writes less: each byte is rewritten roughly once per
// tier instead of once per level. The price is that a level holds several
// overlapping runs, so a point lookup may have to check every one of them,
// and space amplification spikes when a large merge is in flight.
//
// keelstore is an embedded store for a single application process. It is
// read-heavy by assumption, it wants predictable latency more than peak
// ingest throughput, and it should not need twice its data size in free
// disk to compact. Those three constraints all point at levelled.
//
// A compaction picks a level, takes one file from it (all files, for level
// 0), takes every file it overlaps in the level below, merges them, and
// writes a new sorted run. Along the way it drops versions of a key that
// nothing can see any more, and tombstones that have nothing left to hide.

// compactionJob is one unit of merge work.
type compactionJob struct {
	level       int
	outputLevel int
	inputs      [2][]*tableMeta // [0] from level, [1] from level+1
}

func (c *compactionJob) allInputs() []*tableMeta {
	out := make([]*tableMeta, 0, len(c.inputs[0])+len(c.inputs[1]))
	out = append(out, c.inputs[0]...)
	out = append(out, c.inputs[1]...)
	return out
}

// pickCompaction chooses the next compaction, or nil when the tree is
// already within budget. Level 0 is prioritised because its overlapping
// files are what make reads slow.
func (db *DB) pickCompaction(v *version) *compactionJob {
	if len(v.levels[0]) >= db.opts.L0CompactionTrigger {
		inputs0 := append([]*tableMeta(nil), v.levels[0]...)
		smallest, largest := keyRange(inputs0)
		return &compactionJob{
			level:       0,
			outputLevel: 1,
			inputs:      [2][]*tableMeta{inputs0, v.overlapping(1, smallest, largest)},
		}
	}

	best, bestScore := -1, 1.0
	for level := 1; level < len(v.levels)-1; level++ {
		score := float64(v.levelBytes(level)) / float64(db.opts.maxBytesForLevel(level))
		if score > bestScore {
			best, bestScore = level, score
		}
	}
	if best < 0 {
		return nil
	}

	// Round-robin through the level's key space so that repeated
	// compactions of the same level do not keep rewriting the same file.
	var pick *tableMeta
	if ptr := db.compactPtr[best]; ptr != nil {
		for _, t := range v.levels[best] {
			if bytes.Compare(t.largestUser(), ptr) > 0 {
				pick = t
				break
			}
		}
	}
	if pick == nil {
		pick = v.levels[best][0]
	}

	inputs0 := []*tableMeta{pick}
	smallest, largest := keyRange(inputs0)
	return &compactionJob{
		level:       best,
		outputLevel: best + 1,
		inputs:      [2][]*tableMeta{inputs0, v.overlapping(best+1, smallest, largest)},
	}
}

// forcedCompaction merges every file at a level into the level below,
// ignoring size budgets. Compact uses it to rewrite the whole tree on
// demand; the background worker never does.
func forcedCompaction(v *version, level int) *compactionJob {
	if level < 0 || level >= len(v.levels)-1 || len(v.levels[level]) == 0 {
		return nil
	}
	inputs0 := append([]*tableMeta(nil), v.levels[level]...)
	smallest, largest := keyRange(inputs0)
	return &compactionJob{
		level:       level,
		outputLevel: level + 1,
		inputs:      [2][]*tableMeta{inputs0, v.overlapping(level+1, smallest, largest)},
	}
}

// runCompaction merges a job's inputs into new tables at the output level
// and installs the result. It is called with bgMu held, and aborts promptly
// when ctx is cancelled, leaving the tree exactly as it found it.
func (db *DB) runCompaction(ctx context.Context, v *version, job *compactionJob) error {
	inputs := job.allInputs()
	if len(inputs) == 0 {
		return nil
	}

	sources := make([]internalIterator, 0, len(inputs))
	closeSources := func() {
		for _, it := range sources {
			it.Close()
		}
	}
	for _, t := range inputs {
		r, err := db.cache.get(t.Number)
		if err != nil {
			closeSources()
			return err
		}
		sources = append(sources, r.NewIter())
	}
	merged := newMergeIter(sources)
	defer merged.Close()

	// Anything at or below this sequence number is invisible to every
	// reader that exists, which is what makes it safe to throw away.
	oldest := db.oldestSnapshotSeq()

	var (
		outputs      []*tableMeta
		writer       *sstWriter
		lastUser     []byte
		lastSeq      = maxSequence + 1
		prevWritten  []byte
		wantRollover bool
		checkTick    int
	)

	abandon := func() {
		if writer != nil {
			writer.Abandon()
		}
		for _, t := range outputs {
			db.cache.evict(t.Number)
			os.Remove(sstPath(db.dir, t.Number))
		}
	}

	finishOutput := func() error {
		if writer == nil || writer.Empty() {
			if writer != nil {
				writer.Abandon()
				writer = nil
			}
			return nil
		}
		meta, err := writer.Finish()
		writer = nil
		if err != nil {
			return err
		}
		db.stats.bytesWritten.Add(meta.Size)
		outputs = append(outputs, meta)
		return nil
	}

	for ok := merged.First(); ok; ok = merged.Next() {
		checkTick++
		if checkTick%512 == 0 && ctx.Err() != nil {
			abandon()
			db.stats.compactionsCancelled.Add(1)
			return ctx.Err()
		}

		ik := merged.Key()
		uk := userKeyOf(ik)
		seq := seqOf(ik)

		if lastUser == nil || !bytes.Equal(uk, lastUser) {
			lastUser = append(lastUser[:0], uk...)
			lastSeq = maxSequence + 1
		}

		drop := false
		switch {
		case lastSeq <= oldest:
			// A newer version of this key is already visible to every
			// reader, so this one can never be returned again.
			drop = true
		case kindOf(ik) == kindDelete && seq <= oldest &&
			!v.overlapsDeeperLevels(job.outputLevel, uk):
			// A tombstone may only go when no deeper level can still be
			// hiding an older value behind it.
			drop = true
		}
		lastSeq = seq
		if drop {
			continue
		}

		// Roll over to a new output file only on a user key boundary, so
		// that every version of a key stays in one file and the output
		// level's ranges stay disjoint.
		if writer != nil && wantRollover && !bytes.Equal(uk, prevWritten) {
			if err := finishOutput(); err != nil {
				abandon()
				return err
			}
			wantRollover = false
		}

		if writer == nil {
			number := db.nextFile.Add(1) - 1
			w, err := newSSTWriter(sstPath(db.dir, number), number, db.opts)
			if err != nil {
				abandon()
				return err
			}
			writer = w
		}
		if err := writer.Add(ik, merged.Value()); err != nil {
			abandon()
			return err
		}
		prevWritten = append(prevWritten[:0], uk...)
		if writer.EstimatedSize() >= db.opts.TargetFileSize {
			wantRollover = true
		}
	}
	if err := merged.Error(); err != nil {
		abandon()
		return err
	}
	if err := finishOutput(); err != nil {
		abandon()
		return err
	}

	// Build the new version: inputs out, outputs in.
	removed := make(map[uint64]struct{}, len(inputs))
	for _, t := range inputs {
		removed[t.Number] = struct{}{}
	}
	nv := v.clone()
	for _, level := range []int{job.level, job.outputLevel} {
		kept := nv.levels[level][:0]
		for _, t := range nv.levels[level] {
			if _, gone := removed[t.Number]; !gone {
				kept = append(kept, t)
			}
		}
		nv.levels[level] = kept
	}
	nv.levels[job.outputLevel] = append(nv.levels[job.outputLevel], outputs...)
	nv.sortLevel(job.level)
	nv.sortLevel(job.outputLevel)

	if err := db.syncManifest(nv); err != nil {
		abandon()
		return err
	}

	if _, largest := keyRange(job.inputs[0]); largest != nil {
		db.compactPtr[job.level] = append([]byte(nil), largest...)
	}

	db.installVersion(nv)

	obsolete := make([]uint64, 0, len(inputs))
	for _, t := range inputs {
		obsolete = append(obsolete, t.Number)
	}
	db.markObsolete(obsolete)
	db.stats.compactions.Add(1)
	return nil
}
