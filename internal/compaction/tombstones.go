package compaction

import (
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// dropTombstonedLogRows removes rows matching any tombstone that is ELIGIBLE for
// physical removal (see delete.Tombstone.EligibleForPhysicalRemoval) from a
// merge result, returning the survivors. Hide-mode tombstones and tombstones
// still inside their rewrite_delay are skipped: their rows are carried forward,
// still hidden at query time and still restorable by an un-delete.
//
// Compaction rewrites every input row into a new object. Before this, it
// carried tombstoned rows through verbatim: the query-time filter kept hiding
// them, but the bytes were copied forward forever and every compaction undid
// part of the rewriter's work by producing a fresh key the tombstone's
// AffectedKeys list had never heard of. Dropping them here makes compaction a
// second, free reaper — the merge is already reading and rewriting the rows, so
// suppressing them costs one predicate evaluation and saves their storage
// permanently.
//
// The tombstone store is optional; a compactor wired without one behaves
// exactly as before. Returns the survivors and how many rows were dropped.
func dropTombstonedLogRows(store *delete.TombstoneStore, rows []schema.LogRow, now time.Time, rewriteDelay time.Duration) ([]schema.LogRow, int) {
	if store == nil || len(rows) == 0 {
		return rows, 0
	}
	minNs, maxNs := schema.LogRowTimeBounds(rows)
	tss := eligibleTombstones(store.ForRange(minNs, maxNs), now, rewriteDelay)
	if len(tss) == 0 {
		return rows, 0
	}

	kept := rows[:0]
	dropped := 0
	for i := range rows {
		if tombstonedLogRow(tss, &rows[i]) {
			dropped++
			continue
		}
		kept = append(kept, rows[i])
	}
	if dropped > 0 {
		metrics.DeleteCompactionRowsRemoved.Add(dropped)
		logger.Infof("compaction dropped tombstoned rows; dropped=%d, kept=%d", dropped, len(kept))
	}
	return kept, dropped
}

// dropTombstonedTraceRows is dropTombstonedLogRows for spans.
func dropTombstonedTraceRows(store *delete.TombstoneStore, rows []schema.TraceRow, now time.Time, rewriteDelay time.Duration) ([]schema.TraceRow, int) {
	if store == nil || len(rows) == 0 {
		return rows, 0
	}
	minNs, maxNs := schema.TraceRowTimeBounds(rows)
	tss := eligibleTombstones(store.ForRange(minNs, maxNs), now, rewriteDelay)
	if len(tss) == 0 {
		return rows, 0
	}

	kept := rows[:0]
	dropped := 0
	for i := range rows {
		if tombstonedTraceRow(tss, &rows[i]) {
			dropped++
			continue
		}
		kept = append(kept, rows[i])
	}
	if dropped > 0 {
		metrics.DeleteCompactionRowsRemoved.Add(dropped)
		logger.Infof("compaction dropped tombstoned spans; dropped=%d, kept=%d", dropped, len(kept))
	}
	return kept, dropped
}

// survivorMeta carries the manifest fields that must be re-derived from the
// surviving rows once a compaction has dropped tombstoned ones.
type survivorMeta struct {
	labels   map[string][]string
	rawBytes int64
}

func tombstonedLogRow(tss []delete.Tombstone, row *schema.LogRow) bool {
	fields := delete.LogRowFields(row)
	for i := range tss {
		if tss[i].MatchesFields(fields, row.TimestampUnixNano) {
			return true
		}
	}
	return false
}

func tombstonedTraceRow(tss []delete.Tombstone, row *schema.TraceRow) bool {
	fields := delete.TraceRowFields(row)
	for i := range tss {
		if tss[i].MatchesFields(fields, row.TimestampUnixNano) {
			return true
		}
	}
	return false
}

// eligibleTombstones keeps only the tombstones whose rows may be physically
// removed now.
func eligibleTombstones(tss []delete.Tombstone, now time.Time, rewriteDelay time.Duration) []delete.Tombstone {
	out := tss[:0:0]
	for i := range tss {
		if tss[i].EligibleForPhysicalRemoval(now, rewriteDelay) {
			out = append(out, tss[i])
		}
	}
	return out
}

// reconcileTombstones moves every tombstone's bookkeeping from the merged-away
// source keys onto the compacted output.
//
// AffectedKeys is a snapshot of the files a delete covered when it was issued,
// and a tombstone may retire only once none of the files holding its rows
// remain unfiltered. Compaction changes the file set under it, so for each
// tombstone that named a source:
//
//   - the source is marked reaped — the object is gone;
//   - the output is added to AffectedKeys, marked reaped only if compaction
//     filtered this tombstone's rows out of it (the tombstone was eligible for
//     physical removal). Otherwise the rows were carried forward and the output
//     stays pending, so the rewrite scheduler rewrites it once the tombstone
//     becomes eligible.
//
// Without the second half, a tombstone still inside its un-delete window whose
// source was compacted would see every listed key "reaped", retire, and un-hide
// rows that were never removed.
//
// Hide-mode tombstones have no reap lifecycle and are left alone. Keys under a
// never-delete prefix are not compaction's and are skipped.
func reconcileTombstones(store *delete.TombstoneStore, inputKeys []string, outputKey string, neverDelete []string, now time.Time, rewriteDelay time.Duration) {
	if store == nil || len(inputKeys) == 0 {
		return
	}
	eligible := make([]string, 0, len(inputKeys))
	for _, k := range inputKeys {
		if isNeverDeleteKey(k, neverDelete) {
			continue
		}
		eligible = append(eligible, k)
	}
	if len(eligible) == 0 {
		return
	}

	for _, snapshot := range store.Active() {
		if snapshot.Mode == "hide" {
			continue
		}
		clean := snapshot.EligibleForPhysicalRemoval(now, rewriteDelay)
		var reapedHere int
		// Update, not Get-modify-Add: the rewrite scheduler writes the same
		// record concurrently, and a lost update here would drop the transfer
		// of the tombstone to an output that still holds its rows.
		_, changed := store.Update(snapshot.ID, func(ts *delete.Tombstone) bool {
			touched := false
			for _, k := range eligible {
				if !containsKey(ts.AffectedKeys, k) || ts.Reaped[k] {
					continue
				}
				if ts.Reaped == nil {
					ts.Reaped = make(map[string]bool)
				}
				ts.Reaped[k] = true
				touched = true
				reapedHere++
			}
			if !touched {
				return false
			}
			if outputKey != "" {
				if !containsKey(ts.AffectedKeys, outputKey) {
					ts.AffectedKeys = append(ts.AffectedKeys, outputKey)
				}
				// Clean only if this compaction applied the tombstone's
				// predicate; otherwise the rows were carried forward and the
				// output waits for the rewriter.
				ts.Reaped[outputKey] = clean
			}
			return true
		})
		if !changed {
			continue
		}
		metrics.DeleteCompactionKeysReaped.Add(reapedHere)
		store.Complete(snapshot.ID)
	}
}

func containsKey(keys []string, k string) bool {
	for _, v := range keys {
		if v == k {
			return true
		}
	}
	return false
}

// isNeverDeleteKey mirrors OrphanSweep.isProtected's substring test as a free
// function, so the reap bookkeeping honours the same never-delete list the
// sweep does without reaching for a sweep instance it does not have.
func isNeverDeleteKey(key string, neverDelete []string) bool {
	for _, p := range neverDelete {
		if p != "" && strings.Contains(key, p) {
			return true
		}
	}
	return false
}
