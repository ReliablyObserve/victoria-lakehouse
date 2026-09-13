package compaction

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// dropTombstonedLogRows removes rows matching any active tombstone from a merge
// result, returning the survivors.
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
// exactly as before.
func dropTombstonedLogRows(store *delete.TombstoneStore, rows []schema.LogRow) []schema.LogRow {
	if store == nil || len(rows) == 0 {
		return rows
	}
	minNs, maxNs := schema.LogRowTimeBounds(rows)
	tss := store.ForRange(minNs, maxNs)
	if len(tss) == 0 {
		return rows
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
	return kept
}

// dropTombstonedTraceRows is dropTombstonedLogRows for spans.
func dropTombstonedTraceRows(store *delete.TombstoneStore, rows []schema.TraceRow) []schema.TraceRow {
	if store == nil || len(rows) == 0 {
		return rows
	}
	minNs, maxNs := schema.TraceRowTimeBounds(rows)
	tss := store.ForRange(minNs, maxNs)
	if len(tss) == 0 {
		return rows
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
	return kept
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

// markKeysReaped records that a compaction has permanently removed the
// tombstoned rows that lived in the given source keys.
//
// Without this, the rewriter would later try to rewrite keys that compaction
// already merged away: the download 404s, the rewrite errors, and the tombstone
// stays active forever. Marking them here is what lets a tombstone complete
// when compaction — rather than the rewriter — did the work.
//
// Keys under a never-delete prefix are left alone: those objects are not
// compaction's to reap.
func markKeysReaped(store *delete.TombstoneStore, keys []string, neverDelete []string) {
	if store == nil || len(keys) == 0 {
		return
	}
	eligible := make([]string, 0, len(keys))
	for _, k := range keys {
		if isNeverDeleteKey(k, neverDelete) {
			continue
		}
		eligible = append(eligible, k)
	}
	if len(eligible) == 0 {
		return
	}

	for _, ts := range store.Active() {
		if ts.Mode == "hide" {
			// A hide-mode tombstone has no reap lifecycle: its rows were
			// dropped from the merged output all the same, but the tombstone
			// stays standing because the user asked for suppression, not
			// removal.
			continue
		}
		changed := false
		for _, k := range eligible {
			if !containsKey(ts.AffectedKeys, k) || ts.Reaped[k] {
				continue
			}
			if ts.Reaped == nil {
				ts.Reaped = make(map[string]bool)
			}
			ts.Reaped[k] = true
			changed = true
			metrics.DeleteCompactionKeysReaped.Inc()
		}
		if changed {
			store.Add(ts)
			store.Complete(ts.ID)
		}
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
