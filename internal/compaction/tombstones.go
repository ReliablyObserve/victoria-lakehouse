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
// exactly as before. Returns the survivors, how many rows were dropped, and the
// IDs of the tombstones whose predicate was applied to EVERY row — the only
// tombstones the output may later be recorded clean for (see
// reconcileTombstones). A tombstone that becomes eligible, or is issued, after
// this call was not applied, whatever the clock says by bookkeeping time.
//
// Only the tombstones that act on the merge's tenant are applied (see
// keyScope.tombstones): a tenant-scoped delete never removes another tenant's
// rows, and a tombstone that is not applied is not reported applied, so the
// output stays pending for it.
func dropTombstonedLogRows(store *delete.TombstoneStore, rows []schema.LogRow, now time.Time, rewriteDelay time.Duration, scope keyScope) ([]schema.LogRow, int, map[string]bool) {
	if store == nil || len(rows) == 0 {
		return rows, 0, nil
	}
	minNs, maxNs := schema.LogRowTimeBounds(rows)
	tss := scope.tombstones(eligibleTombstones(store.ForRange(minNs, maxNs), now, rewriteDelay))
	if len(tss) == 0 {
		return rows, 0, nil
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
	return kept, dropped, appliedIDs(tss)
}

// dropTombstonedTraceRows is dropTombstonedLogRows for spans.
func dropTombstonedTraceRows(store *delete.TombstoneStore, rows []schema.TraceRow, now time.Time, rewriteDelay time.Duration, scope keyScope) ([]schema.TraceRow, int, map[string]bool) {
	if store == nil || len(rows) == 0 {
		return rows, 0, nil
	}
	minNs, maxNs := schema.TraceRowTimeBounds(rows)
	tss := scope.tombstones(eligibleTombstones(store.ForRange(minNs, maxNs), now, rewriteDelay))
	if len(tss) == 0 {
		return rows, 0, nil
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
	return kept, dropped, appliedIDs(tss)
}

// keyScope names the objects a merge reads, so the tombstones it applies can be
// attributed to their tenant the way the read path attributes objects: by key.
// parse is the manifest's tenant key parser (nil: the default
// {AccountID}/{ProjectID} layout).
type keyScope struct {
	keys  []string
	parse delete.KeyTenantFunc
}

// tombstones keeps the tombstones that act on EVERY input of the merge. A
// tenant-scoped tombstone is applied only when all inputs belong to one of its
// tenants; with no inputs named, only instance-wide tombstones apply. A merge
// normally reads one tenant's objects (see groupFilesByTenant), so this only
// ever withholds a tombstone from a group whose keys do not attribute to one
// tenant — its rows are then carried forward and left to the rewriter, which
// judges the output by its own key.
func (ks keyScope) tombstones(tss []delete.Tombstone) []delete.Tombstone {
	out := tss[:0:0]
	for i := range tss {
		if ks.appliesToAll(&tss[i]) {
			out = append(out, tss[i])
		}
	}
	return out
}

func (ks keyScope) appliesToAll(ts *delete.Tombstone) bool {
	if !ts.Scoped() {
		return true
	}
	if len(ks.keys) == 0 {
		return false
	}
	for _, k := range ks.keys {
		if !ts.AppliesToKey(ks.parse, k) {
			return false
		}
	}
	return true
}

// appliedIDs is the ID set of the tombstones a drop evaluated.
func appliedIDs(tss []delete.Tombstone) map[string]bool {
	out := make(map[string]bool, len(tss))
	for i := range tss {
		out[tss[i].ID] = true
	}
	return out
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
//   - the output is added to AffectedKeys, marked reaped only if the merge
//     applied this tombstone's predicate to every row (its ID is in applied, as
//     returned by the drop step). Otherwise the rows were carried forward and the
//     output stays pending, so the rewrite scheduler rewrites it once the
//     tombstone becomes eligible.
//
// Cleanliness is a fact about what the merge DID, so it is taken from the drop
// step rather than re-judged here: judging eligibility again at bookkeeping time
// recorded the output clean for a tombstone that became eligible — or was
// issued — while the merge ran, although the merge had carried its rows. That
// tombstone then retired and the carried rows came back.
//
// Without the second half, a tombstone still inside its un-delete window whose
// source was compacted would see every listed key "reaped", retire, and un-hide
// rows that were never removed.
//
// Hide-mode tombstones have no reap lifecycle and are left alone. Keys under a
// never-delete prefix are not compaction's and are skipped. canRetire is false
// while the manifest has not listed the bucket in this process or the tombstone
// store was not fully restored: the bookkeeping is still recorded, but nothing
// is retired on a file set that may be incomplete.
//
// A tombstone follows its rows only onto an output of a tenant it acts on;
// parse attributes the output key to its tenant (nil: the default layout).
func reconcileTombstones(store *delete.TombstoneStore, inputKeys []string, outputKey string, neverDelete []string, applied map[string]bool, canRetire bool, parse delete.KeyTenantFunc) {
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
		clean := applied[snapshot.ID]
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
				ts.MarkReaped(k) // a clean file merged away is now gone too
				touched = true
				reapedHere++
			}
			if !touched {
				return false
			}
			if outputKey != "" && ts.AppliesToKey(parse, outputKey) {
				if !containsKey(ts.AffectedKeys, outputKey) {
					ts.AffectedKeys = append(ts.AffectedKeys, outputKey)
				}
				// Clean only if this merge applied the tombstone's predicate;
				// otherwise the rows were carried forward and the output waits
				// for the rewriter.
				ts.SetClean(outputKey, clean)
			}
			return true
		})
		if !changed {
			continue
		}
		metrics.DeleteCompactionKeysReaped.Add(reapedHere)
		if canRetire {
			store.Complete(snapshot.ID)
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
