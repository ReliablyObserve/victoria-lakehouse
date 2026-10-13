package compaction

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// mergeIdxFiles reads the rows of the inputs, objects of the trace_id_idx table.
// Compaction of this table only merge-sorts and re-encodes: no row is merged with
// another, because VictoriaTraces' own storage keeps every index row it was sent and
// hot VictoriaTraces counts them.
func (c *Compactor) mergeIdxFiles(allData [][]byte) ([]tracesidx.Row, error) {
	var merged []tracesidx.Row
	for _, data := range allData {
		rows, err := tracesidx.ReadAll(data)
		if err != nil {
			return nil, fmt.Errorf("read trace_id_idx rows: %w", err)
		}
		merged = append(merged, rows...)
	}
	return merged, nil
}

// dropTombstonedIdxRows is dropTombstonedTraceRows for index rows.
func dropTombstonedIdxRows(store *delete.TombstoneStore, rows []tracesidx.Row, now time.Time, rewriteDelay time.Duration, scope keyScope) ([]tracesidx.Row, int, map[string]bool) {
	if store == nil || len(rows) == 0 {
		return rows, 0, nil
	}
	minNs, maxNs := rows[0].Time, rows[0].Time
	for i := range rows {
		minNs, maxNs = min(minNs, rows[i].Time), max(maxNs, rows[i].Time)
	}
	tss := scope.tombstones(eligibleTombstones(store.ForRange(minNs, maxNs), now, rewriteDelay))
	if len(tss) == 0 {
		return rows, 0, nil
	}
	kept := rows[:0]
	dropped := 0
	for i := range rows {
		fields := delete.IdxRowFields(&rows[i])
		hit := false
		for j := range tss {
			if tss[j].MatchesFields(fields, rows[i].Time) {
				hit = true
				break
			}
		}
		if hit {
			dropped++
			continue
		}
		kept = append(kept, rows[i])
	}
	if dropped > 0 {
		metrics.DeleteCompactionRowsRemoved.Add(dropped)
		logger.Infof("compaction dropped tombstoned trace_id_idx rows; dropped=%d, kept=%d", dropped, len(kept))
	}
	return kept, dropped, appliedIDs(tss)
}
