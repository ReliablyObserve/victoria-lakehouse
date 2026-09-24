package parquets3

import (
	"context"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// fieldValuesRequest is one field enumeration (field_values, streams,
// stream_ids) over a tenant-scoped file list.
type fieldValuesRequest struct {
	op         string // endpoint label for logs and metrics
	column     string // Parquet column whose values are enumerated
	field      string // the same field's name in rows (VictoriaLogs naming)
	tenantIDs  []logstorage.TenantID
	query      *logstorage.Query
	aggregates bool // the column may be answered from per-file label aggregates
	filter     *logstorage.Filter
	tombstones []tombstone // every tombstone overlapping the files' rows
	parse      keyTenantFunc
	startNs    int64
	endNs      int64
}

// collectFieldValues answers a field enumeration exactly: every value in the
// window and, for each, the number of rows carrying it (VictoriaLogs' hits).
//
// A file is answered from metadata, with no S3 read, when all of these hold:
// the request is unfiltered, the file lies wholly inside the window, no
// tombstone of its tenant reaches its time range, and its manifest entry carries the
// column's label aggregate — the exact per-value row count computed from the
// file's rows at flush and again at compaction. Every other file (straddling
// the window, tombstoned, filtered, or without the aggregate: a field over the
// per-file cap, a column that is not a label, a file written without one) is
// scanned, confined to the window, on a bounded worker pool, so a scan costs
// one wave of round-trips instead of one per file.
//
// Every file is read to the end even when a limit is set: past the limit
// VictoriaLogs returns the first values in natural order with zeroed hits
// (valuesWithHits), which a partial scan cannot tell apart from a subset.
//
// The per-value set a partition catalog holds cannot serve this: it has no
// counts, only membership, and answered every value with hits=1.
func (s *Storage) collectFieldValues(ctx context.Context, files []manifest.FileInfo, r fieldValuesRequest) (map[string]uint64, error) {
	seen := make(map[string]uint64)
	scan := make([]manifest.FileInfo, 0, len(files))
	fromMeta := 0
	for _, fi := range files {
		if counts, ok := s.fileAggregate(fi, r); ok {
			for v, c := range counts {
				if v != "" && c > 0 {
					seen[v] += uint64(c)
				}
			}
			fromMeta++
			continue
		}
		scan = append(scan, fi)
	}
	metrics.FieldValuesFiles.Add("aggregate", fromMeta)
	metrics.FieldValuesFiles.Add("scan", len(scan))
	if len(scan) == 0 {
		metrics.CatalogValueLookups.Add("catalog", 1) // whole answer from RAM
	} else {
		metrics.CatalogValueLookups.Add("scan", 1)
		if err := s.scanFieldValuesParallel(ctx, scan, r, seen); err != nil {
			return nil, err
		}
	}
	s.collectBufferedValues(ctx, files, r, seen)
	return seen, ctx.Err()
}

// collectBufferedValues adds the rows not yet flushed to Parquet — this
// node's co-located buffer, or every insert peer's through the buffer bridge
// — exactly as a query merges them: tenant-scoped, only rows newer than each
// tenant's flush watermark over the selected objects (so no row is counted
// from both tiers), through the request's filter and its tenants' tombstones.
// Without them a dropdown over the last minutes misses values upstream
// returns and undercounts hits.
func (s *Storage) collectBufferedValues(ctx context.Context, files []manifest.FileInfo, r fieldValuesRequest, seen map[string]uint64) {
	if r.query == nil || r.field == "" {
		return
	}
	var mu sync.Mutex
	count := func(tss []tombstone) logstorage.WriteDataBlockFunc {
		return func(_ uint, db *logstorage.DataBlock) {
			if db = filterDataBlock(db, r.filter); db == nil || db.RowsCount() == 0 {
				return
			}
			if len(tss) > 0 {
				if db = suppressTombstonedRows(db, tss); db == nil || db.RowsCount() == 0 {
					return
				}
			}
			for _, c := range db.GetColumns(false) {
				if c.Name != r.field {
					continue
				}
				mu.Lock()
				for _, v := range c.Values {
					if v != "" {
						seen[v]++
					}
				}
				mu.Unlock()
			}
		}
	}
	scope := scopeFor(ctx, r.tenantIDs)
	sink := newTombstoneSink(scope, r.tombstones, r.parse, s.AccountOnlyTenantKeys(), count)
	s.bufferRowsTo(ctx, r.startNs, r.endNs, s.bufferWatermarksFor(files), r.query, r.tenantIDs, sink)
}

// fileAggregate returns the file's exact per-value counts for the request's
// column when the file can be answered from metadata (see collectFieldValues).
func (s *Storage) fileAggregate(fi manifest.FileInfo, r fieldValuesRequest) (map[string]int64, bool) {
	if !r.aggregates || r.filter != nil || !fileWithinWindow(fi, r.startNs, r.endNs) {
		return nil, false
	}
	// Counts predate any delete: a tombstone of the file's tenant whose time
	// range reaches the file's rows sends it to the scan, which applies it.
	for _, ts := range tombstonesForKey(r.tombstones, r.parse, fi.Key) {
		if ts.AffectsFile(fi.MinTimeNs, fi.MaxTimeNs) {
			return nil, false
		}
	}
	counts, ok := fi.LabelAggregates[r.column]
	return counts, ok && len(counts) > 0
}

// scanFieldValuesParallel scans files on the query path's file-worker pool,
// each confined to the window, merging per-file counts into seen.
func (s *Storage) scanFieldValuesParallel(ctx context.Context, files []manifest.FileInfo, r fieldValuesRequest, seen map[string]uint64) error {
	workers := s.cfg.Query.FileWorkers
	if workers <= 0 {
		workers = 8
	}
	if workers > len(files) {
		workers = len(files)
	}

	var mu sync.Mutex
	taskCh := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		taskCh <- fi
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if ctx.Err() != nil {
					return
				}
				// Column-projected read: only the target (+ filter and
				// timestamp) chunks are fetched, never the whole object body.
				local := make(map[string]uint64)
				if err := s.scanProjectedFieldValues(ctx, fi, r.column, r.filter, tombstonesForKey(r.tombstones, r.parse, fi.Key), local, r.startNs, r.endNs); err != nil {
					if ctx.Err() == nil {
						logger.Warnf("scan projected %s: %s; key=%s", r.op, err, fi.Key)
					}
					continue
				}
				mu.Lock()
				for v, c := range local {
					seen[v] += c
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// valuesWithHits builds the response the way VictoriaLogs does, with its own
// merge: descending hits, then values in natural order; past the limit the
// hits are zeroed and the first limit values in natural order are kept.
func valuesWithHits(seen map[string]uint64, limit uint64) []logstorage.ValueWithHits {
	if len(seen) == 0 {
		return nil
	}
	vhs := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		vhs = append(vhs, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	return logstorage.MergeValuesWithHits([][]logstorage.ValueWithHits{vhs}, limit, true)
}
