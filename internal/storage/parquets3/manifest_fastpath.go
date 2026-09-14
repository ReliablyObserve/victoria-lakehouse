package parquets3

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// The manifest fast path answers count-class queries straight from manifest
// metadata: a file whose whole time span sits inside the query window
// contributes exactly RowCount rows, so no Parquet byte has to be read and a
// warm query costs zero S3 requests.
//
// What the emitted rows look like matters for exactness:
//
//   - The block carries ONE column, `_time`, holding a single CONSTANT value.
//     VL collapses a constant column to one stored value on the way in
//     (blockResult.addResultColumn -> addResultColumnConst) and pipeStats then
//     answers `count()` and `count() by (_time:<bucket>)` from br.rowsLen
//     without walking the rows. Cost per file is one FormatField call and a
//     fixed number of allocations, whatever RowCount is.
//   - A constant timestamp is only an honest stand-in while the query cannot
//     tell the rows apart. planMetadataOnly decides that from the query's
//     pipes and coversSpan re-checks it per file: every bucketing the query
//     groups by must put the file's whole [MinTimeNs, MaxTimeNs] span in ONE
//     bucket. Anything else falls through to a real read.
//
// Mirror in lakehouse-traces/internal/storage/parquets3/manifest_fastpath.go.

// syntheticChunkSize bounds how many rows one emitted block covers, so the
// block a downstream pipe materializes stays small no matter how many rows the
// file holds.
const syntheticChunkSize = 10_000

// maxPlausibleRowCount guards against a corrupt manifest entry whose RowCount
// is not a row count at all. It is NOT a cap on the answer: a file above it is
// handed back for a real read (which counts its rows exactly) rather than
// answered with a truncated number. Real Parquet objects stay many orders of
// magnitude below 2^40 rows.
const maxPlausibleRowCount = int64(1) << 40

// metadataOnlyPlan is what the query classifier concluded about serving a
// query from manifest metadata instead of Parquet data.
type metadataOnlyPlan struct {
	// eligible is false when some pipe reads a per-row value that metadata
	// cannot reproduce — then no file may be served from the manifest.
	eligible bool

	// buckets are the `_time` bucketings the query groups by (empty for a
	// plain `count()`). A file qualifies only when its whole time span falls
	// inside one bucket of every bucketing; then the per-bucket count is
	// exactly the file's RowCount.
	buckets []logstorage.TimeBucket
}

// planMetadataOnly classifies q. The decision table lives in VL-side
// GetQueryTimeBucketing (patches/vl-*/external_query.go.src), which walks the
// pipes and reports both the `_time` bucketings and whether any pipe needs
// real row values.
func planMetadataOnly(q *logstorage.Query) metadataOnlyPlan {
	buckets, needsRowValues := logstorage.GetQueryTimeBucketing(q)
	if needsRowValues {
		return metadataOnlyPlan{}
	}
	return metadataOnlyPlan{eligible: true, buckets: buckets}
}

// coversSpan reports whether rows spanning [minNs, maxNs] are indistinguishable
// to this query — i.e. every bucketing it groups by puts the whole span in a
// single bucket, so attributing all of them to one timestamp inside the span
// yields the same per-bucket counts a scan would.
func (p metadataOnlyPlan) coversSpan(minNs, maxNs int64) bool {
	if !p.eligible {
		return false
	}
	if minNs > maxNs {
		return false
	}
	for i := range p.buckets {
		b := p.buckets[i]
		if logstorage.TruncateTimestampToBucket(minNs, b) != logstorage.TruncateTimestampToBucket(maxNs, b) {
			return false
		}
	}
	return true
}

// coversFile reports whether fi may be answered from manifest metadata alone.
func (p metadataOnlyPlan) coversFile(fi manifest.FileInfo) bool {
	return p.coversSpan(fi.MinTimeNs, fi.MaxTimeNs)
}

type metadataOnlyPlanKey struct{}

// withMetadataOnlyPlan carries the per-query plan down to the file/row-group
// readers, which apply the same containment rule to a row group's own bounds.
func withMetadataOnlyPlan(ctx context.Context, p metadataOnlyPlan) context.Context {
	return context.WithValue(ctx, metadataOnlyPlanKey{}, p)
}

// metadataOnlyPlanFromContext returns the plan attached by RunQuery, or a
// disabled plan when there is none. Defaulting to disabled keeps every caller
// that forgets to attach one on the read-for-real path.
func metadataOnlyPlanFromContext(ctx context.Context) metadataOnlyPlan {
	p, _ := ctx.Value(metadataOnlyPlanKey{}).(metadataOnlyPlan)
	return p
}

// fileFullyInRange reports whether fi's manifest metadata is populated and its
// whole time span sits inside [startNs, endNs]. MinTimeNs == 0 is VL-side
// sentinel for "bounds not yet enriched from the Parquet footer".
func fileFullyInRange(fi manifest.FileInfo, startNs, endNs int64) bool {
	return fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 &&
		fi.MinTimeNs >= startNs && fi.MaxTimeNs <= endNs
}

// timestampFieldName returns the field name synthetic blocks publish the
// timestamp under — the registry's internal name, which is what the pipes see.
func (s *Storage) timestampFieldName() string {
	tsCol := s.registry.TimestampColumn()
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		return m.InternalName
	}
	return tsCol
}

// manifestFastPath answers every file it can from manifest metadata and returns
// the files that still need a real read.
func (s *Storage) manifestFastPath(ctx context.Context, files []manifest.FileInfo, startNs, endNs int64, plan metadataOnlyPlan, writeBlock logstorage.WriteDataBlockFunc) []manifest.FileInfo {
	var remaining []manifest.FileInfo
	for _, fi := range files {
		if !fileFullyInRange(fi, startNs, endNs) {
			remaining = append(remaining, fi)
			continue
		}
		if !plan.coversFile(fi) || fi.RowCount > maxPlausibleRowCount {
			// The file is in range but the query can tell its rows apart (or
			// the manifest entry is not trustworthy). Read it for real rather
			// than answer with a fabricated distribution.
			metrics.MetadataOnlyFallbackFiles.Inc()
			if fi.RowCount > maxPlausibleRowCount {
				logger.Warnf("manifest row count is implausible; reading file instead of serving it from metadata; key=%s rows=%d", fi.Key, fi.RowCount)
			}
			remaining = append(remaining, fi)
			continue
		}
		if s.streamConstTimeBlocks(ctx, fi, writeBlock) {
			metrics.MetadataOnlyFiles.Inc()
		}
	}
	if len(remaining) < len(files) {
		logger.Infof("metadata fast path: resolved %d/%d files from manifest, %d remain for S3",
			len(files)-len(remaining), len(files), len(remaining))
	}
	return remaining
}

// streamConstTimeBlocks emits fi.RowCount rows as blocks of at most
// syntheticChunkSize rows, each carrying one constant `_time` column. It
// reports whether at least one block was emitted.
//
// The values slice, the column slice and the DataBlock are allocated once and
// reused for every chunk, so the number of allocations does not grow with
// RowCount. It stops early when ctx is cancelled — the query's max-rows and
// live-bytes budgets cancel it, which is what bounds a runaway row count.
func (s *Storage) streamConstTimeBlocks(ctx context.Context, fi manifest.FileInfo, writeBlock logstorage.WriteDataBlockFunc) bool {
	total := fi.RowCount
	if total <= 0 || writeBlock == nil {
		return false
	}

	name := s.timestampFieldName()
	// One formatted value for the whole file — the per-row FormatField loop
	// this replaces was the fast path's dominant cost and scaled with RowCount.
	value := s.registry.FormatField(name, fi.MinTimeNs)

	chunk := int64(syntheticChunkSize)
	if total < chunk {
		chunk = total
	}
	values := make([]string, chunk)
	for i := range values {
		values[i] = value
	}
	cols := []logstorage.BlockColumn{{Name: name, Values: values}}
	var db logstorage.DataBlock
	db.SetColumns(cols)

	emitted := false
	for offset := int64(0); offset < total; offset += chunk {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		n := chunk
		if rem := total - offset; rem < n {
			n = rem
		}
		cols[0].Values = values[:n]
		writeBlock(0, &db)
		emitted = true
	}
	return emitted
}

// syntheticManifestBlock builds a single metadata-only block of at most
// syntheticChunkSize rows for fi. Query callers use streamConstTimeBlocks,
// which covers the file's full row count; this single-block form is kept for
// callers that only need one block's worth.
func (s *Storage) syntheticManifestBlock(fi manifest.FileInfo) *logstorage.DataBlock {
	n := fi.RowCount
	if n <= 0 {
		return nil
	}
	if n > syntheticChunkSize {
		n = syntheticChunkSize
	}
	name := s.timestampFieldName()
	value := s.registry.FormatField(name, fi.MinTimeNs)
	values := make([]string, n)
	for i := range values {
		values[i] = value
	}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{{Name: name, Values: values}})
	return db
}
