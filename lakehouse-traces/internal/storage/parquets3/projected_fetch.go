package parquets3

import (
	"context"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// defaultProjectedFetchMaxBytes is the per-file plan cap when
// S3Config.ProjectedFetchMaxBytes is unset: a plan-then-fetch projected
// read may pin at most this many coalesced span bytes in memory. Plans
// above the cap fall back to the adaptive-window path
// (lakehouse_s3_projected_fetch_fallback_total{reason="cap"}).
const defaultProjectedFetchMaxBytes = 16 * 1024 * 1024

// indexedRowGroup pairs a parquet.RowGroup with its FOOTER ORDINAL so the
// row-group pre-filter can prune/sort freely while armProjectedPlan can
// still map each survivor back to meta.RowGroups[idx] for its column-chunk
// byte ranges.
type indexedRowGroup struct {
	idx int
	rg  parquet.RowGroup
}

// planProjectedRanges derives the exact byte ranges a projected read of
// the given row groups needs, from the parquet footer metadata of the
// already-open file (s3-optimization research Tier-2 items 8/9 — the CH
// plan-then-fetch / arrow-rs vectored per-RG pattern). Per matched row
// group and per planned column it returns:
//
//   - the column chunk's page bytes: [min(DictionaryPageOffset,
//     DataPageOffset), +TotalCompressedSize] — DICTIONARY PAGE INCLUDED
//     (DictionaryPageOffset precedes DataPageOffset when the chunk is
//     dictionary-encoded; TotalCompressedSize covers all pages);
//   - the chunk's ColumnIndex/OffsetIndex sections when present:
//     SkipPageIndex(true) defers them to the first ColumnIndex()/
//     OffsetIndex() call (detectConstantColumns, null-count crediting),
//     and including these tiny near-footer sections keeps those lazy
//     reads in-plan instead of falling through as out-of-plan GETs.
//
// planCols keys are TOP-LEVEL column names, matching PathInSchema[0]
// (the same convention as allLeafColumns / findColumnIndex).
func planProjectedRanges(f *parquet.File, rgIdxs []int, planCols map[string]bool) []s3reader.Range {
	meta := f.Metadata()
	if meta == nil || len(planCols) == 0 {
		return nil
	}
	var ranges []s3reader.Range
	for _, ri := range rgIdxs {
		if ri < 0 || ri >= len(meta.RowGroups) {
			continue
		}
		columns := meta.RowGroups[ri].Columns
		for ci := range columns {
			cc := &columns[ci]
			md := &cc.MetaData
			if len(md.PathInSchema) == 0 || !planCols[md.PathInSchema[0]] {
				continue
			}
			start := md.DataPageOffset
			if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
				start = md.DictionaryPageOffset
			}
			if md.TotalCompressedSize > 0 && start >= 0 {
				ranges = append(ranges, s3reader.Range{Off: start, Len: md.TotalCompressedSize})
			}
			if cc.ColumnIndexOffset > 0 && cc.ColumnIndexLength > 0 {
				ranges = append(ranges, s3reader.Range{Off: cc.ColumnIndexOffset, Len: int64(cc.ColumnIndexLength)})
			}
			if cc.OffsetIndexOffset > 0 && cc.OffsetIndexLength > 0 {
				ranges = append(ranges, s3reader.Range{Off: cc.OffsetIndexOffset, Len: int64(cc.OffsetIndexLength)})
			}
		}
	}
	return ranges
}

// armProjectedPlan prices and fetches the plan for a projected read: the
// planned columns are the PROJECTED columns plus any push-down filter
// columns (prewhereFilter reads the pdf columns' pages to build its row
// bitmap — leaving them out of the plan would turn every prewhere read
// into an out-of-plan GET).
//
// Fallback ladder (all rollbacks keep the query correct):
//   - plan over s3.projected_fetch_max_bytes → reason="cap", the view is
//     redirected to the adaptive-window stack (the exact reader stack the
//     "window" mode / openRangedParquet builds);
//   - span download error → reason="error", same redirect;
//   - empty plan (no matched RGs / no overlapping columns) → the view
//     stays a passthrough (exact-range GETs, no window).
//
// NOTE constant columns are NOT excluded from the plan: detecting them
// here would force ColumnIndex reads at plan time (one GET per column),
// costing more round trips than the few KB of unread constant-chunk bytes
// the coalesced fetch pulls.
func (s *Storage) armProjectedPlan(ctx context.Context, view *s3reader.PlannedFetchReaderAt, f *parquet.File, rgIdxs []int, projected map[string]bool, pdf *PushDownFilter) {
	if view == nil || len(projected) == 0 || len(rgIdxs) == 0 {
		return
	}
	planCols := make(map[string]bool, len(projected)+2)
	for c := range projected {
		planCols[c] = true
	}
	if pdf != nil {
		for i := range pdf.Checks {
			planCols[pdf.Checks[i].Column] = true
		}
	}

	ranges := planProjectedRanges(f, rgIdxs, planCols)
	if len(ranges) == 0 {
		return
	}

	maxBytes := int64(s.cfg.S3.ProjectedFetchMaxBytes)
	if maxBytes <= 0 {
		maxBytes = defaultProjectedFetchMaxBytes
	}
	if _, total := view.PlanRanges(ranges); total > maxBytes {
		metrics.S3ProjectedFetchFallback.Inc("cap")
		s.fallbackPlannedToWindow(view)
		return
	}

	if err := view.Fetch(ctx, ranges); err != nil {
		metrics.S3ProjectedFetchFallback.Inc("error")
		s.fallbackPlannedToWindow(view)
	}
}

// fallbackPlannedToWindow redirects every subsequent read of the planned
// view to a freshly-built adaptive-window stack over the SAME raw phased
// reader — the byte-identical rollback to projected_fetch_mode: window,
// with no second S3 open.
func (s *Storage) fallbackPlannedToWindow(view *s3reader.PlannedFetchReaderAt) {
	view.Redirect(s.buildWindowReader(view.Inner(), view.Size()))
}
