package parquets3

import (
	"context"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// Gap-discipline constants (planned-fetch v2 slice 1c). armProjectedPlan
// prices the plan at each candidate gap — pure in-memory math over the
// already-parsed ranges — and fetches with the cheapest:
//
//	cost(gap) = ceil(spans/k) * RTT_assumed + bytes / BW_assumed
//
// RTT_assumed = 100 ms and BW_assumed = 50 MB/s per connection are the
// offline planner simulator's constants (worst-credible S3 round trip;
// single-connection S3 throughput). The spans*RTT term is amortized over
// the k = s3.planned_fetch_max_inflight concurrent span GETs the fetch
// actually issues — ceil(spans/k) serial RTT WAVES — because pricing
// serial RTTs the fetch doesn't pay would make the largest gap win always
// and the discipline a no-op. The candidates deliberately exclude the
// simulator's proven trap (>= the ~1.9MB cross-RG stride merges to
// whole-file, wasting 74-98% of bytes); the operative levers are span
// concurrency and the cap scope, the gap only needs to not be pathological.
const (
	plannedGapRTTAssumedSec  = 0.100            // 100 ms per serial RTT wave
	plannedGapBWAssumedBytes = 50 * 1024 * 1024 // 50 MB/s per connection
)

// plannedGapCandidates are the priced gaps, smallest first — ties go to
// the smaller gap (fewer bytes on the wire for the same wave count).
var plannedGapCandidates = []int64{64 << 10, 256 << 10, 1 << 20}

// plannedGapLabel maps a candidate gap to its metric label
// (lakehouse_s3_planned_gap_choice_total{gap=...}).
func plannedGapLabel(gap int64) string {
	switch gap {
	case 64 << 10:
		return "64k"
	case 256 << 10:
		return "256k"
	default:
		return "1m"
	}
}

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
// into an out-of-plan GET). The plan is priced at each gap-discipline
// candidate (cost model documented at plannedGapCandidates) and fetched
// with the cheapest gap; spans above s3.planned_fetch_span_cap_bytes are
// split, never rejected (the per-PLAN cap is retired — plan admission is
// the memory ledger's: view bytes are charged to the same fileBudget the
// decode admission reads, and the file worker already holds an fi.Size
// admission that subsumes any plan).
//
// Fallback ladder (all rollbacks keep the query correct):
//   - plan over the absolute ceiling fi.Size (defensive — spans are
//     file-clamped and disjoint, so this cannot fire on a well-formed
//     footer) → reason="cap", the view is redirected to the
//     adaptive-window stack (the exact reader stack the "window" mode /
//     openRangedParquet builds);
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

	// Gap discipline (slice 1c): price the plan at each candidate gap and
	// fetch with the cheapest.
	k := s.cfg.S3.PlannedFetchMaxInflight
	if k <= 0 {
		k = 16
	}
	bestGap, bestTotal := choosePlannedGap(view, ranges, k)
	view.SetGap(bestGap)
	metrics.S3PlannedGapChoice.Inc(plannedGapLabel(bestGap))

	// Absolute plan ceiling = fi.Size (defensive). The old per-plan 16MB
	// cap is retired: it punished exactly the cross-RG coalescing that
	// cuts GETs (merged gap bytes counted into the plan total).
	if bestTotal > view.Size() {
		metrics.S3ProjectedFetchFallback.Inc("cap")
		s.fallbackPlannedToWindow(view)
		return
	}

	if err := view.Fetch(ctx, ranges); err != nil {
		metrics.S3ProjectedFetchFallback.Inc("error")
		s.fallbackPlannedToWindow(view)
	}
}

// choosePlannedGap prices the plan at each gap-discipline candidate and
// returns the cheapest gap and that plan's total bytes (model documented
// at plannedGapCandidates: ceil(spans/k) RTT waves + bytes/BW). Ties go to
// the smaller gap — fewer bytes on the wire for the same wave count
// (candidates iterate smallest-first; strict < keeps the earlier winner).
// Pure in-memory math over the already-parsed ranges; no I/O.
func choosePlannedGap(view *s3reader.PlannedFetchReaderAt, ranges []s3reader.Range, k int) (bestGap, bestTotal int64) {
	if k <= 0 {
		k = 16
	}
	bestCost := 0.0
	for _, gap := range plannedGapCandidates {
		spans, total := view.PlanRangesAt(ranges, gap)
		waves := (len(spans) + k - 1) / k
		cost := float64(waves)*plannedGapRTTAssumedSec + float64(total)/plannedGapBWAssumedBytes
		if bestGap == 0 || cost < bestCost {
			bestGap, bestTotal, bestCost = gap, total, cost
		}
	}
	return bestGap, bestTotal
}

// fallbackPlannedToWindow redirects every subsequent read of the planned
// view to a freshly-built adaptive-window stack over the SAME raw phased
// reader — the byte-identical rollback to projected_fetch_mode: window,
// with no second S3 open.
func (s *Storage) fallbackPlannedToWindow(view *s3reader.PlannedFetchReaderAt) {
	view.Redirect(s.buildWindowReader(view.Inner(), view.Size()))
}
