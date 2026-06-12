package schema

// MaxLabelAggregateValues caps the number of distinct values a single field may
// carry in a label-aggregate map. A field that exceeds it is high-cardinality
// (e.g. trace_id) — not a useful manifest group-by — and dropping it bounds
// manifest growth. The flush writer, the compactor, and the manifest-side merge
// all share this ONE constant so a compacted file is never "more complete" than
// a freshly flushed one (and vice versa).
const MaxLabelAggregateValues = 100

// ExtractLogLabelAggregates counts rows per (field, value) for the same
// low-cardinality fields the inverted label index covers. The result powers
// manifest-side `stats count() by (field)` so that query answers from metadata
// without opening any Parquet file (PERF-2). A field whose distinct-value count
// exceeds MaxLabelAggregateValues is dropped — it's high-cardinality (e.g.
// trace_id), not a useful group-by, and dropping it bounds manifest growth.
// Returns nil if there's nothing to aggregate.
//
// This is the ONE implementation shared by the flush writer (both modules) and
// the compactor: the compactor extracts from the merged ROWS — identical field
// list, identical cap — so compaction HEALS files whose FileInfo carries no
// aggregates (the pre-#138-fix wipe) instead of propagating the empty maps.
func ExtractLogLabelAggregates(rows []LogRow) map[string]map[string]int64 {
	if len(rows) == 0 {
		return nil
	}
	agg := map[string]map[string]int64{}
	for i := range rows {
		for _, c := range LogLabelColumns {
			countLabelAggregate(agg, c.Name, c.Get(&rows[i]))
		}
	}
	return capLabelAggregates(agg)
}

// ExtractTraceLabelAggregates is the traces counterpart. Both extractors and the
// inverted cardinality index draw from the SAME column set (LogLabelColumns /
// TraceLabelColumns in label_columns.go) so they can never disagree.
func ExtractTraceLabelAggregates(rows []TraceRow) map[string]map[string]int64 {
	if len(rows) == 0 {
		return nil
	}
	agg := map[string]map[string]int64{}
	for i := range rows {
		for _, c := range TraceLabelColumns {
			countLabelAggregate(agg, c.Name, c.Get(&rows[i]))
		}
	}
	return capLabelAggregates(agg)
}

func countLabelAggregate(agg map[string]map[string]int64, field, value string) {
	if value == "" {
		return
	}
	m, ok := agg[field]
	if !ok {
		m = make(map[string]int64)
		agg[field] = m
	}
	m[value]++
}

// capLabelAggregates drops any field with more than MaxLabelAggregateValues
// distinct values (high-cardinality → not a useful manifest group-by, unbounded
// growth) and drops fields that ended up empty. Returns nil if nothing survives.
func capLabelAggregates(agg map[string]map[string]int64) map[string]map[string]int64 {
	for field, vals := range agg {
		if len(vals) == 0 || len(vals) > MaxLabelAggregateValues {
			delete(agg, field)
		}
	}
	if len(agg) == 0 {
		return nil
	}
	return agg
}
