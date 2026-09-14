package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// maxLabelsPerField caps per-field distinct values in the label SETS, and is
// deliberately the same constant as the label-AGGREGATE cap so the inverted
// index and the aggregate fast-path never disagree on which fields are
// "low-cardinality enough" to keep.
const maxLabelsPerField = schema.MaxLabelAggregateValues

// extractLogLabels / extractTraceLabels delegate to the shared implementation
// in internal/schema. The flush writer (both modules), the compactor and the
// delete rewriter all record these label sets into the same manifest field and
// feed them to the same pmeta catalog, so they must compute them identically —
// see schema.ExtractLogLabels.
func extractLogLabels(rows []schema.LogRow) map[string][]string {
	return schema.ExtractLogLabels(rows)
}

func extractTraceLabels(rows []schema.TraceRow) map[string][]string {
	return schema.ExtractTraceLabels(rows)
}

// Label AGGREGATES (per-(field,value) row counts) live in
// internal/schema/label_aggregates.go: schema.ExtractLogLabelAggregates /
// schema.ExtractTraceLabelAggregates are the ONE shared implementation used by
// the flush writers (both modules) AND the compactor, so compaction extracts
// from merged rows with the exact field list and cap the flush path uses.
