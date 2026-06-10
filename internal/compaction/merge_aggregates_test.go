package compaction

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// mergeFileLabelAggregates is the RETIRED production path (the compactor now
// extracts aggregates from the merged ROWS via schema.Extract*LabelAggregates —
// see compactGroup). It is kept here as the test-only cross-check for the
// equivalence regression in aggregate_healing_test.go: when every input file
// carries correct aggregates, summing the input maps and re-extracting from the
// merged rows must agree, because each input holds a disjoint set of rows.
func mergeFileLabelAggregates(files []manifest.FileInfo) map[string]map[string]int64 {
	merged := make(map[string]map[string]int64)
	for _, f := range files {
		for field, vals := range f.LabelAggregates {
			m, ok := merged[field]
			if !ok {
				m = make(map[string]int64)
				merged[field] = m
			}
			for v, c := range vals {
				m[v] += c
			}
		}
	}
	for field, m := range merged {
		if len(m) == 0 || len(m) > schema.MaxLabelAggregateValues {
			delete(merged, field)
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func TestMergeFileLabelAggregates_SumsAndCaps(t *testing.T) {
	files := []manifest.FileInfo{
		{LabelAggregates: map[string]map[string]int64{"service.name": {"api-gateway": 100, "user-service": 50}}},
		{LabelAggregates: map[string]map[string]int64{"service.name": {"api-gateway": 30, "order-service": 10}}},
		{LabelAggregates: nil}, // a file without aggregates contributes nothing
	}
	got := mergeFileLabelAggregates(files)
	sn := got["service.name"]
	if sn["api-gateway"] != 130 || sn["user-service"] != 50 || sn["order-service"] != 10 {
		t.Fatalf("merge sum wrong: %v", sn)
	}

	// Merging past the cap drops the field (high-cardinality after merge).
	var big []manifest.FileInfo
	for i := 0; i < 2; i++ {
		m := map[string]int64{}
		for j := 0; j < schema.MaxLabelAggregateValues; j++ {
			m[fmt.Sprintf("f%d-v%d", i, j)] = 1 // disjoint values across the two files
		}
		big = append(big, manifest.FileInfo{LabelAggregates: map[string]map[string]int64{"span.name": m}})
	}
	if _, present := mergeFileLabelAggregates(big)["span.name"]; present {
		t.Fatal("field exceeding the cap after merge must be dropped")
	}

	if mergeFileLabelAggregates(nil) != nil {
		t.Fatal("no files must return nil")
	}
}
