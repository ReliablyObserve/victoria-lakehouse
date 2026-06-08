package compaction

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

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
		for j := 0; j < maxLabelAggregateValues; j++ {
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
