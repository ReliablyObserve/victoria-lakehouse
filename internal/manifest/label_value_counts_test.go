package manifest

import "testing"

// TestLabelValueCounts_SumsAggregatesAcrossFiles locks the Storage Breakdown's
// metadata source: LabelValueCounts sums per-(field,value) row counts across all
// files' LabelAggregates — including dedicated dimensional columns — so a field
// like k8s.cluster.name breaks down with real per-value counts instead of blank.
func TestLabelValueCounts_SumsAggregatesAcrossFiles(t *testing.T) {
	m := New("bucket", "")
	m.AddFile("dt=2026-06-01/hour=00", FileInfo{Key: "0/0/dt=2026-06-01/hour=00/a.parquet", Size: 10, RawBytes: 100, RowCount: 5,
		LabelAggregates: map[string]map[string]int64{"k8s.cluster.name": {"prod-us-east-1": 3, "prod-eu-west-1": 2}}})
	m.AddFile("dt=2026-06-02/hour=00", FileInfo{Key: "0/0/dt=2026-06-02/hour=00/b.parquet", Size: 10, RawBytes: 100, RowCount: 4,
		LabelAggregates: map[string]map[string]int64{"k8s.cluster.name": {"prod-eu-west-1": 4}}})

	got := m.LabelValueCounts("k8s.cluster.name")
	if got["prod-us-east-1"] != 3 || got["prod-eu-west-1"] != 6 {
		t.Errorf("LabelValueCounts = %v, want {prod-us-east-1:3, prod-eu-west-1:6 (2+4 across files)}", got)
	}
	if len(m.LabelValueCounts("absent.field")) != 0 {
		t.Error("absent field should yield empty counts")
	}
}
