package bloomindex

import (
	"fmt"
	"testing"
)

func TestPartitionedIndex_AddFile(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	pi.AddFile("dt=2026-05-02/hour=10", "file1.parquet", map[string][]string{
		"trace_id":     {"trace-aaa", "trace-bbb"},
		"service.name": {"api-gateway"},
	})

	idx := pi.GetPartition("dt=2026-05-02/hour=10")
	if idx == nil {
		t.Fatal("partition not created")
	}
	if idx.Len() != 1 {
		t.Errorf("want 1 file entry, got %d", idx.Len())
	}

	result := idx.MayContain([]string{"file1.parquet"}, "trace_id", "trace-aaa")
	if len(result) != 1 {
		t.Error("bloom should contain trace-aaa")
	}
}

func TestPartitionedIndex_DirtyTracking(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	pi.AddFile("p1", "f1", map[string][]string{"trace_id": {"a"}})
	pi.AddFile("p2", "f2", map[string][]string{"trace_id": {"b"}})

	dirty := pi.DirtyPartitions()
	if len(dirty) != 2 {
		t.Errorf("want 2 dirty, got %d", len(dirty))
	}

	pi.ClearDirty("p1")
	dirty = pi.DirtyPartitions()
	if len(dirty) != 1 {
		t.Errorf("want 1 dirty after clear, got %d", len(dirty))
	}
}

func TestPartitionedIndex_PartitionKey_Hourly(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	tests := []struct {
		key  string
		want string
	}{
		{"dt=2026-05-02/hour=10/abc.parquet", "dt=2026-05-02/hour=10"},
		{"dt=2026-05-02/hour=0/xyz.parquet", "dt=2026-05-02/hour=0"},
		{"dt=2026-05-02/hour=23/file.parquet", "dt=2026-05-02/hour=23"},
		{"nohour.parquet", "nohour.parquet"},
	}

	for _, tt := range tests {
		got := pi.PartitionKey(tt.key)
		if got != tt.want {
			t.Errorf("PartitionKey(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestPartitionedIndex_PartitionKey_Daily(t *testing.T) {
	pi := NewPartitionedIndex(GranularityDay, 0.01)

	tests := []struct {
		key  string
		want string
	}{
		{"dt=2026-05-02/hour=10/abc.parquet", "dt=2026-05-02"},
		{"dt=2026-05-02/hour=0/xyz.parquet", "dt=2026-05-02"},
		{"dt=2026-05-02", "dt=2026-05-02"},
	}

	for _, tt := range tests {
		got := pi.PartitionKey(tt.key)
		if got != tt.want {
			t.Errorf("PartitionKey(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestPartitionedIndex_MarshalPartition(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	pi.AddFile("p1", "f1", map[string][]string{"trace_id": {"t1", "t2"}})
	data := pi.MarshalPartition("p1")
	if len(data) == 0 {
		t.Fatal("marshal returned empty data")
	}

	idx, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Len() != 1 {
		t.Errorf("unmarshaled: want 1 entry, got %d", idx.Len())
	}

	// Non-existent partition
	data = pi.MarshalPartition("nonexistent")
	if data != nil {
		t.Error("non-existent partition should return nil")
	}
}

func TestPartitionedIndex_HighCardinality_Skipped(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	// Generate >50K values for one column
	highCard := make([]string, 50001)
	for i := range highCard {
		highCard[i] = fmt.Sprintf("val-%d", i)
	}

	pi.AddFile("p1", "f1", map[string][]string{
		"high_col": highCard,
		"low_col":  {"a", "b", "c"},
	})

	idx := pi.GetPartition("p1")
	if idx == nil {
		t.Fatal("partition should exist")
	}

	// High cardinality column should be skipped
	result := idx.MayContain([]string{"f1"}, "high_col", "val-0")
	if len(result) != 1 {
		t.Error("high cardinality column should not have bloom (conservative include)")
	}

	// Low cardinality column should have bloom
	result = idx.MayContain([]string{"f1"}, "low_col", "a")
	if len(result) != 1 {
		t.Error("low cardinality column should have bloom and match")
	}
	result = idx.MayContain([]string{"f1"}, "low_col", "z")
	if len(result) != 0 {
		t.Error("low cardinality column should exclude non-member")
	}
}

func TestPartitionedIndex_MultipleFiles(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	for i := 0; i < 100; i++ {
		pi.AddFile("p1", fmt.Sprintf("file%d", i), map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
		})
	}

	idx := pi.GetPartition("p1")
	if idx.Len() != 100 {
		t.Errorf("want 100 entries, got %d", idx.Len())
	}

	// Query specific trace — must include file42, may include FPs
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("file%d", i)
	}
	result := idx.MayContain(keys, "trace_id", "trace-42")
	found := false
	for _, k := range result {
		if k == "file42" {
			found = true
			break
		}
	}
	if !found {
		t.Error("should find trace-42 in file42")
	}
	// FP rate should be reasonable (< 20% of 100 files)
	if len(result) > 20 {
		t.Errorf("too many false positives: %d/100", len(result))
	}
}

func TestPartitionedIndex_RemovePartition(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	pi.AddFile("p1", "f1", map[string][]string{"trace_id": {"a"}})
	pi.AddFile("p2", "f2", map[string][]string{"trace_id": {"b"}})

	if pi.Len() != 2 {
		t.Fatalf("want 2 partitions, got %d", pi.Len())
	}

	pi.RemovePartition("p1")
	if pi.Len() != 1 {
		t.Errorf("want 1 partition after remove, got %d", pi.Len())
	}
	if pi.GetPartition("p1") != nil {
		t.Error("removed partition should be nil")
	}
}

func TestPartitionedIndex_SetPartition(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	idx := New()
	idx.Add("f1", "trace_id", filterWith("external"))

	pi.SetPartition("p1", idx)
	if pi.GetPartition("p1") == nil {
		t.Fatal("set partition not found")
	}

	dirty := pi.DirtyPartitions()
	if len(dirty) != 1 {
		t.Error("SetPartition should mark dirty")
	}
}
