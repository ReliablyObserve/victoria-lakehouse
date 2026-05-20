package bloomindex

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestFilter_AddAndCheck(t *testing.T) {
	f := NewFilter(100, 0.01)
	f.Add("trace-abc123")
	f.Add("trace-def456")

	if !f.MayContain("trace-abc123") {
		t.Error("should contain trace-abc123")
	}
	if !f.MayContain("trace-def456") {
		t.Error("should contain trace-def456")
	}
	if f.MayContain("trace-nothere") {
		t.Error("should not contain trace-nothere (may be FP but unlikely with 100-item filter)")
	}
}

func TestFilter_FalsePositiveRate(t *testing.T) {
	n := 200
	f := NewFilter(n, 0.01)
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("trace-%06d", i))
	}

	fp := 0
	tests := 10000
	for i := n; i < n+tests; i++ {
		if f.MayContain(fmt.Sprintf("trace-%06d", i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(tests)
	if rate > 0.05 {
		t.Errorf("false positive rate too high: %.3f (want < 0.05)", rate)
	}
}

func TestFilter_MarshalUnmarshal(t *testing.T) {
	f := NewFilter(50, 0.01)
	f.Add("hello")
	f.Add("world")

	data := f.Marshal()
	f2, err := UnmarshalFilter(data)
	if err != nil {
		t.Fatal(err)
	}
	if !f2.MayContain("hello") {
		t.Error("deserialized filter should contain hello")
	}
	if !f2.MayContain("world") {
		t.Error("deserialized filter should contain world")
	}
}

func TestIndex_MultiColumn_MarshalUnmarshal(t *testing.T) {
	idx := New()

	f1 := NewFilter(10, 0.01)
	f1.Add("trace-aaa")
	f2 := NewFilter(5, 0.01)
	f2.Add("api-gateway")
	idx.AddColumns("partition/file1.parquet", map[string]*Filter{
		"trace_id":     f1,
		"service.name": f2,
	})

	f3 := NewFilter(10, 0.01)
	f3.Add("trace-bbb")
	f4 := NewFilter(5, 0.01)
	f4.Add("order-service")
	idx.AddColumns("partition/file2.parquet", map[string]*Filter{
		"trace_id":     f3,
		"service.name": f4,
	})

	data := idx.Marshal()
	idx2, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	if idx2.Len() != 2 {
		t.Errorf("expected 2 entries, got %d", idx2.Len())
	}

	keys := []string{"partition/file1.parquet", "partition/file2.parquet"}

	// trace_id check
	matches := idx2.MayContain(keys, "trace_id", "trace-aaa")
	if len(matches) != 1 || matches[0] != "partition/file1.parquet" {
		t.Errorf("expected only file1 for trace-aaa, got %v", matches)
	}

	// service.name check
	matches = idx2.MayContain(keys, "service.name", "order-service")
	if len(matches) != 1 || matches[0] != "partition/file2.parquet" {
		t.Errorf("expected only file2 for order-service, got %v", matches)
	}
}

func TestIndex_MayContainAll(t *testing.T) {
	idx := New()

	// file1: has api-gateway + trace-aaa
	idx.AddColumns("file1", map[string]*Filter{
		"trace_id":     filterWith("trace-aaa"),
		"service.name": filterWith("api-gateway"),
	})
	// file2: has order-service + trace-bbb
	idx.AddColumns("file2", map[string]*Filter{
		"trace_id":     filterWith("trace-bbb"),
		"service.name": filterWith("order-service"),
	})

	keys := []string{"file1", "file2"}

	// Single condition
	matches := idx.MayContainAll(keys, []ColumnCheck{
		{"service.name", "api-gateway"},
	})
	if len(matches) != 1 || matches[0] != "file1" {
		t.Errorf("single condition: expected [file1], got %v", matches)
	}

	// AND condition: trace-aaa + api-gateway → only file1
	matches = idx.MayContainAll(keys, []ColumnCheck{
		{"trace_id", "trace-aaa"},
		{"service.name", "api-gateway"},
	})
	if len(matches) != 1 || matches[0] != "file1" {
		t.Errorf("AND condition: expected [file1], got %v", matches)
	}

	// AND condition: trace-aaa + order-service → no file matches both
	matches = idx.MayContainAll(keys, []ColumnCheck{
		{"trace_id", "trace-aaa"},
		{"service.name", "order-service"},
	})
	if len(matches) != 0 {
		t.Errorf("contradicting AND: expected [], got %v", matches)
	}
}

func TestIndex_MayContain_MissingKey(t *testing.T) {
	idx := New()
	idx.Add("known.parquet", "trace_id", filterWith("val"))

	keys := []string{"known.parquet", "unknown.parquet"}
	matches := idx.MayContain(keys, "trace_id", "other-val")
	// unknown.parquet should always be included (conservative)
	found := false
	for _, m := range matches {
		if m == "unknown.parquet" {
			found = true
		}
	}
	if !found {
		t.Error("unknown keys should always be included in results")
	}
}

func TestIndex_MayContain_MissingColumn(t *testing.T) {
	idx := New()
	idx.Add("file1", "trace_id", filterWith("aaa"))

	keys := []string{"file1"}
	// Query a column that isn't indexed — file should be included
	matches := idx.MayContain(keys, "span.name", "GET /users")
	if len(matches) != 1 {
		t.Error("missing column should conservatively include the file")
	}
}

func TestIndex_MergeFrom(t *testing.T) {
	idx1 := New()
	idx1.Add("file1", "trace_id", filterWith("a"))

	idx2 := New()
	idx2.Add("file2", "trace_id", filterWith("b"))
	idx2.Add("file1", "service.name", filterWith("svc"))

	idx1.MergeFrom(idx2)
	if idx1.Len() != 2 {
		t.Errorf("expected 2 entries after merge, got %d", idx1.Len())
	}
	// file1 should now have both columns
	matches := idx1.MayContain([]string{"file1"}, "service.name", "svc")
	if len(matches) != 1 {
		t.Error("merged column should be queryable")
	}
}

func TestFilter_Size_ByCardinality(t *testing.T) {
	// Low cardinality (service.name): 5 items
	low := NewFilter(5, 0.01)
	// High cardinality (trace_id): 200 items
	high := NewFilter(200, 0.01)

	t.Logf("low cardinality (5 items): %d bytes", low.Size())
	t.Logf("high cardinality (200 items): %d bytes", high.Size())

	if low.Size() >= high.Size() {
		t.Error("low cardinality filter should be smaller")
	}
	if low.Size() > 20 {
		t.Errorf("5-item filter too large: %d bytes", low.Size())
	}
	if high.Size() > 300 {
		t.Errorf("200-item filter too large: %d bytes", high.Size())
	}
}

func TestUnmarshalV1_Compat(t *testing.T) {
	// Build a v1 format index manually
	idx := &Index{entries: make(map[string]map[string]*Filter)}
	idx.entries["file1"] = map[string]*Filter{"trace_id": filterWith("abc")}

	// Serialize as v1 (old format)
	buf := []byte{1} // version 1
	buf = binary.LittleEndian.AppendUint32(buf, 1)
	key := "file1"
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
	buf = append(buf, key...)
	fData := filterWith("abc").Marshal()
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(fData)))
	buf = append(buf, fData...)

	// Unmarshal should work and place filter under "trace_id"
	loaded, err := Unmarshal(buf)
	if err != nil {
		t.Fatal(err)
	}
	matches := loaded.MayContain([]string{"file1"}, "trace_id", "abc")
	if len(matches) != 1 {
		t.Error("v1 compat: should find trace_id via MayContain")
	}
}

func filterWith(values ...string) *Filter {
	f := NewFilter(len(values)+1, 0.01)
	for _, v := range values {
		f.Add(v)
	}
	return f
}

// TestFilter_MergeFrom_NilOther exercises the nil guard in Filter.MergeFrom (previously 71.4%).
func TestFilter_MergeFrom_NilOther(t *testing.T) {
	f := NewFilter(10, 0.01)
	f.Add("value1")
	// Merging nil should not panic and should leave filter unchanged.
	f.MergeFrom(nil)
	if !f.MayContain("value1") {
		t.Error("filter should still contain value1 after nil merge")
	}
}

// TestFilter_MergeFrom_DifferentSizes2 exercises size mismatch path in MergeFrom.
func TestFilter_MergeFrom_DifferentSizes2(t *testing.T) {
	f1 := NewFilter(10, 0.01)
	f1.Add("hello")

	f2 := NewFilter(1000, 0.01) // larger filter
	f2.Add("world")

	// Merge larger into smaller — should not panic.
	f1.MergeFrom(f2)
	// f1 should still contain "hello".
	if !f1.MayContain("hello") {
		t.Error("filter should still contain hello after merge from larger")
	}
}

// TestMayContainAll_EmptyChecks exercises the early-return for empty checks.
func TestMayContainAll_EmptyChecks(t *testing.T) {
	idx := New()
	idx.Add("f1.parquet", "trace_id", filterWith("abc"))

	keys := []string{"f1.parquet", "f2.parquet"}
	result := idx.MayContainAll(keys, nil)
	if len(result) != 2 {
		t.Errorf("empty checks should return all keys, got %d", len(result))
	}
}

// TestMayContainAll_MissingColumn exercises the "no filter for column" path.
func TestMayContainAll_MissingColumn(t *testing.T) {
	idx := New()
	cols := map[string]*Filter{
		"trace_id": NewFilter(10, 0.01),
	}
	cols["trace_id"].Add("abc")
	idx.AddColumns("f1.parquet", cols)

	// Check on a column that doesn't exist should not exclude the file.
	checks := []ColumnCheck{
		{Column: "nonexistent_col", Value: "anything"},
	}
	result := idx.MayContainAll([]string{"f1.parquet"}, checks)
	if len(result) != 1 {
		t.Errorf("missing column should not exclude file, got %d results", len(result))
	}
}

// TestUnmarshal_UnknownVersion exercises the unknown version error path.
func TestUnmarshal_UnknownVersion(t *testing.T) {
	// Create data with version byte = 99.
	data := make([]byte, 10)
	data[0] = 99 // unknown version
	_, err := Unmarshal(data)
	if err == nil {
		t.Error("expected error for unknown version")
	}
}

// TestUnmarshal_TooShort exercises the too-short data error path.
func TestUnmarshal_TooShort(t *testing.T) {
	_, err := Unmarshal([]byte{1, 2})
	if err == nil {
		t.Error("expected error for too-short data")
	}
}

// TestUnmarshalV1_TruncatedKey exercises truncated key error path.
func TestUnmarshalV1_TruncatedKey(t *testing.T) {
	// v1 header: [version=1][count=1 as uint32][keyLen=1000 as uint16] but no key data
	data := make([]byte, 7)
	data[0] = 1                                    // version
	binary.LittleEndian.PutUint32(data[1:5], 1)    // count = 1
	binary.LittleEndian.PutUint16(data[5:7], 1000) // keyLen = 1000 (truncated)
	_, err := unmarshalV1(data)
	if err == nil {
		t.Error("expected error for truncated key in v1")
	}
}

// TestUnmarshalV1_MaxEntriesExceeded exercises the max entries guard.
func TestUnmarshalV1_MaxEntriesExceeded(t *testing.T) {
	data := make([]byte, 10)
	data[0] = 1
	binary.LittleEndian.PutUint32(data[1:5], 2_000_000) // count > maxIndexEntries
	_, err := unmarshalV1(data)
	if err == nil {
		t.Error("expected error for count exceeding max")
	}
}

// TestUnmarshalV2_MaxEntriesExceeded exercises the max entries guard in v2.
func TestUnmarshalV2_MaxEntriesExceeded(t *testing.T) {
	data := make([]byte, 10)
	data[0] = 2
	binary.LittleEndian.PutUint32(data[1:5], 2_000_000) // count > maxIndexEntries
	_, err := unmarshalV2(data)
	if err == nil {
		t.Error("expected error for count exceeding max in v2")
	}
}

// TestUnmarshalV2_TruncatedData exercises various truncation paths in v2.
func TestUnmarshalV2_TruncatedData(t *testing.T) {
	// Truncated at key length position.
	data := make([]byte, 5)
	data[0] = 2
	binary.LittleEndian.PutUint32(data[1:5], 1) // count = 1, but no key data
	_, err := unmarshalV2(data)
	if err == nil {
		t.Error("expected error for truncated v2 data")
	}
}

// TestIndex_MergeFrom_NilOther exercises nil guard in Index.MergeFrom.
func TestIndex_MergeFrom_NilOther(t *testing.T) {
	idx := New()
	idx.Add("f1", "trace_id", filterWith("abc"))

	// Merging nil should not panic.
	idx.MergeFrom(nil)
	if idx.Len() != 1 {
		t.Errorf("len = %d after nil merge, want 1", idx.Len())
	}
}

// TestIndex_AddColumns_EmptyColumns exercises the early-return for empty columns map.
func TestIndex_AddColumns_EmptyColumns(t *testing.T) {
	idx := New()
	// Adding empty columns should be a no-op.
	idx.AddColumns("f1.parquet", map[string]*Filter{})
	if idx.Len() != 0 {
		t.Errorf("empty columns should not add entry, len = %d", idx.Len())
	}
	// Adding nil columns should also be a no-op.
	idx.AddColumns("f1.parquet", nil)
	if idx.Len() != 0 {
		t.Errorf("nil columns should not add entry, len = %d", idx.Len())
	}
}

// TestNewPartitionedIndex_InvalidFPRate exercises boundary validation for fpRate.
func TestNewPartitionedIndex_InvalidFPRate(t *testing.T) {
	// fpRate <= 0 should use default 0.01
	pi := NewPartitionedIndex(GranularityHour, 0)
	if pi == nil {
		t.Fatal("expected non-nil PartitionedIndex")
	}

	// fpRate >= 1 should use default 0.01
	pi2 := NewPartitionedIndex(GranularityHour, 1.5)
	if pi2 == nil {
		t.Fatal("expected non-nil PartitionedIndex for invalid rate")
	}
}

func BenchmarkFilter_MayContain(b *testing.B) {
	f := NewFilter(200, 0.01)
	for i := 0; i < 200; i++ {
		f.Add(fmt.Sprintf("trace-%032x", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.MayContain("trace-00000000000000000000000000000001")
	}
}

func BenchmarkIndex_MayContainAll_MultiColumn(b *testing.B) {
	idx := New()
	keys := make([]string, 360)
	for i := 0; i < 360; i++ {
		cols := map[string]*Filter{
			"trace_id":     NewFilter(200, 0.01),
			"service.name": NewFilter(5, 0.01),
			"span.name":    NewFilter(10, 0.01),
		}
		for j := 0; j < 200; j++ {
			cols["trace_id"].Add(fmt.Sprintf("trace-%d-%d", i, j))
		}
		cols["service.name"].Add("api-gateway")
		cols["service.name"].Add("order-service")
		cols["span.name"].Add("GET /api/users")

		key := fmt.Sprintf("partition/hour=21/file%d.parquet", i)
		idx.AddColumns(key, cols)
		keys[i] = key
	}
	checks := []ColumnCheck{
		{"trace_id", "trace-180-100"},
		{"service.name", "api-gateway"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.MayContainAll(keys, checks)
	}
}
