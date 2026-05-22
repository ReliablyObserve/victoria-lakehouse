package bloomindex

import "testing"

func TestNewFileBloomIndex(t *testing.T) {
	values := map[string][]string{
		"trace_id":     {"abc123", "def456", "ghi789"},
		"service.name": {"api-gateway", "worker"},
	}
	idx := NewFileBloomIndex(values, 0.01)

	// Should find added values
	if !FileBloomMayContain(idx, "trace_id", "abc123") {
		t.Error("should contain abc123")
	}
	if !FileBloomMayContain(idx, "service.name", "api-gateway") {
		t.Error("should contain api-gateway")
	}

	// Should reject values not added (low fp rate)
	if FileBloomMayContain(idx, "trace_id", "nonexistent-trace") {
		t.Error("should not contain nonexistent-trace")
	}

	// Unknown column — assume present
	if !FileBloomMayContain(idx, "unknown_col", "anything") {
		t.Error("unknown column should assume present")
	}
}

func TestFileBloomIndex_MarshalRoundTrip(t *testing.T) {
	values := map[string][]string{
		"trace_id":     {"abc123", "def456"},
		"service.name": {"api-gateway"},
	}
	idx := NewFileBloomIndex(values, 0.01)

	data := idx.Marshal()
	if len(data) == 0 {
		t.Fatal("marshal returned empty bytes")
	}

	idx2, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !FileBloomMayContain(idx2, "trace_id", "abc123") {
		t.Error("roundtrip: should contain abc123")
	}
	if !FileBloomMayContain(idx2, "service.name", "api-gateway") {
		t.Error("roundtrip: should contain api-gateway")
	}
	if FileBloomMayContain(idx2, "trace_id", "nonexistent") {
		t.Error("roundtrip: should not contain nonexistent")
	}
}

func TestFileBloomMayContainAll(t *testing.T) {
	values := map[string][]string{
		"trace_id":     {"abc123"},
		"service.name": {"api-gateway"},
	}
	idx := NewFileBloomIndex(values, 0.01)

	// Both match
	if !FileBloomMayContainAll(idx, []ColumnCheck{
		{Column: "trace_id", Value: "abc123"},
		{Column: "service.name", Value: "api-gateway"},
	}) {
		t.Error("both values present, should match")
	}

	// One doesn't match
	if FileBloomMayContainAll(idx, []ColumnCheck{
		{Column: "trace_id", Value: "abc123"},
		{Column: "service.name", Value: "nonexistent"},
	}) {
		t.Error("one value missing, should not match")
	}
}
