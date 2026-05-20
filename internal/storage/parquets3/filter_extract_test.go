package parquets3

import "testing"

func TestExtractExactMatch_QuotedExact(t *testing.T) {
	val := extractExactMatch(`trace_id:="abc123"`, "trace_id")
	if val != "abc123" {
		t.Errorf("expected abc123, got %q", val)
	}
}

func TestExtractExactMatch_NoMatch(t *testing.T) {
	val := extractExactMatch(`service.name:="api"`, "trace_id")
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}
}

func TestExtractInValues_Basic(t *testing.T) {
	vals := extractInValues(`service.name:in("api","web","worker")`, "service.name")
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d: %v", len(vals), vals)
	}
	expected := map[string]bool{"api": true, "web": true, "worker": true}
	for _, v := range vals {
		if !expected[v] {
			t.Errorf("unexpected value %q", v)
		}
	}
}

func TestExtractInValues_NoMatch(t *testing.T) {
	vals := extractInValues(`trace_id:="abc"`, "service.name")
	if len(vals) != 0 {
		t.Errorf("expected 0 values, got %d", len(vals))
	}
}

func TestExtractInValues_SingleValue(t *testing.T) {
	vals := extractInValues(`service.name:in("api")`, "service.name")
	if len(vals) != 1 || vals[0] != "api" {
		t.Errorf("expected [api], got %v", vals)
	}
}

func TestExtractFilterValues_CombinesExactAndIn(t *testing.T) {
	vals := extractFilterValues(`service.name:="api"`, "service.name")
	if len(vals) != 1 || vals[0] != "api" {
		t.Errorf("expected [api] from exact match, got %v", vals)
	}

	vals = extractFilterValues(`service.name:in("api","web")`, "service.name")
	if len(vals) != 2 {
		t.Errorf("expected 2 values from in(), got %d", len(vals))
	}
}
