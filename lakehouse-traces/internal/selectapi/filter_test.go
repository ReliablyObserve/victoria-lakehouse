package selectapi

import (
	"testing"
)

func TestParseFilter_MatchAll(t *testing.T) {
	for _, q := range []string{"*", "", "  ", "  *  "} {
		node := ParseFilter(q)
		if node == nil {
			t.Fatalf("ParseFilter(%q) returned nil", q)
		}
		if node.Op != FilterMatchAll {
			t.Errorf("ParseFilter(%q).Op = %v, want FilterMatchAll", q, node.Op)
		}
	}
}

func TestParseFilter_ExactMatch(t *testing.T) {
	node := ParseFilter(`service.name:="api-gateway"`)
	if node.Op != FilterExact {
		t.Fatalf("expected FilterExact, got %v", node.Op)
	}
	if node.Field != "service.name" {
		t.Errorf("field = %q, want %q", node.Field, "service.name")
	}
	if node.Value != "api-gateway" {
		t.Errorf("value = %q, want %q", node.Value, "api-gateway")
	}
}

func TestParseFilter_Substring(t *testing.T) {
	node := ParseFilter(`_msg:error`)
	if node.Op != FilterSubstring {
		t.Fatalf("expected FilterSubstring, got %v", node.Op)
	}
	if node.Field != "_msg" {
		t.Errorf("field = %q, want %q", node.Field, "_msg")
	}
	if node.Value != "error" {
		t.Errorf("value = %q, want %q", node.Value, "error")
	}
}

func TestParseFilter_Regex(t *testing.T) {
	node := ParseFilter(`_msg:~"fail.*EOF"`)
	if node.Op != FilterRegex {
		t.Fatalf("expected FilterRegex, got %v", node.Op)
	}
	if node.Field != "_msg" {
		t.Errorf("field = %q, want %q", node.Field, "_msg")
	}
	if node.Regex == nil {
		t.Fatal("regex is nil")
	}
	if !node.Regex.MatchString("some failure at EOF") {
		t.Error("regex should match 'some failure at EOF'")
	}
	if node.Regex.MatchString("success") {
		t.Error("regex should not match 'success'")
	}
}

func TestParseFilter_AND(t *testing.T) {
	node := ParseFilter(`service.name:="api-gateway" AND level:="ERROR"`)
	if node.Op != FilterAnd {
		t.Fatalf("expected FilterAnd, got %v", node.Op)
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
	if node.Children[0].Op != FilterExact || node.Children[0].Field != "service.name" {
		t.Errorf("child[0] unexpected: op=%v field=%q", node.Children[0].Op, node.Children[0].Field)
	}
	if node.Children[1].Op != FilterExact || node.Children[1].Field != "level" {
		t.Errorf("child[1] unexpected: op=%v field=%q", node.Children[1].Op, node.Children[1].Field)
	}
}

func TestParseFilter_OR(t *testing.T) {
	node := ParseFilter(`service.name:="api-gateway" OR service.name:="user-service"`)
	if node.Op != FilterOr {
		t.Fatalf("expected FilterOr, got %v", node.Op)
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
}

func TestParseFilter_NOT(t *testing.T) {
	node := ParseFilter(`NOT level:="DEBUG"`)
	if node.Op != FilterNot {
		t.Fatalf("expected FilterNot, got %v", node.Op)
	}
	if len(node.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(node.Children))
	}
	if node.Children[0].Op != FilterExact || node.Children[0].Value != "DEBUG" {
		t.Errorf("child unexpected: op=%v value=%q", node.Children[0].Op, node.Children[0].Value)
	}
}

func TestParseFilter_ComplexGrouping(t *testing.T) {
	node := ParseFilter(`(service.name:="api-gateway" OR service.name:="user-service") AND level:="ERROR"`)
	if node.Op != FilterAnd {
		t.Fatalf("expected FilterAnd at root, got %v", node.Op)
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
	orNode := node.Children[0]
	if orNode.Op != FilterOr {
		t.Fatalf("expected FilterOr as first child, got %v", orNode.Op)
	}
	if len(orNode.Children) != 2 {
		t.Fatalf("expected 2 OR children, got %d", len(orNode.Children))
	}
	if node.Children[1].Op != FilterExact || node.Children[1].Field != "level" {
		t.Errorf("second child unexpected: op=%v field=%q", node.Children[1].Op, node.Children[1].Field)
	}
}

func TestParseFilter_ImplicitAND(t *testing.T) {
	// Space-separated terms without explicit operator are AND
	node := ParseFilter(`service.name:="api-gateway" level:="ERROR"`)
	if node.Op != FilterAnd {
		t.Fatalf("expected FilterAnd for implicit AND, got %v", node.Op)
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
}

func TestParseFilter_BareWord(t *testing.T) {
	// A bare word with no field:value syntax matches _msg
	node := ParseFilter(`timeout`)
	if node.Op != FilterSubstring {
		t.Fatalf("expected FilterSubstring, got %v", node.Op)
	}
	if node.Field != "_msg" {
		t.Errorf("field = %q, want %q", node.Field, "_msg")
	}
	if node.Value != "timeout" {
		t.Errorf("value = %q, want %q", node.Value, "timeout")
	}
}

// --- Evaluate tests ---

func makeColMap(cols map[string][]string) map[string][]string {
	return cols
}

func TestEvaluateFilter_MatchAll(t *testing.T) {
	node := ParseFilter("*")
	colMap := makeColMap(map[string][]string{
		"_msg": {"hello"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("* should match everything")
	}
}

func TestEvaluateFilter_ExactMatch(t *testing.T) {
	node := ParseFilter(`service.name:="api-gateway"`)
	colMap := makeColMap(map[string][]string{
		"service.name": {"api-gateway", "user-service"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match exact api-gateway")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should not match exact api-gateway")
	}
}

func TestEvaluateFilter_Substring(t *testing.T) {
	node := ParseFilter(`_msg:error`)
	colMap := makeColMap(map[string][]string{
		"_msg": {"connection error occurred", "all good", "error at startup"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match substring 'error'")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should not match substring 'error'")
	}
	if !EvaluateFilter(node, colMap, 2) {
		t.Error("row 2 should match substring 'error'")
	}
}

func TestEvaluateFilter_Regex(t *testing.T) {
	node := ParseFilter(`_msg:~"fail.*EOF"`)
	colMap := makeColMap(map[string][]string{
		"_msg": {"failure at EOF", "success", "failed with EOF marker"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match regex")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should not match regex")
	}
	if !EvaluateFilter(node, colMap, 2) {
		t.Error("row 2 should match regex")
	}
}

func TestEvaluateFilter_AND(t *testing.T) {
	node := ParseFilter(`service.name:="api-gateway" AND level:="ERROR"`)
	colMap := makeColMap(map[string][]string{
		"service.name": {"api-gateway", "api-gateway", "user-service"},
		"level":        {"ERROR", "INFO", "ERROR"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match AND")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should not match AND (level != ERROR)")
	}
	if EvaluateFilter(node, colMap, 2) {
		t.Error("row 2 should not match AND (service != api-gateway)")
	}
}

func TestEvaluateFilter_OR(t *testing.T) {
	node := ParseFilter(`service.name:="api-gateway" OR service.name:="user-service"`)
	colMap := makeColMap(map[string][]string{
		"service.name": {"api-gateway", "user-service", "billing-service"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match OR")
	}
	if !EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should match OR")
	}
	if EvaluateFilter(node, colMap, 2) {
		t.Error("row 2 should not match OR")
	}
}

func TestEvaluateFilter_NOT(t *testing.T) {
	node := ParseFilter(`NOT level:="DEBUG"`)
	colMap := makeColMap(map[string][]string{
		"level": {"ERROR", "DEBUG", "INFO"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match NOT DEBUG")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should not match NOT DEBUG")
	}
	if !EvaluateFilter(node, colMap, 2) {
		t.Error("row 2 should match NOT DEBUG")
	}
}

func TestEvaluateFilter_ComplexGrouping(t *testing.T) {
	node := ParseFilter(`(service.name:="api-gateway" OR service.name:="user-service") AND level:="ERROR"`)
	colMap := makeColMap(map[string][]string{
		"service.name": {"api-gateway", "user-service", "billing-service", "api-gateway"},
		"level":        {"ERROR", "INFO", "ERROR", "ERROR"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0: api-gateway + ERROR should match")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1: user-service + INFO should not match")
	}
	if EvaluateFilter(node, colMap, 2) {
		t.Error("row 2: billing-service + ERROR should not match")
	}
	if !EvaluateFilter(node, colMap, 3) {
		t.Error("row 3: api-gateway + ERROR should match")
	}
}

func TestEvaluateFilter_EmptyQuery(t *testing.T) {
	node := ParseFilter("")
	colMap := makeColMap(map[string][]string{
		"_msg": {"anything"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("empty query should match all")
	}
}

func TestEvaluateFilter_MissingField(t *testing.T) {
	node := ParseFilter(`nonexistent:="value"`)
	colMap := makeColMap(map[string][]string{
		"_msg": {"hello"},
	})
	if EvaluateFilter(node, colMap, 0) {
		t.Error("exact match on missing field should not match")
	}
}

func TestEvaluateFilter_NilNode(t *testing.T) {
	if !EvaluateFilter(nil, nil, 0) {
		t.Error("nil node should match (no filter)")
	}
}

func TestParseFilter_NestedNOT(t *testing.T) {
	node := ParseFilter(`NOT NOT level:="ERROR"`)
	colMap := makeColMap(map[string][]string{
		"level": {"ERROR", "DEBUG"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("double NOT ERROR on ERROR row should match")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("double NOT ERROR on DEBUG row should not match")
	}
}

func TestParseFilter_MultipleOR(t *testing.T) {
	node := ParseFilter(`level:="ERROR" OR level:="WARN" OR level:="FATAL"`)
	if node.Op != FilterOr {
		t.Fatalf("expected FilterOr, got %v", node.Op)
	}
	if len(node.Children) != 3 {
		t.Fatalf("expected 3 OR children, got %d", len(node.Children))
	}
	colMap := makeColMap(map[string][]string{
		"level": {"ERROR", "INFO", "WARN", "FATAL", "DEBUG"},
	})
	expected := []bool{true, false, true, true, false}
	for i, want := range expected {
		got := EvaluateFilter(node, colMap, i)
		if got != want {
			t.Errorf("row %d: got %v, want %v", i, got, want)
		}
	}
}

func TestEvaluateFilter_SubstringQuoted(t *testing.T) {
	node := ParseFilter(`_msg:"connection refused"`)
	if node.Op != FilterSubstring {
		t.Fatalf("expected FilterSubstring, got %v", node.Op)
	}
	if node.Value != "connection refused" {
		t.Errorf("value = %q, want %q", node.Value, "connection refused")
	}
	colMap := makeColMap(map[string][]string{
		"_msg": {"error: connection refused by server", "ok"},
	})
	if !EvaluateFilter(node, colMap, 0) {
		t.Error("row 0 should match quoted substring")
	}
	if EvaluateFilter(node, colMap, 1) {
		t.Error("row 1 should not match quoted substring")
	}
}
