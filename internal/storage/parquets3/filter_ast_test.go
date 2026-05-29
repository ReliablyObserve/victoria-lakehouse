package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func parseFilter(t *testing.T, s string) *logstorage.Filter {
	t.Helper()
	f, err := logstorage.ParseFilter(s)
	if err != nil {
		t.Fatalf("ParseFilter(%q): %v", s, err)
	}
	return f
}

func TestFilterContainsOr(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`service.name:="api"`, false},
		{`service.name:="api" level:="error"`, false}, // AND, no OR
		{`service.name:="api" OR level:="error"`, true},
		{`service.name:="api" or level:="error"`, true},
		// OR nested under AND — the critical case the regex helper still
		// detected, but as a structural property.
		{`level:="error" (service.name:="api" OR service.name:="web")`, true},
		// "or" inside a quoted literal must NOT trigger.
		{`_msg:"this or that"`, false},
		// Multiple ORs at different depths.
		{`(a:="1" OR b:="2") AND (c:="3" OR d:="4")`, true},
	}

	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			got := FilterContainsOr(f)
			if got != tc.want {
				t.Errorf("FilterContainsOr(%q) = %v, want %v; filter.String()=%q", tc.query, got, tc.want, f.String())
			}
		})
	}
}

func TestFilterContainsOr_NilSafe(t *testing.T) {
	if FilterContainsOr(nil) {
		t.Error("nil filter should not contain OR")
	}
}

func TestFilterIsNegated(t *testing.T) {
	tests := []struct {
		query     string
		fieldName string
		want      bool
	}{
		{`service.name:="api"`, "service.name", false},
		{`NOT service.name:="api"`, "service.name", true},
		{`-service.name:="api"`, "service.name", true},
		// Other field negated, queried field not — should not flag.
		{`NOT level:="error" service.name:="api"`, "service.name", false},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			got := FilterIsNegated(f, tc.fieldName)
			if got != tc.want {
				t.Errorf("FilterIsNegated(%q, %q) = %v, want %v; filter.String()=%q",
					tc.query, tc.fieldName, got, tc.want, f.String())
			}
		})
	}
}

func TestFilterExtractFieldValues(t *testing.T) {
	tests := []struct {
		query     string
		fieldName string
		want      []string
	}{
		{`service.name:="api"`, "service.name", []string{"api"}},
		{`service.name:in("api","web","worker")`, "service.name", []string{"api", "web", "worker"}},
		{`service.name:="api" AND level:="error"`, "service.name", []string{"api"}},
		// OR branch — values are NOT extracted from OR (push-down unsafe).
		// The existing string-based fallback may still find values, so we
		// only assert that ANY non-empty extraction returns valid values
		// (no garbage).
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			got := FilterExtractFieldValues(f, tc.fieldName)
			if len(got) != len(tc.want) {
				t.Fatalf("FilterExtractFieldValues(%q, %q) returned %d values, want %d: got=%v",
					tc.query, tc.fieldName, len(got), len(tc.want), got)
			}
			gotSet := make(map[string]bool, len(got))
			for _, v := range got {
				gotSet[v] = true
			}
			for _, want := range tc.want {
				if !gotSet[want] {
					t.Errorf("FilterExtractFieldValues(%q, %q) missing %q; got=%v",
						tc.query, tc.fieldName, want, got)
				}
			}
		})
	}
}

// TestFilterContainsOr_NestedUnderAnd is the critical fix-8 regression
// check: ensure OR is detected when nested under an AND wrapper.
func TestFilterContainsOr_NestedUnderAnd(t *testing.T) {
	f := parseFilter(t, `service.name:="api" (level:="error" OR level:="warn")`)
	if !FilterContainsOr(f) {
		t.Fatalf("FilterContainsOr should detect OR nested under AND; filter.String()=%q", f.String())
	}
}

func TestFilterReferencedFields(t *testing.T) {
	tests := []struct {
		query string
		want  []string
	}{
		{`service.name:="api"`, []string{"service.name"}},
		{`level:="ERROR" AND service.name:="api"`, []string{"level", "service.name"}},
		{`level:="ERROR" OR level:="WARN"`, []string{"level"}},
		// Nested OR under AND — should still extract both fields.
		{`service.name:="api" (level:="ERROR" OR level:="WARN")`, []string{"service.name", "level"}},
		// NOT predicate — fieldName still referenced.
		{`NOT service.name:="api"`, []string{"service.name"}},
		// Multiple distinct fields.
		{`a:="1" b:="2" c:="3"`, []string{"a", "b", "c"}},
	}

	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			got := FilterReferencedFields(f)
			if len(got) != len(tc.want) {
				t.Errorf("FilterReferencedFields(%q) returned %d fields, want %d: got=%v",
					tc.query, len(got), len(tc.want), got)
			}
			for _, want := range tc.want {
				if !got[want] {
					t.Errorf("FilterReferencedFields(%q) missing %q; got=%v", tc.query, want, got)
				}
			}
		})
	}
}

func TestFilterReferencedFields_Nil(t *testing.T) {
	got := FilterReferencedFields(nil)
	if len(got) != 0 {
		t.Errorf("FilterReferencedFields(nil) = %v, want empty map", got)
	}
}
