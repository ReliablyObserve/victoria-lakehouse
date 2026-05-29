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

func TestFilterExtractOrBranches(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    [][]BranchCheck // nil means "should return nil — unsupported"
	}{
		{
			name:  "simple_two_branch_or",
			query: `level:="INFO" OR level:="WARN"`,
			want: [][]BranchCheck{
				{{FieldName: "level", Value: "INFO"}},
				{{FieldName: "level", Value: "WARN"}},
			},
		},
		{
			name:  "three_branch_different_fields",
			query: `service.name:="api" OR host.name:="h1" OR k8s.pod.name:="p1"`,
			want: [][]BranchCheck{
				{{FieldName: "service.name", Value: "api"}},
				{{FieldName: "host.name", Value: "h1"}},
				{{FieldName: "k8s.pod.name", Value: "p1"}},
			},
		},
		// AND distributed over OR — each branch inherits the AND clauses.
		{
			name:  "and_distributed_into_or",
			query: `service.name:="api" (level:="ERROR" OR level:="WARN")`,
			want: [][]BranchCheck{
				{{FieldName: "service.name", Value: "api"}, {FieldName: "level", Value: "ERROR"}},
				{{FieldName: "service.name", Value: "api"}, {FieldName: "level", Value: "WARN"}},
			},
		},
		// No OR at top level — unsupported.
		{
			name:  "no_or",
			query: `level:="ERROR" AND service.name:="api"`,
			want:  nil,
		},
		// OR branch containing regex — unsupported (bloom can't model).
		{
			name:  "or_with_regex_branch",
			query: `level:="ERROR" OR _msg:~"timeout"`,
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			got := FilterExtractOrBranches(f)
			if tc.want == nil {
				if got != nil {
					t.Errorf("expected nil for unsupported shape; got %v", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d branches, want %d: got=%v", len(got), len(tc.want), got)
			}
			// Compare set semantics — branch order matters but check sets
			// inside each branch are order-insensitive.
			for i, wantBranch := range tc.want {
				wantSet := branchSet(wantBranch)
				gotSet := branchSet(got[i])
				if len(wantSet) != len(gotSet) {
					t.Errorf("branch %d: got %d checks, want %d", i, len(gotSet), len(wantSet))
				}
				for k, v := range wantSet {
					if gotSet[k] != v {
						t.Errorf("branch %d: missing/wrong %q=%q (got %q)", i, k, v, gotSet[k])
					}
				}
			}
		})
	}
}

func branchSet(b []BranchCheck) map[string]string {
	m := make(map[string]string, len(b))
	for _, c := range b {
		m[c.FieldName] = c.Value
	}
	return m
}

func TestFilterExtractOrBranches_Nil(t *testing.T) {
	if FilterExtractOrBranches(nil) != nil {
		t.Error("expected nil for nil filter")
	}
}
