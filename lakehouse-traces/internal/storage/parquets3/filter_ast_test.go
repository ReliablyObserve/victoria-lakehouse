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

// NOTE: This module pins an older VL commit than the logs module, and
// VL's filter AST shape differs between versions (simple predicates
// like `field:=value` are wrapped in filterGeneric{f:filterExact} in
// newer VL but live directly as filterExact in the older commit).
// The shared filter_ast.go helpers handle both shapes via permissive
// field-name detection. Tests below cover only the surface actually
// used by lakehouse-traces query paths: FilterReferencedFields, which
// powers the column-projected reads in GetFieldValues.

func TestFilterReferencedFields(t *testing.T) {
	tests := []struct {
		query string
		want  []string
	}{
		{`service.name:="api"`, []string{"service.name"}},
		{`level:="ERROR" AND service.name:="api"`, []string{"level", "service.name"}},
		{`level:="ERROR" OR level:="WARN"`, []string{"level"}},
		// Multiple distinct fields under implicit AND.
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
