package parquets3

import (
	"reflect"
	"testing"
)

// branchSets normalizes [][]BranchCheck into a comparable shape.
func branchSets(branches [][]BranchCheck) [][]BranchCheck {
	return branches
}

// TestFilterExtractOrBranches_PureOr covers the core Grafana-drilldown
// shape: a top-level OR of simple field=value exact matches must yield
// one branch per OR arm.
func TestFilterExtractOrBranches_PureOr(t *testing.T) {
	f := parseFilter(t, `service.name:="svc-a" OR service.name:="svc-b" OR service.name:="svc-c"`)
	branches := FilterExtractOrBranches(f)
	if len(branches) != 3 {
		t.Fatalf("expected 3 branches, got %d: %v", len(branches), branches)
	}
	want := [][]BranchCheck{
		{{FieldName: "service.name", Value: "svc-a"}},
		{{FieldName: "service.name", Value: "svc-b"}},
		{{FieldName: "service.name", Value: "svc-c"}},
	}
	if !reflect.DeepEqual(branchSets(branches), want) {
		t.Errorf("branches = %v, want %v", branches, want)
	}
}

// TestFilterExtractOrBranches_AndDistribution: an AND clause wrapping
// the OR must be distributed into every branch (each branch inherits
// the surrounding constraint).
func TestFilterExtractOrBranches_AndDistribution(t *testing.T) {
	f := parseFilter(t, `env:="prod" AND (service.name:="svc-a" OR service.name:="svc-b")`)
	branches := FilterExtractOrBranches(f)
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got %d: %v", len(branches), branches)
	}
	for i, b := range branches {
		if len(b) != 2 {
			t.Fatalf("branch %d: expected 2 checks (AND clause distributed), got %v", i, b)
		}
		if b[0].FieldName != "env" || b[0].Value != "prod" {
			t.Errorf("branch %d missing distributed AND clause: %v", i, b)
		}
	}
	if branches[0][1].Value != "svc-a" || branches[1][1].Value != "svc-b" {
		t.Errorf("branch-specific checks wrong: %v", branches)
	}
}

// TestFilterExtractOrBranches_AndInsideBranch: a branch that is itself
// an AND of simple predicates contributes all its checks.
func TestFilterExtractOrBranches_AndInsideBranch(t *testing.T) {
	f := parseFilter(t, `(a:="1" AND b:="2") OR c:="3"`)
	branches := FilterExtractOrBranches(f)
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got %d: %v", len(branches), branches)
	}
	if len(branches[0]) != 2 {
		t.Errorf("AND branch should carry both checks: %v", branches[0])
	}
	if len(branches[1]) != 1 || branches[1][0].FieldName != "c" || branches[1][0].Value != "3" {
		t.Errorf("simple branch wrong: %v", branches[1])
	}
}

// TestFilterExtractOrBranches_UnsupportedShapes pins every bail-out:
// returning nil makes the caller fall back to non-OR bloom logic,
// which is the safe (never over-filter) answer.
func TestFilterExtractOrBranches_UnsupportedShapes(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		// No OR at all — splitTopLevelAndOr returns no OR node.
		{"no OR", `a:="1" AND b:="2"`},
		// Two OR children under one AND — too complex to distribute.
		{"two ORs under AND", `(a:="1" OR a:="2") AND (b:="3" OR b:="4")`},
		// Branch contains a prefix predicate bloom can't model.
		{"prefix predicate branch", `a:="1" OR b:abc*`},
		// Branch contains an in() predicate (not a single exact value).
		{"in() branch", `a:="1" OR b:in(x,y)`},
		// OR nested under NOT inverts semantics — must bail.
		{"negated OR", `!(a:="1" OR a:="2")`},
		// Range predicate in a branch.
		{"range branch", `a:="1" OR b:>5`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			if got := FilterExtractOrBranches(f); got != nil {
				t.Errorf("FilterExtractOrBranches(%q) = %v, want nil (unsupported shape must fall back)", tc.query, got)
			}
		})
	}
}

func TestFilterExtractOrBranches_NilFilter(t *testing.T) {
	if got := FilterExtractOrBranches(nil); got != nil {
		t.Errorf("nil filter must yield nil branches, got %v", got)
	}
}

func TestFilterContainsOr(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`a:="1" OR b:="2"`, true},
		{`a:="1" AND b:="2"`, false},
		{`a:="1"`, false},
		// OR nested under AND still counts — push-down is unsafe.
		{`x:="q" AND (a:="1" OR a:="2")`, true},
		// Quoted literal containing " OR " must NOT count as an OR
		// operator — this is exactly the dumb-substring bug the AST
		// walk exists to fix.
		{`body:="alpha OR beta"`, false},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			f := parseFilter(t, tc.query)
			if got := FilterContainsOr(f); got != tc.want {
				t.Errorf("FilterContainsOr(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
	if FilterContainsOr(nil) {
		t.Error("FilterContainsOr(nil) must be false")
	}
}

func TestAstTypeName_NonStructValues(t *testing.T) {
	// Invalid value.
	if got := astTypeName(reflect.Value{}); got != "" {
		t.Errorf("astTypeName(invalid) = %q, want \"\"", got)
	}
	// Nil pointer.
	var p *struct{ X int }
	if got := astTypeName(reflect.ValueOf(p)); got != "" {
		t.Errorf("astTypeName(nil ptr) = %q, want \"\"", got)
	}
	// Non-struct kind.
	if got := astTypeName(reflect.ValueOf(42)); got != "" {
		t.Errorf("astTypeName(int) = %q, want \"\"", got)
	}
	// Nil interface.
	var iface interface{ M() }
	v := reflect.ValueOf(&iface).Elem()
	if got := astTypeName(v); got != "" {
		t.Errorf("astTypeName(nil interface) = %q, want \"\"", got)
	}
}

func TestFilterInner_Nil(t *testing.T) {
	v := filterInner(nil)
	if v.IsValid() {
		t.Error("filterInner(nil) must return the zero Value")
	}
}

// TestCountPushdownFilterFields pins the SOUNDNESS gate semantics:
// ok=true only when every node type is known; pseudo-fields reported
// for time/stream nodes so callers can reject them.
func TestCountPushdownFilterFields(t *testing.T) {
	t.Run("nil filter is trivially complete", func(t *testing.T) {
		fields, ok := countPushdownFilterFields(nil)
		if !ok || len(fields) != 0 {
			t.Errorf("got (%v, %v), want (empty, true)", fields, ok)
		}
	})

	t.Run("single field across AND/OR/NOT", func(t *testing.T) {
		f := parseFilter(t, `level:="ERROR" OR level:="WARN"`)
		fields, ok := countPushdownFilterFields(f)
		if !ok {
			t.Fatal("expected ok=true for exact-match OR")
		}
		if len(fields) != 1 || !fields["level"] {
			t.Errorf("fields = %v, want {level}", fields)
		}
	})

	t.Run("multiple fields all reported", func(t *testing.T) {
		f := parseFilter(t, `a:="1" AND !(b:="2")`)
		fields, ok := countPushdownFilterFields(f)
		if !ok {
			t.Fatal("expected ok=true")
		}
		if !fields["a"] || !fields["b"] {
			t.Errorf("fields = %v, want a and b", fields)
		}
	})

	t.Run("time filter reports pseudo-field", func(t *testing.T) {
		f := parseFilter(t, `_time:5m a:="1"`)
		fields, ok := countPushdownFilterFields(f)
		if !ok {
			t.Fatal("expected ok=true for known time node")
		}
		if !fields["_time"] {
			t.Errorf("fields = %v, want _time pseudo-field reported", fields)
		}
	})

	t.Run("regexp leaf reports its field", func(t *testing.T) {
		// filterRegexp is in the known-type allowlist — the gate stays
		// ok=true and the field is reported, so the single-field check
		// upstream still sees every column the filter touches.
		f := parseFilter(t, `a:~"err.*"`)
		fields, ok := countPushdownFilterFields(f)
		if !ok {
			t.Fatal("expected ok=true for known regexp node")
		}
		if !fields["a"] {
			t.Errorf("fields = %v, want a", fields)
		}
	})

	t.Run("eq_field reports BOTH fields", func(t *testing.T) {
		// filterEqField references two columns; both must be reported
		// (naturally disqualifying the single-field count gate — a
		// synthetic row stream reproduces only ONE field's values).
		f := parseFilter(t, `a:eq_field(b)`)
		fields, ok := countPushdownFilterFields(f)
		if !ok {
			t.Fatal("expected ok=true for known eq_field node")
		}
		if !fields["a"] || !fields["b"] {
			t.Errorf("fields = %v, want both a and b", fields)
		}
	})
}
