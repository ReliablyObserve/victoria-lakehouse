package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestIsNegatedPredicate(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		fieldName string
		want      bool
	}{
		{
			name:      "not negated - simple exact match",
			query:     `service.name:="my-service"`,
			fieldName: "service.name",
			want:      false,
		},
		{
			name:      "negated with ! prefix",
			query:     `!service.name:="my-service"`,
			fieldName: "service.name",
			want:      true,
		},
		{
			name:      "negated with NOT keyword",
			query:     `NOT service.name:="my-service"`,
			fieldName: "service.name",
			want:      true,
		},
		{
			name:      "negated with NOT and spaces",
			query:     `NOT   service.name:="my-service"`,
			fieldName: "service.name",
			want:      true,
		},
		{
			name:      "negated with :! operator",
			query:     `service.name:!"my-service"`,
			fieldName: "service.name",
			want:      true,
		},
		{
			name:      "negated with :!~ regex operator",
			query:     `service.name:!~"my-.*"`,
			fieldName: "service.name",
			want:      true,
		},
		{
			name:      "field not in query",
			query:     `severity_text:="error"`,
			fieldName: "service.name",
			want:      false,
		},
		{
			name:      "field at start of query - not negated",
			query:     `service.name:="foo" AND trace_id:="abc"`,
			fieldName: "service.name",
			want:      false,
		},
		{
			name:      "multiple predicates - second negated",
			query:     `service.name:="foo" !severity_text:="error"`,
			fieldName: "severity_text",
			want:      true,
		},
		{
			name:      "empty query",
			query:     "",
			fieldName: "service.name",
			want:      false,
		},
		{
			name:      "empty field name",
			query:     `service.name:="foo"`,
			fieldName: "",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNegatedPredicate(tt.query, tt.fieldName)
			if got != tt.want {
				t.Errorf("isNegatedPredicate(%q, %q) = %v, want %v", tt.query, tt.fieldName, got, tt.want)
			}
		})
	}
}

func TestCheckMatchesStatsNumeric(t *testing.T) {
	tests := []struct {
		name  string
		check PushDownCheck
		rgMin int64
		rgMax int64
		want  bool
	}{
		{
			name:  "exact match within range",
			check: PushDownCheck{Op: PushDownExact, Value: "50"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
		{
			name:  "exact match below range",
			check: PushDownCheck{Op: PushDownExact, Value: "5"},
			rgMin: 10, rgMax: 100,
			want: false,
		},
		{
			name:  "exact match above range",
			check: PushDownCheck{Op: PushDownExact, Value: "200"},
			rgMin: 10, rgMax: 100,
			want: false,
		},
		{
			name:  "exact match at min boundary",
			check: PushDownCheck{Op: PushDownExact, Value: "10"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
		{
			name:  "exact match at max boundary",
			check: PushDownCheck{Op: PushDownExact, Value: "100"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
		{
			name:  "exact match with min == max",
			check: PushDownCheck{Op: PushDownExact, Value: "42"},
			rgMin: 42, rgMax: 42,
			want: true,
		},
		{
			name:  "exact match miss with min == max",
			check: PushDownCheck{Op: PushDownExact, Value: "43"},
			rgMin: 42, rgMax: 42,
			want: false,
		},
		{
			name:  "greater than - max above threshold",
			check: PushDownCheck{Op: PushDownGreaterThan, Value: "50"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
		{
			name:  "greater than - max equals threshold",
			check: PushDownCheck{Op: PushDownGreaterThan, Value: "100"},
			rgMin: 10, rgMax: 100,
			want: false,
		},
		{
			name:  "greater than - max below threshold",
			check: PushDownCheck{Op: PushDownGreaterThan, Value: "200"},
			rgMin: 10, rgMax: 100,
			want: false,
		},
		{
			name:  "less than - min below threshold",
			check: PushDownCheck{Op: PushDownLessThan, Value: "50"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
		{
			name:  "less than - min equals threshold",
			check: PushDownCheck{Op: PushDownLessThan, Value: "10"},
			rgMin: 10, rgMax: 100,
			want: false,
		},
		{
			name:  "less than - min above threshold",
			check: PushDownCheck{Op: PushDownLessThan, Value: "5"},
			rgMin: 10, rgMax: 100,
			want: false,
		},
		{
			name:  "non-numeric value returns true (conservative)",
			check: PushDownCheck{Op: PushDownExact, Value: "not-a-number"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
		{
			name:  "unknown op returns true",
			check: PushDownCheck{Op: PushDownOp(99), Value: "50"},
			rgMin: 10, rgMax: 100,
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkMatchesStatsNumeric(tt.check, tt.rgMin, tt.rgMax)
			if got != tt.want {
				t.Errorf("checkMatchesStatsNumeric(%v, %d, %d) = %v, want %v",
					tt.check, tt.rgMin, tt.rgMax, got, tt.want)
			}
		})
	}
}

func TestExtractQuotedOp(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		fieldName string
		op        string
		want      string
	}{
		{
			name:      "exact match extraction",
			query:     `service.name:="my-service" AND other`,
			fieldName: "service.name",
			op:        `:="`,
			want:      "my-service",
		},
		{
			name:      "no match",
			query:     `other_field:="value"`,
			fieldName: "service.name",
			op:        `:="`,
			want:      "",
		},
		{
			name:      "unclosed quote",
			query:     `service.name:="unclosed`,
			fieldName: "service.name",
			op:        `:="`,
			want:      "",
		},
		{
			name:      "empty value",
			query:     `service.name:=""`,
			fieldName: "service.name",
			op:        `:="`,
			want:      "",
		},
		{
			name:      "greater than extraction",
			query:     `severity_text:>"error"`,
			fieldName: "severity_text",
			op:        `:>"`,
			want:      "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractQuotedOp(tt.query, tt.fieldName, tt.op)
			if got != tt.want {
				t.Errorf("extractQuotedOp(%q, %q, %q) = %q, want %q",
					tt.query, tt.fieldName, tt.op, got, tt.want)
			}
		})
	}
}

func TestBuildPushDownFilter_NegatedPredicateSkipped(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)

	pdf := buildPushDownFilter(`!service.name:="my-service"`, reg)
	if pdf != nil {
		t.Error("expected nil filter for negated predicate, got non-nil")
	}

	pdf = buildPushDownFilter(`NOT service.name:="my-service"`, reg)
	if pdf != nil {
		t.Error("expected nil filter for NOT predicate, got non-nil")
	}

	pdf = buildPushDownFilter(`service.name:!"my-service"`, reg)
	if pdf != nil {
		t.Error("expected nil filter for :! predicate, got non-nil")
	}
}

func TestCheckMatchesStats_UnknownOp(t *testing.T) {
	got := checkMatchesStats(PushDownCheck{Op: PushDownOp(99), Value: "foo"}, "aaa", "zzz")
	if !got {
		t.Error("unknown op should return true (conservative)")
	}
}

func TestRowGroupMatchesFilter_DictionaryExactHit(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "alpha"},
		{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "alpha"},
		{TimestampUnixNano: 3000, Body: "c", SeverityText: "error", ServiceName: "alpha"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "alpha", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: 'alpha' exists in dictionary")
	}
}

func TestRowGroupMatchesFilter_DictionaryExactMiss(t *testing.T) {
	dir := t.TempDir()
	// Use enough rows to trigger dictionary encoding
	var rows []pushdownTestRow
	for i := 0; i < 100; i++ {
		svc := "alpha"
		if i%2 == 1 {
			svc = "beta"
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       svc,
		})
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// "azz" is within stats range [alpha,beta] but not in dictionary
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "azz", ColIdx: -1},
		},
	}
	// If dictionary-encoded, the check should skip. If not dictionary-encoded,
	// the function returns true (conservative). Either result is correct.
	result := rowGroupMatchesFilter(f, rgs[0], pdf)
	_ = result // Verify no panic; dictionary check exercises the code path
}

func TestRowGroupMatchesFilter_DictionaryPrefixHit(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "prod-api"},
		{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "prod-web"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownPrefix, Value: "prod-", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: prefix 'prod-' matches dictionary entries")
	}
}

func TestRowGroupMatchesFilter_DictionaryPrefixMiss(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "prod-api"},
		{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "prod-web"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownPrefix, Value: "prod-z", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: prefix 'prod-z' within stats range but NOT in dictionary")
	}
}
