package parquets3

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// --- buildPushDownFilter tests ---

func TestBuildPushDownFilter_ExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter(`service.name:="my-service"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil filter")
	}
	if len(pdf.Checks) != 1 {
		t.Fatalf("expected 1 check, got %d", len(pdf.Checks))
	}
	c := pdf.Checks[0]
	if c.Column != "service.name" {
		t.Errorf("expected column service.name, got %s", c.Column)
	}
	if c.Op != PushDownExact {
		t.Errorf("expected PushDownExact, got %d", c.Op)
	}
	if c.Value != "my-service" {
		t.Errorf("expected value my-service, got %s", c.Value)
	}
}

func TestBuildPushDownFilter_Prefix(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter(`service.name:="prod-*"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil filter")
	}
	if len(pdf.Checks) != 1 {
		t.Fatalf("expected 1 check, got %d", len(pdf.Checks))
	}
	c := pdf.Checks[0]
	if c.Op != PushDownPrefix {
		t.Errorf("expected PushDownPrefix, got %d", c.Op)
	}
	if c.Value != "prod-" {
		t.Errorf("expected prefix prod-, got %s", c.Value)
	}
}

func TestBuildPushDownFilter_GreaterThan(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter(`severity_text:>"error"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil filter")
	}
	if len(pdf.Checks) != 1 {
		t.Fatalf("expected 1 check, got %d", len(pdf.Checks))
	}
	c := pdf.Checks[0]
	if c.Op != PushDownGreaterThan {
		t.Errorf("expected PushDownGreaterThan, got %d", c.Op)
	}
	if c.Value != "error" {
		t.Errorf("expected value error, got %s", c.Value)
	}
}

func TestBuildPushDownFilter_LessThan(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter(`severity_text:<"warn"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil filter")
	}
	if len(pdf.Checks) != 1 {
		t.Fatalf("expected 1 check, got %d", len(pdf.Checks))
	}
	c := pdf.Checks[0]
	if c.Op != PushDownLessThan {
		t.Errorf("expected PushDownLessThan, got %d", c.Op)
	}
	if c.Value != "warn" {
		t.Errorf("expected value warn, got %s", c.Value)
	}
}

func TestBuildPushDownFilter_EmptyQuery(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter("", reg)
	if pdf != nil {
		t.Errorf("expected nil for empty query, got %+v", pdf)
	}
}

func TestBuildPushDownFilter_NilRegistry(t *testing.T) {
	pdf := buildPushDownFilter(`service.name:="foo"`, nil)
	if pdf != nil {
		t.Errorf("expected nil for nil registry, got %+v", pdf)
	}
}

func TestBuildPushDownFilter_NoMatchablePredicates(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter(`* | stats count()`, reg)
	if pdf != nil {
		t.Errorf("expected nil for non-matchable query, got %+v", pdf)
	}
}

func TestBuildPushDownFilter_MultipleChecks(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	pdf := buildPushDownFilter(`service.name:="api" severity_text:="error"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil filter")
	}
	if len(pdf.Checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(pdf.Checks))
	}
}

// --- checkMatchesStats tests ---

func TestCheckMatchesStats_ExactWithinRange(t *testing.T) {
	// "dog" is within ["ant", "fox"]
	if !checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "dog"}, "ant", "fox") {
		t.Error("expected match: value within range")
	}
}

func TestCheckMatchesStats_ExactBelowRange(t *testing.T) {
	// "aaa" is below ["bbb", "ddd"]
	if checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "aaa"}, "bbb", "ddd") {
		t.Error("expected skip: value below range")
	}
}

func TestCheckMatchesStats_ExactAboveRange(t *testing.T) {
	// "zzz" is above ["bbb", "ddd"]
	if checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "zzz"}, "bbb", "ddd") {
		t.Error("expected skip: value above range")
	}
}

func TestCheckMatchesStats_ExactAtBoundary(t *testing.T) {
	// value equals min
	if !checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "bbb"}, "bbb", "ddd") {
		t.Error("expected match: value equals min")
	}
	// value equals max
	if !checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "ddd"}, "bbb", "ddd") {
		t.Error("expected match: value equals max")
	}
}

func TestCheckMatchesStats_GreaterThan(t *testing.T) {
	// max="fox" > "dog" → might have values > "dog"
	if !checkMatchesStats(PushDownCheck{Op: PushDownGreaterThan, Value: "dog"}, "ant", "fox") {
		t.Error("expected match: max > threshold")
	}
	// max="bbb" <= "zzz" → no values > "zzz"
	if checkMatchesStats(PushDownCheck{Op: PushDownGreaterThan, Value: "zzz"}, "aaa", "bbb") {
		t.Error("expected skip: max <= threshold")
	}
}

func TestCheckMatchesStats_LessThan(t *testing.T) {
	// min="ant" < "dog" → might have values < "dog"
	if !checkMatchesStats(PushDownCheck{Op: PushDownLessThan, Value: "dog"}, "ant", "fox") {
		t.Error("expected match: min < threshold")
	}
	// min="mmm" >= "aaa" → no values < "aaa"
	if checkMatchesStats(PushDownCheck{Op: PushDownLessThan, Value: "aaa"}, "mmm", "zzz") {
		t.Error("expected skip: min >= threshold")
	}
}

func TestCheckMatchesStats_Prefix(t *testing.T) {
	// prefix "do" overlaps ["ant", "fox"] since "do" < "fox" and "dp" > "ant"
	if !checkMatchesStats(PushDownCheck{Op: PushDownPrefix, Value: "do"}, "ant", "fox") {
		t.Error("expected match: prefix overlaps range")
	}
	// prefix "zz" > max "fox" → no overlap
	if checkMatchesStats(PushDownCheck{Op: PushDownPrefix, Value: "zz"}, "ant", "fox") {
		t.Error("expected skip: prefix above range")
	}
	// prefix "aa" with successor "ab" <= min "bbb" → no overlap
	if checkMatchesStats(PushDownCheck{Op: PushDownPrefix, Value: "aa"}, "bbb", "ddd") {
		t.Error("expected skip: prefix successor <= min")
	}
}

func TestCheckMatchesStats_PrefixAtBoundary(t *testing.T) {
	// prefix "bb" should match range ["bbb", "ddd"] since "bbb" starts with "bb"
	if !checkMatchesStats(PushDownCheck{Op: PushDownPrefix, Value: "bb"}, "bbb", "ddd") {
		t.Error("expected match: prefix matches min")
	}
}

// --- prefixSuccessor tests ---

func TestPrefixSuccessor(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"abc", "abd"},
		{"a", "b"},
		{"az", "a{"},
		{string([]byte{0xFF, 0xFF}), ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := prefixSuccessor(tt.input)
		if got != tt.want {
			t.Errorf("prefixSuccessor(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- rowGroupMatchesFilter integration tests with real parquet files ---

type pushdownTestRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
}

func writePushdownTestParquet(t *testing.T, dir string, rows []pushdownTestRow) string {
	t.Helper()
	path := filepath.Join(dir, "pushdown_test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[pushdownTestRow](f,
		parquet.Compression(&parquet.Zstd),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func openTestParquet(t *testing.T, path string) *parquet.File {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRowGroupMatchesFilter_NilFilter(t *testing.T) {
	// nil filter should always return true (can't skip)
	if !rowGroupMatchesFilter(nil, nil, nil) {
		t.Error("nil filter should return true")
	}
}

func TestRowGroupMatchesFilter_ExactMatchSkip(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "alpha"},
		{TimestampUnixNano: 2000, Body: "world", SeverityText: "warn", ServiceName: "beta"},
		{TimestampUnixNano: 3000, Body: "test", SeverityText: "error", ServiceName: "gamma"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// "zzz" is above all service names (alpha, beta, gamma) → should skip
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "zzz", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: value 'zzz' is above all service names")
	}
}

func TestRowGroupMatchesFilter_ExactMatchNoSkip(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "alpha"},
		{TimestampUnixNano: 2000, Body: "world", SeverityText: "warn", ServiceName: "beta"},
		{TimestampUnixNano: 3000, Body: "test", SeverityText: "error", ServiceName: "gamma"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// "beta" is within [alpha, gamma] → should NOT skip
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "beta", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: value 'beta' is within service name range")
	}
}

func TestRowGroupMatchesFilter_UnknownColumn(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "alpha"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// Unknown column → should return true (can't skip)
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "nonexistent.column", Op: PushDownExact, Value: "anything", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: unknown column should not cause skip")
	}
}

func TestRowGroupMatchesFilter_MultipleChecksAllMustPass(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "alpha"},
		{TimestampUnixNano: 2000, Body: "world", SeverityText: "warn", ServiceName: "beta"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// First check passes (beta in [alpha, beta]), second fails (zzz not in [info, warn])
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "beta", ColIdx: -1},
			{Column: "severity_text", Op: PushDownExact, Value: "zzz", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: second check should fail")
	}
}

func TestRowGroupMatchesFilter_GreaterThan(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "alpha"},
		{TimestampUnixNano: 2000, Body: "world", SeverityText: "warn", ServiceName: "beta"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// GT "aaa" → max is "beta" > "aaa" → should NOT skip
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownGreaterThan, Value: "aaa", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: max > threshold")
	}

	// GT "zzz" → max is "beta" <= "zzz" → should skip
	pdf = &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownGreaterThan, Value: "zzz", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: max <= threshold")
	}
}

func TestRowGroupMatchesFilter_LessThan(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "mmm"},
		{TimestampUnixNano: 2000, Body: "world", SeverityText: "warn", ServiceName: "zzz"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// LT "zzz" → min is "mmm" < "zzz" → should NOT skip
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownLessThan, Value: "zzz", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: min < threshold")
	}

	// LT "aaa" → min is "mmm" >= "aaa" → should skip
	pdf = &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownLessThan, Value: "aaa", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: min >= threshold")
	}
}

func TestRowGroupMatchesFilter_Prefix(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "prod-api"},
		{TimestampUnixNano: 2000, Body: "world", SeverityText: "warn", ServiceName: "prod-web"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// prefix "prod-" overlaps [prod-api, prod-web] → should NOT skip
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownPrefix, Value: "prod-", ColIdx: -1},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: prefix overlaps range")
	}

	// prefix "zzz" > max "prod-web" → should skip
	pdf = &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownPrefix, Value: "zzz", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: prefix above range")
	}

	// prefix "aaa" with successor "aab" <= min "prod-api" → should skip
	pdf = &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownPrefix, Value: "aaa", ColIdx: -1},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: prefix successor <= min")
	}
}
