package parquets3

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// ---------------------------------------------------------------------------
// 1. rowGroupFullyInRange (storage_query.go ~1522) — 0% → covered
// ---------------------------------------------------------------------------

func TestMedium_rowGroupFullyInRange(t *testing.T) {
	dir := t.TempDir()

	// Write a parquet with known timestamp range [1000..3000].
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "svc"},
		{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "svc"},
		{TimestampUnixNano: 3000, Body: "c", SeverityText: "error", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	rg := rgs[0]
	tsIdx := findColumnIndex(f.Root(), "timestamp_unix_nano")
	if tsIdx < 0 {
		t.Fatal("timestamp_unix_nano column not found")
	}

	tests := []struct {
		name    string
		startNs int64
		endNs   int64
		want    bool
	}{
		{"fully within range", 0, 5000, true},
		{"exact boundary match", 1000, 3000, true},
		{"extends before start", 1500, 5000, false},
		{"extends past end", 0, 2500, false},
		{"completely outside before", 5000, 6000, false},
		{"completely outside after", 0, 500, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rowGroupFullyInRange(rg, tsIdx, tt.startNs, tt.endNs)
			if got != tt.want {
				t.Errorf("rowGroupFullyInRange(rg, %d, %d, %d) = %v, want %v",
					tsIdx, tt.startNs, tt.endNs, got, tt.want)
			}
		})
	}

	t.Run("tsColIdx out of bounds", func(t *testing.T) {
		got := rowGroupFullyInRange(rg, 999, 0, 5000)
		if got {
			t.Error("expected false for out-of-bounds tsColIdx")
		}
	})
}

// ---------------------------------------------------------------------------
// 2. prewhereFilter (prewhere.go ~12) — 7.3% → covered
// ---------------------------------------------------------------------------

func TestMedium_prewhereFilter(t *testing.T) {
	dir := t.TempDir()

	// Write a parquet with mixed service names for filtering.
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "alpha"},
		{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "beta"},
		{TimestampUnixNano: 3000, Body: "c", SeverityText: "error", ServiceName: "alpha"},
		{TimestampUnixNano: 4000, Body: "d", SeverityText: "info", ServiceName: "gamma"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	rg := rgs[0]
	svcIdx := findColumnIndex(f.Root(), "service.name")

	t.Run("exact match filters correctly", func(t *testing.T) {
		pdf := &PushDownFilter{
			Checks: []PushDownCheck{
				{Column: "service.name", Op: PushDownExact, Value: "alpha", ColIdx: svcIdx},
			},
		}
		bitmap := prewhereFilter(f, rg, pdf)
		// Only rows 0 and 2 should match "alpha"
		if bitmap == nil {
			t.Fatal("expected non-nil bitmap")
		}
		if len(bitmap) != 4 {
			t.Fatalf("expected bitmap length 4, got %d", len(bitmap))
		}
		expected := []bool{true, false, true, false}
		for i, want := range expected {
			if bitmap[i] != want {
				t.Errorf("bitmap[%d] = %v, want %v", i, bitmap[i], want)
			}
		}
	})

	t.Run("prefix match", func(t *testing.T) {
		pdf := &PushDownFilter{
			Checks: []PushDownCheck{
				{Column: "service.name", Op: PushDownPrefix, Value: "al", ColIdx: svcIdx},
			},
		}
		bitmap := prewhereFilter(f, rg, pdf)
		if bitmap == nil {
			t.Fatal("expected non-nil bitmap")
		}
		expected := []bool{true, false, true, false}
		for i, want := range expected {
			if bitmap[i] != want {
				t.Errorf("bitmap[%d] = %v, want %v", i, bitmap[i], want)
			}
		}
	})

	t.Run("all rows match returns nil", func(t *testing.T) {
		pdf := &PushDownFilter{
			Checks: []PushDownCheck{
				{Column: "service.name", Op: PushDownGreaterThan, Value: "", ColIdx: svcIdx},
			},
		}
		bitmap := prewhereFilter(f, rg, pdf)
		if bitmap != nil {
			t.Error("expected nil bitmap when all rows match")
		}
	})

	t.Run("nil pdf returns nil", func(t *testing.T) {
		bitmap := prewhereFilter(f, rg, nil)
		if bitmap != nil {
			t.Error("expected nil bitmap for nil pdf")
		}
	})

	t.Run("empty checks returns nil", func(t *testing.T) {
		pdf := &PushDownFilter{Checks: []PushDownCheck{}}
		bitmap := prewhereFilter(f, rg, pdf)
		if bitmap != nil {
			t.Error("expected nil bitmap for empty checks")
		}
	})

	t.Run("invalid column index falls through", func(t *testing.T) {
		pdf := &PushDownFilter{
			Checks: []PushDownCheck{
				{Column: "nonexistent_column", Op: PushDownExact, Value: "alpha", ColIdx: -1},
			},
		}
		bitmap := prewhereFilter(f, rg, pdf)
		if bitmap != nil {
			t.Error("expected nil bitmap when column not found")
		}
	})

	t.Run("zero rows returns nil", func(t *testing.T) {
		// Create a file with no rows by writing and reading an empty file.
		// This tests the numRows == 0 guard.
		emptyPath := filepath.Join(dir, "empty.parquet")
		ef, err := os.Create(emptyPath)
		if err != nil {
			t.Fatal(err)
		}
		w := parquet.NewGenericWriter[pushdownTestRow](ef)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		_ = ef.Close()

		data, err := os.ReadFile(emptyPath)
		if err != nil {
			t.Fatal(err)
		}
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		rgs := pf.RowGroups()
		if len(rgs) == 0 {
			// No row groups at all; prewhereFilter would never be called.
			t.Skip("no row groups in empty file")
		}
		bitmap := prewhereFilter(pf, rgs[0], &PushDownFilter{
			Checks: []PushDownCheck{{Column: "service.name", Op: PushDownExact, Value: "x", ColIdx: 0}},
		})
		if bitmap != nil {
			t.Error("expected nil bitmap for zero-row file")
		}
	})
}

// ---------------------------------------------------------------------------
// 3. detectConstantColumns (constant_columns.go ~15) — 53.3% → covered
// ---------------------------------------------------------------------------

func TestMedium_detectConstantColumns(t *testing.T) {
	dir := t.TempDir()

	t.Run("single constant column", func(t *testing.T) {
		// All rows have same service.name = "alpha"
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
		wantCols := map[string]bool{"service.name": true}
		constants := detectConstantColumns(f, rgs[0], wantCols)

		if len(constants) != 1 {
			t.Fatalf("expected 1 constant column, got %d", len(constants))
		}
		if constants[0].name != "service.name" {
			t.Errorf("expected name='service.name', got %q", constants[0].name)
		}
		if constants[0].value != "alpha" {
			t.Errorf("expected value='alpha', got %v", constants[0].value)
		}
	})

	t.Run("varying column not detected", func(t *testing.T) {
		subDir := t.TempDir()
		rows := []pushdownTestRow{
			{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "alpha"},
			{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "beta"},
			{TimestampUnixNano: 3000, Body: "c", SeverityText: "error", ServiceName: "gamma"},
		}
		path := writePushdownTestParquet(t, subDir, rows)
		f := openTestParquet(t, path)
		rgs := f.RowGroups()
		if len(rgs) == 0 {
			t.Fatal("no row groups")
		}
		wantCols := map[string]bool{"service.name": true}
		constants := detectConstantColumns(f, rgs[0], wantCols)
		if len(constants) != 0 {
			t.Errorf("expected 0 constant columns for varying data, got %d", len(constants))
		}
	})

	t.Run("empty wantCols returns nil", func(t *testing.T) {
		subDir := t.TempDir()
		rows := []pushdownTestRow{
			{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "alpha"},
		}
		path := writePushdownTestParquet(t, subDir, rows)
		f := openTestParquet(t, path)
		rgs := f.RowGroups()
		if len(rgs) == 0 {
			t.Fatal("no row groups")
		}
		constants := detectConstantColumns(f, rgs[0], nil)
		if constants != nil {
			t.Error("expected nil for empty wantCols")
		}
	})

	t.Run("nonexistent column skipped", func(t *testing.T) {
		subDir := t.TempDir()
		rows := []pushdownTestRow{
			{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "alpha"},
		}
		path := writePushdownTestParquet(t, subDir, rows)
		f := openTestParquet(t, path)
		rgs := f.RowGroups()
		if len(rgs) == 0 {
			t.Fatal("no row groups")
		}
		wantCols := map[string]bool{"nonexistent_col": true}
		constants := detectConstantColumns(f, rgs[0], wantCols)
		if len(constants) != 0 {
			t.Errorf("expected 0 constants for missing column, got %d", len(constants))
		}
	})

	t.Run("multiple wantCols mixed", func(t *testing.T) {
		subDir := t.TempDir()
		// service.name constant, severity_text varies
		rows := []pushdownTestRow{
			{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "alpha"},
			{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "alpha"},
		}
		path := writePushdownTestParquet(t, subDir, rows)
		f := openTestParquet(t, path)
		rgs := f.RowGroups()
		if len(rgs) == 0 {
			t.Fatal("no row groups")
		}
		wantCols := map[string]bool{"service.name": true, "severity_text": true}
		constants := detectConstantColumns(f, rgs[0], wantCols)
		// service.name should be detected as constant, severity_text should not.
		found := false
		for _, c := range constants {
			if c.name == "service.name" {
				found = true
			}
			if c.name == "severity_text" {
				t.Error("severity_text should NOT be constant")
			}
		}
		if !found {
			t.Error("service.name should be detected as constant")
		}
	})
}

// ---------------------------------------------------------------------------
// 4. stripTimePredicates (filter.go ~51) — 78.9% → cover uncovered branches
// ---------------------------------------------------------------------------

func TestMedium_stripTimePredicates(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"no time predicate", `service.name:="api"`, `service.name:="api"`},
		{"single bracketed time", `_time:[2025-01-01,2025-01-02]`, ``},
		{"time with trailing filter", `_time:[2025-01-01,2025-01-02] service.name:="api"`, ` service.name:="api"`},
		{"time with leading filter", `service.name:="api" _time:[2025-01-01,2025-01-02]`, `service.name:="api" `},
		{"multiple time predicates", `_time:[2025-01-01,2025-01-02] _time:[2025-03-01,2025-04-01]`, ` `},
		{"nested brackets", `_time:[2025-01-01,[nested],end]`, ``},
		{"empty string", ``, ``},
		{"time without brackets (space-delimited)", `_time:5m service.name:="api"`, ` service.name:="api"`},
		{"time without brackets at end", `service.name:="api" _time:5m`, `service.name:="api" `},
		{"time without brackets only", `_time:5m`, ``},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTimePredicates(tt.input)
			if got != tt.want {
				t.Errorf("stripTimePredicates(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 5. parquetValueToAny (storage_query.go ~1714) — 46.2% → covered
// ---------------------------------------------------------------------------

func TestMedium_parquetValueToAny(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
		want any
	}{
		{"null", parquet.NullValue(), ""},
		{"int32", parquet.ValueOf(int32(42)), int32(42)},
		{"int64", parquet.ValueOf(int64(123456)), int64(123456)},
		{"float32", parquet.ValueOf(float32(3.14)), float64(float32(3.14))},
		{"float64", parquet.ValueOf(float64(2.71828)), float64(2.71828)},
		{"bool true", parquet.ValueOf(true), true},
		{"bool false", parquet.ValueOf(false), false},
		{"byte array string", parquet.ValueOf("hello").Level(0, 0, 0), "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parquetValueToAny(tt.val)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("parquetValueToAny() = %v (%T), want %v (%T)",
					got, got, tt.want, tt.want)
			}
		})
	}

	t.Run("non-printable byte array returns hex", func(t *testing.T) {
		val := parquet.ValueOf([]byte{0x00, 0x01, 0x02}).Level(0, 0, 0)
		got := parquetValueToAny(val)
		gotStr, ok := got.(string)
		if !ok {
			t.Fatalf("expected string, got %T", got)
		}
		if gotStr != "000102" {
			t.Errorf("expected hex '000102', got %q", gotStr)
		}
	})
}

// ---------------------------------------------------------------------------
// 6. parquetValueToInterface (reader_projected.go ~163) — 37.5% → covered
// ---------------------------------------------------------------------------

func TestMedium_parquetValueToInterface(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
		want any
	}{
		{"byte array", parquet.ValueOf("hello").Level(0, 0, 0), "hello"},
		{"int64", parquet.ValueOf(int64(42)), int64(42)},
		{"int32", parquet.ValueOf(int32(7)), int64(7)},
		{"float64", parquet.ValueOf(float64(3.14)), float64(3.14)},
		{"bool true", parquet.ValueOf(true), true},
		{"bool false", parquet.ValueOf(false), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parquetValueToInterface(tt.val)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("parquetValueToInterface() = %v (%T), want %v (%T)",
					got, got, tt.want, tt.want)
			}
		})
	}

	t.Run("fixed len byte array", func(t *testing.T) {
		// FixedLenByteArray — create by writing a parquet with FLBA type.
		// For now, test the default branch with a Float value (not handled explicitly).
		val := parquet.ValueOf(float32(1.5))
		got := parquetValueToInterface(val)
		// Float falls to default case which calls v.String()
		if got == nil {
			t.Error("expected non-nil for float value")
		}
	})
}

// ---------------------------------------------------------------------------
// 7. isFreeTextSearch (projection.go ~81) — 75% → cover uncovered branches
// ---------------------------------------------------------------------------

func TestMedium_isFreeTextSearch(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{"empty string", "", false},
		{"wildcard", "*", false},
		{"quoted text", `"error connecting"`, true},
		{"bare word no colon", "error", true},
		{"field:value with colon", `service.name:"api"`, false},
		{"leading spaces + bare word", "  error", true},
		{"leading spaces + quoted", `  "error"`, true},
		{"leading spaces + wildcard", "  *", false},
		{"colon-only string", ":", false},
		{"has colon but no field name", `:="test"`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isFreeTextSearch(tt.query)
			if got != tt.want {
				t.Errorf("isFreeTextSearch(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 8. rowGroupMatchesTimeRange (storage_query.go ~1666) — 78.6% → cover boundaries
// ---------------------------------------------------------------------------

func TestMedium_rowGroupMatchesTimeRange(t *testing.T) {
	dir := t.TempDir()

	// Row group with timestamps [1000, 2000, 3000]
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "svc"},
		{TimestampUnixNano: 2000, Body: "b", SeverityText: "warn", ServiceName: "svc"},
		{TimestampUnixNano: 3000, Body: "c", SeverityText: "error", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	rg := rgs[0]
	tsIdx := findColumnIndex(f.Root(), "timestamp_unix_nano")

	tests := []struct {
		name    string
		startNs int64
		endNs   int64
		want    bool
	}{
		{"overlapping range", 500, 3500, true},
		{"exact boundaries", 1000, 3001, true},
		{"range starts after max", 3001, 5000, false},
		{"range ends before min", 0, 1000, false},
		{"range at max boundary", 3000, 4000, true},
		{"range at min boundary", 0, 1001, true},
		{"range ends at min (exclusive end)", 0, 1000, false},
		{"single point inside", 2000, 2001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rowGroupMatchesTimeRange(rg, tsIdx, tt.startNs, tt.endNs)
			if got != tt.want {
				t.Errorf("rowGroupMatchesTimeRange(rg, %d, %d, %d) = %v, want %v",
					tsIdx, tt.startNs, tt.endNs, got, tt.want)
			}
		})
	}

	t.Run("tsColIdx out of bounds returns true", func(t *testing.T) {
		got := rowGroupMatchesTimeRange(rg, 999, 0, 5000)
		if !got {
			t.Error("expected true for out-of-bounds tsColIdx (conservative)")
		}
	})
}

// ---------------------------------------------------------------------------
// 9. readMapColumnToBlockCols (reader_columnar.go ~224) — 0% → covered
// ---------------------------------------------------------------------------

func TestMedium_readMapColumnToBlockCols(t *testing.T) {
	dir := t.TempDir()

	type mapRow struct {
		TimestampUnixNano int64             `parquet:"timestamp_unix_nano"`
		Attrs             map[string]string `parquet:"attrs"`
	}

	path := filepath.Join(dir, "map_test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[mapRow](f, parquet.Compression(&parquet.Zstd))
	rows := []mapRow{
		{TimestampUnixNano: 1000, Attrs: map[string]string{"host": "server1", "env": "prod"}},
		{TimestampUnixNano: 2000, Attrs: map[string]string{"host": "server2", "env": "staging"}},
		{TimestampUnixNano: 3000, Attrs: map[string]string{"host": "server3", "region": "us-east"}},
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := pf.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	rg := rgs[0]

	// Find the MAP columns: attrs has key and value sub-columns.
	root := pf.Root()
	attrsCol := root.Column("attrs")
	if attrsCol == nil {
		t.Fatal("attrs column not found in schema")
	}

	// MAP columns in parquet have structure: attrs -> key_value -> key, value
	// Find the key and value leaf column indices.
	var keyIdx, valIdx = -1, -1
	for _, col := range attrsCol.Columns() {
		for _, leaf := range col.Columns() {
			if leaf.Name() == "key" {
				keyIdx = leaf.Index()
			} else if leaf.Name() == "value" {
				valIdx = leaf.Index()
			}
		}
	}
	if keyIdx < 0 || valIdx < 0 {
		t.Fatalf("could not find MAP key/value columns: keyIdx=%d, valIdx=%d", keyIdx, valIdx)
	}

	cols := rg.ColumnChunks()
	keyChunk := cols[keyIdx]
	valChunk := cols[valIdx]

	rowMask := []bool{true, true, true}
	passCount := 3

	result := readMapColumnToBlockCols(keyChunk, valChunk, 3, rowMask, passCount, "attrs", nil)

	if len(result) == 0 {
		t.Fatal("expected at least one block column from MAP data")
	}

	// Verify we got the expected attribute columns.
	colNames := make(map[string]bool)
	for _, bc := range result {
		colNames[bc.Name] = true
		if len(bc.Values) != passCount {
			t.Errorf("column %q has %d values, expected %d", bc.Name, len(bc.Values), passCount)
		}
	}

	// attrs has keys: host, env, region
	for _, key := range []string{"attrs:host", "attrs:env", "attrs:region"} {
		if !colNames[key] {
			t.Errorf("expected column %q in result", key)
		}
	}

	t.Run("with row mask filtering", func(t *testing.T) {
		rowMask := []bool{true, false, true}
		passCount := 2
		result := readMapColumnToBlockCols(keyChunk, valChunk, 3, rowMask, passCount, "attrs", nil)

		for _, bc := range result {
			if len(bc.Values) != passCount {
				t.Errorf("column %q has %d values, expected %d", bc.Name, len(bc.Values), passCount)
			}
		}
	})

	t.Run("with promoted keys excluded", func(t *testing.T) {
		promoted := map[string]bool{"host": true}
		result := readMapColumnToBlockCols(keyChunk, valChunk, 3, rowMask, 3, "attrs", promoted)

		for _, bc := range result {
			if bc.Name == "attrs:host" {
				t.Error("promoted key 'host' should be excluded from MAP results")
			}
		}
	})
}

// ---------------------------------------------------------------------------
// 10. NewFooterCache (footer_cache.go ~35) — 66.7% → cover uncovered init paths
// ---------------------------------------------------------------------------

func TestMedium_NewFooterCache(t *testing.T) {
	t.Run("zero maxItems defaults to 10000", func(t *testing.T) {
		fc := NewFooterCache(0)
		if fc.maxItems != 10000 {
			t.Errorf("expected maxItems=10000, got %d", fc.maxItems)
		}
		if fc.items == nil {
			t.Error("expected non-nil items map")
		}
		if fc.lru == nil {
			t.Error("expected non-nil LRU list")
		}
	})

	t.Run("negative maxItems defaults to 10000", func(t *testing.T) {
		fc := NewFooterCache(-5)
		if fc.maxItems != 10000 {
			t.Errorf("expected maxItems=10000, got %d", fc.maxItems)
		}
	})

	t.Run("positive maxItems used as-is", func(t *testing.T) {
		fc := NewFooterCache(42)
		if fc.maxItems != 42 {
			t.Errorf("expected maxItems=42, got %d", fc.maxItems)
		}
	})

	t.Run("Has method", func(t *testing.T) {
		fc := NewFooterCache(10)
		fc.Put("key1", &CachedFooter{FileSize: 100})
		if !fc.Has("key1") {
			t.Error("expected Has('key1') = true")
		}
		if fc.Has("nonexistent") {
			t.Error("expected Has('nonexistent') = false")
		}
	})
}

// ---------------------------------------------------------------------------
// Additional: findColumnIndex (storage_query.go ~1691) — exercise fallback
// ---------------------------------------------------------------------------

func TestMedium_findColumnIndex(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	t.Run("existing column", func(t *testing.T) {
		idx := findColumnIndex(f.Root(), "body")
		if idx < 0 {
			t.Error("expected non-negative index for 'body'")
		}
	})

	t.Run("nonexistent column returns -1", func(t *testing.T) {
		idx := findColumnIndex(f.Root(), "nonexistent")
		if idx != -1 {
			t.Errorf("expected -1 for nonexistent column, got %d", idx)
		}
	})
}

// ---------------------------------------------------------------------------
// Additional: columnNames (storage_query.go ~1705)
// ---------------------------------------------------------------------------

func TestMedium_columnNames(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	names := columnNames(f.Root())

	expected := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
	}
	for _, name := range names {
		if !expected[name] {
			t.Errorf("unexpected column name: %q", name)
		}
		delete(expected, name)
	}
	for name := range expected {
		t.Errorf("missing column name: %q", name)
	}
}

// ---------------------------------------------------------------------------
// Additional: valueToString (storage_query.go ~1740)
// ---------------------------------------------------------------------------

func TestMedium_valueToString(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
		want string
	}{
		{"null", parquet.NullValue(), ""},
		{"int32", parquet.ValueOf(int32(42)), "42"},
		{"int64", parquet.ValueOf(int64(123456)), "123456"},
		{"float32", parquet.ValueOf(float32(3.14)), "3.14"},
		{"float64", parquet.ValueOf(float64(2.71828)), "2.71828"},
		{"bool true", parquet.ValueOf(true), "true"},
		{"bool false", parquet.ValueOf(false), "false"},
		{"byte array", parquet.ValueOf("hello").Level(0, 0, 0), "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valueToString(tt.val)
			if got != tt.want {
				t.Errorf("valueToString() = %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("non-printable byte array returns hex", func(t *testing.T) {
		val := parquet.ValueOf([]byte{0x00, 0x01, 0x02}).Level(0, 0, 0)
		got := valueToString(val)
		if got != "000102" {
			t.Errorf("expected hex '000102', got %q", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Additional: isPrintable (storage_query.go ~1785)
// ---------------------------------------------------------------------------

func TestMedium_isPrintable(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  bool
	}{
		{"empty", []byte{}, true},
		{"ascii text", []byte("hello world"), true},
		{"with tab", []byte("hello\tworld"), true},
		{"with newline", []byte("hello\nworld"), true},
		{"with carriage return", []byte("hello\rworld"), true},
		{"control char 0x01", []byte{0x01}, false},
		{"null byte", []byte{0x00}, false},
		{"mixed printable and control", []byte("hello\x01world"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPrintable(tt.input)
			if got != tt.want {
				t.Errorf("isPrintable(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: isTimeOnlyFilter (filter.go ~40)
// ---------------------------------------------------------------------------

func TestMedium_isTimeOnlyFilter(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", true},
		{"wildcard", "*", true},
		{"time only bracketed", `_time:[2025-01-01,2025-01-02]`, true},
		{"time only no bracket", `_time:5m`, true},
		{"time plus field filter", `_time:[2025-01-01,2025-01-02] service.name:="api"`, false},
		{"field only", `service.name:="api"`, false},
		{"multiple times", `_time:[2025-01-01,2025-01-02] _time:[2025-03-01,2025-04-01]`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTimeOnlyFilter(tt.input)
			if got != tt.want {
				t.Errorf("isTimeOnlyFilter(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: mapColumnToAttrPrefix (storage_query.go ~833)
// ---------------------------------------------------------------------------

func TestMedium_mapColumnToAttrPrefix(t *testing.T) {
	tests := []struct {
		col  string
		want string
	}{
		{"resource.attributes", ""},
		{"log.attributes", ""},
		{"span.attributes", ""},
		{"scope.attributes", ""},
		{"custom.col", "custom.col:"},
		{"attrs", "attrs:"},
	}

	for _, tt := range tests {
		t.Run(tt.col, func(t *testing.T) {
			got := mapColumnToAttrPrefix(tt.col)
			if got != tt.want {
				t.Errorf("mapColumnToAttrPrefix(%q) = %q, want %q", tt.col, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: partitionFromKey (storage_query.go ~1244) — 91.7%
// ---------------------------------------------------------------------------

func TestMedium_partitionFromKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"hourly partition", "logs/dt=2025-01-15/hour=10/file.parquet", "logs/dt=2025-01-15/hour=10"},
		{"daily partition", "logs/dt=2025-01-15/file.parquet", "dt=2025-01-15"},
		{"daily partition at end", "dt=2025-01-15", "dt=2025-01-15"},
		{"no partition", "file.parquet", "file.parquet"},
		{"hourly with trailing path", "ns/dt=2025-05-25/hour=14/chunk-001.parquet", "ns/dt=2025-05-25/hour=14"},
		{"empty key", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := partitionFromKey(tt.key)
			if got != tt.want {
				t.Errorf("partitionFromKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: extractExactMatch (storage_query.go ~1341) — 80%
// ---------------------------------------------------------------------------

func TestMedium_extractExactMatch(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		fieldName string
		want      string
	}{
		{"quoted exact match :=", `trace_id:="abc123"`, "trace_id", "abc123"},
		{"quoted substring :", `trace_id:"abc123"`, "trace_id", "abc123"},
		{"unquoted exact match", `trace_id:=abc123`, "trace_id", "abc123"},
		{"unquoted with space", `trace_id:=abc123 other`, "trace_id", "abc123"},
		{"unquoted with pipe", `trace_id:=abc123|something`, "trace_id", "abc123"},
		{"no match", `service.name:="api"`, "trace_id", ""},
		{"empty query", "", "trace_id", ""},
		{"unclosed quote :=", `trace_id:="unclosed`, "trace_id", ""},
		{"unclosed quote :", `trace_id:"unclosed`, "trace_id", ""},
		{"empty value :=", `trace_id:=""`, "trace_id", ""},
		{"quoted prevents unquoted fallback", `trace_id:="abc"`, "trace_id", "abc"},
		{"unquoted at end of string", `trace_id:=xyz`, "trace_id", "xyz"},
		{"unquoted with paren", `trace_id:=abc)`, "trace_id", "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractExactMatch(tt.query, tt.fieldName)
			if got != tt.want {
				t.Errorf("extractExactMatch(%q, %q) = %q, want %q", tt.query, tt.fieldName, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: extractInValues (storage_query.go ~1307) — 93.8%
// ---------------------------------------------------------------------------

func TestMedium_extractInValues(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		fieldName string
		want      []string
	}{
		{"in values", `service.name:in("api","web","worker")`, "service.name", []string{"api", "web", "worker"}},
		{"no match", `service.name:="api"`, "service.name", nil},
		{"empty in list", `service.name:in()`, "service.name", nil},
		{"unclosed paren", `service.name:in("api","web"`, "service.name", nil},
		{"single value", `service.name:in("api")`, "service.name", []string{"api"}},
		{"different field", `level:in("error","warn")`, "service.name", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractInValues(tt.query, tt.fieldName)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d values, got %d: %v", len(tt.want), len(got), got)
			}
			for i, v := range tt.want {
				if got[i] != v {
					t.Errorf("value[%d] = %q, want %q", i, got[i], v)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: extractFilterValues (storage_query.go ~1331)
// ---------------------------------------------------------------------------

func TestMedium_extractFilterValues(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		fieldName string
		wantLen   int
	}{
		{"exact match", `trace_id:="abc"`, "trace_id", 1},
		{"in values", `trace_id:in("a","b","c")`, "trace_id", 3},
		{"no match", `service.name:="api"`, "trace_id", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFilterValues(tt.query, tt.fieldName)
			if len(got) != tt.wantLen {
				t.Errorf("extractFilterValues(%q, %q) returned %d values, want %d: %v",
					tt.query, tt.fieldName, len(got), tt.wantLen, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: resolvePushDownIndices (filter_pushdown.go ~151) — 28.6%
// ---------------------------------------------------------------------------

func TestMedium_resolvePushDownIndices(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	t.Run("nil filter returns nil", func(t *testing.T) {
		got := resolvePushDownIndices(f, nil)
		if got != nil {
			t.Error("expected nil for nil filter")
		}
	})

	t.Run("resolves existing columns", func(t *testing.T) {
		pdf := &PushDownFilter{
			Checks: []PushDownCheck{
				{Column: "service.name", Op: PushDownExact, Value: "svc"},
				{Column: "body", Op: PushDownPrefix, Value: "hel"},
			},
		}
		got := resolvePushDownIndices(f, pdf)
		if got == nil {
			t.Fatal("expected non-nil result")
		}
		if len(got.Checks) != 2 {
			t.Fatalf("expected 2 checks, got %d", len(got.Checks))
		}
		for _, check := range got.Checks {
			if check.ColIdx < 0 {
				t.Errorf("expected non-negative ColIdx for %q, got %d", check.Column, check.ColIdx)
			}
		}
	})

	t.Run("missing column gets -1 index", func(t *testing.T) {
		pdf := &PushDownFilter{
			Checks: []PushDownCheck{
				{Column: "nonexistent", Op: PushDownExact, Value: "x"},
			},
		}
		got := resolvePushDownIndices(f, pdf)
		if got == nil {
			t.Fatal("expected non-nil result")
		}
		if len(got.Checks) != 1 {
			t.Fatalf("expected 1 check, got %d", len(got.Checks))
		}
		if got.Checks[0].ColIdx != -1 {
			t.Errorf("expected -1 for nonexistent column, got %d", got.Checks[0].ColIdx)
		}
	})
}

// ---------------------------------------------------------------------------
// Additional: resolveBloomCheckIndices (storage_query.go ~1265) — 50%
// ---------------------------------------------------------------------------

func TestMedium_resolveBloomCheckIndices(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	t.Run("resolves existing columns", func(t *testing.T) {
		checks := []bloomCheck{
			{colName: "service.name", value: parquet.ValueOf("svc")},
			{colName: "body", value: parquet.ValueOf("hello")},
		}
		got := resolveBloomCheckIndices(f, checks)
		if len(got) != 2 {
			t.Fatalf("expected 2 resolved checks, got %d", len(got))
		}
		for _, c := range got {
			if c.colIdx < 0 {
				t.Errorf("expected non-negative colIdx for %q", c.colName)
			}
		}
	})

	t.Run("missing columns are filtered out", func(t *testing.T) {
		checks := []bloomCheck{
			{colName: "nonexistent", value: parquet.ValueOf("x")},
			{colName: "service.name", value: parquet.ValueOf("svc")},
		}
		got := resolveBloomCheckIndices(f, checks)
		if len(got) != 1 {
			t.Fatalf("expected 1 resolved check (nonexistent filtered out), got %d", len(got))
		}
		if got[0].colName != "service.name" {
			t.Errorf("expected service.name, got %q", got[0].colName)
		}
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		got := resolveBloomCheckIndices(f, nil)
		if len(got) != 0 {
			t.Errorf("expected 0 checks, got %d", len(got))
		}
	})
}

// ---------------------------------------------------------------------------
// Additional: allLeafColumns (storage_query.go ~584)
// ---------------------------------------------------------------------------

func TestMedium_allLeafColumns(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{
		{TimestampUnixNano: 1000, Body: "hello", SeverityText: "info", ServiceName: "svc"},
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)

	cols := allLeafColumns(f)
	expected := []string{"timestamp_unix_nano", "body", "severity_text", "service.name"}
	for _, name := range expected {
		if !cols[name] {
			t.Errorf("expected column %q in allLeafColumns result", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Additional: logRowToFields (storage_query.go ~847) — 66.7%
// ---------------------------------------------------------------------------

func TestMedium_logRowToFields(t *testing.T) {
	row := &schema.LogRow{
		TimestampUnixNano: 1234567890,
		Body:              "test message",
		SeverityText:      "error",
		SeverityNumber:    17,
		ServiceName:       "api-gateway",
		K8sNamespaceName:  "prod",
		K8sPodName:        "api-pod-1",
		TraceID:           "trace-abc",
		SpanID:            "span-123",
		ResourceAttributes: map[string]string{
			"custom.resource": "val1",
		},
		LogAttributes: map[string]string{
			"custom.log": "val2",
		},
	}

	var buf []field
	fields := logRowToFields(row, buf)

	if len(fields) == 0 {
		t.Fatal("expected non-empty fields")
	}

	fieldMap := make(map[string]any)
	for _, f := range fields {
		fieldMap[f.name] = f.value
	}

	// Check promoted fields
	if v, ok := fieldMap["_time"]; !ok || v != int64(1234567890) {
		t.Errorf("_time = %v, want 1234567890", v)
	}
	if v, ok := fieldMap["_msg"]; !ok || v != "test message" {
		t.Errorf("_msg = %v, want 'test message'", v)
	}
	if v, ok := fieldMap["level"]; !ok || v != "error" {
		t.Errorf("level = %v, want 'error'", v)
	}
	if v, ok := fieldMap["service.name"]; !ok || v != "api-gateway" {
		t.Errorf("service.name = %v, want 'api-gateway'", v)
	}
	if v, ok := fieldMap["trace_id"]; !ok || v != "trace-abc" {
		t.Errorf("trace_id = %v, want 'trace-abc'", v)
	}
	// Check map attributes
	if v, ok := fieldMap["custom.resource"]; !ok || v != "val1" {
		t.Errorf("custom.resource = %v, want 'val1'", v)
	}
	if v, ok := fieldMap["custom.log"]; !ok || v != "val2" {
		t.Errorf("custom.log = %v, want 'val2'", v)
	}
}

// ---------------------------------------------------------------------------
// Additional: traceRowToFields (storage_query.go ~876) — 90%
// ---------------------------------------------------------------------------

func TestMedium_traceRowToFields(t *testing.T) {
	row := &schema.TraceRow{
		TimestampUnixNano: 1234567890,
		StartTimeUnixNano: 1234567800,
		TraceID:           "trace-abc",
		SpanID:            "span-123",
		ParentSpanID:      "span-000",
		SpanName:          "GET /api/users",
		SpanKind:          2, // SERVER
		StatusCode:        1, // OK
		StatusMessage:     "success",
		DurationNs:        90,
		ServiceName:       "api-gateway",
		ScopeName:         "my.library",
		DeployEnv:         "prod",
		CloudRegion:       "us-east-1",
		HostName:          "host-1",
		HTTPMethod:        "GET",
		HTTPStatusCode:    "200",
		HTTPUrl:           "/api/users",
		DBSystem:          "postgres",
		DBStatement:       "SELECT * FROM users",
		ResourceAttributes: map[string]string{
			"custom.resource": "val1",
			"service.name":    "should-be-skipped", // promoted key
		},
		SpanAttributes: map[string]string{
			"custom.span": "val2",
			"http.method": "should-be-skipped", // promoted key
		},
		ScopeAttributes: map[string]string{
			"scope.attr": "val3",
		},
	}

	var buf []field
	fields := traceRowToFields(row, buf)

	if len(fields) == 0 {
		t.Fatal("expected non-empty fields")
	}

	fieldMap := make(map[string]any)
	for _, f := range fields {
		fieldMap[f.name] = f.value
	}

	// Check promoted trace fields
	if v, ok := fieldMap["trace_id"]; !ok || v != "trace-abc" {
		t.Errorf("trace_id = %v, want 'trace-abc'", v)
	}
	if v, ok := fieldMap["name"]; !ok || v != "GET /api/users" {
		t.Errorf("name = %v, want 'GET /api/users'", v)
	}
	if _, ok := fieldMap["http.method"]; !ok {
		t.Error("http.method missing from fields")
	}
	if _, ok := fieldMap["db.system"]; !ok {
		t.Error("db.system missing from fields")
	}

	// Check custom attributes
	if v, ok := fieldMap["custom.resource"]; !ok || v != "val1" {
		t.Errorf("custom.resource = %v, want 'val1'", v)
	}
	if v, ok := fieldMap["custom.span"]; !ok || v != "val2" {
		t.Errorf("custom.span = %v, want 'val2'", v)
	}
	if v, ok := fieldMap["scope.attr"]; !ok || v != "val3" {
		t.Errorf("scope.attr = %v, want 'val3'", v)
	}
}

// ---------------------------------------------------------------------------
// Additional: referencesField (projection.go ~65)
// ---------------------------------------------------------------------------

func TestMedium_referencesField(t *testing.T) {
	tests := []struct {
		name  string
		query string
		field string
		want  bool
	}{
		{"exact match :=", `service.name:="api"`, "service.name", true},
		{"substring :", `service.name:"api"`, "service.name", true},
		{"assignment :=", `service.name:=api`, "service.name", true},
		{"in operator", `service.name:in("a","b")`, "service.name", true},
		{"bare colon", `service.name:something`, "service.name", true},
		{"no reference", `level:="error"`, "service.name", false},
		{"partial name no match", `service:="api"`, "service.name", false},
		{"empty query", "", "service.name", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := referencesField(tt.query, tt.field)
			if got != tt.want {
				t.Errorf("referencesField(%q, %q) = %v, want %v", tt.query, tt.field, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Additional: hasColumnSelectingPipe (projection.go ~50)
// ---------------------------------------------------------------------------

func TestMedium_hasColumnSelectingPipe(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`* | fields _time, _msg`, true},
		{`* | stats count()`, true},
		{`* | uniq by(level)`, true},
		{`* | top 10 by(level)`, true},
		{`* | sort by(_time) desc`, false},
		{`* | limit 10`, false},
		{`service.name:="api"`, false},
		{`*`, false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := hasColumnSelectingPipe(tt.query)
			if got != tt.want {
				t.Errorf("hasColumnSelectingPipe(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}
