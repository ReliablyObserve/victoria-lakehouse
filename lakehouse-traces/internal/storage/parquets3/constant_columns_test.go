package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestDetectConstantColumns_SkipsByteArray regression-guards the
// truncation fix for /select/jaeger/api/services. When a row group has
// only one distinct value for a BYTE_ARRAY (string) column, parquet's
// column-index min/max may be truncated per the PageIndex spec. Treating
// that truncated min == max as a constant injects the truncated bytes
// as the column's row value — which surfaced as e.g. "notification-ser"
// alongside "notification-service" in field-value APIs.
//
// The fix skips constant-column detection for any column whose min/max
// is a ByteArray Kind. This test writes a file where the constant
// value is long enough that the writer truncates it in column-index,
// then asserts detectConstantColumns returns it as NOT constant — i.e.
// callers fall through to actual data-page reads which return the full,
// untruncated value.
func TestDetectConstantColumns_SkipsByteArray(t *testing.T) {
	type row struct {
		ServiceName string `parquet:"service.name"`
	}
	rows := make([]row, 100)
	for i := range rows {
		rows[i] = row{ServiceName: "notification-service-with-very-long-name"}
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rg := f.RowGroups()[0]
	got := detectConstantColumns(f, rg, map[string]bool{"service.name": true})

	if len(got) != 0 {
		t.Errorf("ByteArray column should never be detected as constant (would inject truncated min/max as row value); got %d constants: %+v", len(got), got)
	}
}

// TestDetectConstantColumns_AllowsFixedWidth verifies fixed-width
// columns (Int64) are still optimized as constants when min == max.
// Truncation only applies to BYTE_ARRAY in column-index stats; numeric
// types serialize their full value and remain safe to treat as constant.
func TestDetectConstantColumns_AllowsFixedWidth(t *testing.T) {
	type row struct {
		SpanKind int64 `parquet:"span.kind"`
	}
	rows := make([]row, 100)
	for i := range rows {
		rows[i] = row{SpanKind: 2}
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rg := f.RowGroups()[0]
	got := detectConstantColumns(f, rg, map[string]bool{"span.kind": true})

	if len(got) != 1 {
		t.Fatalf("Int64 constant column should be detected; got %d constants: %+v", len(got), got)
	}
	if got[0].name != "span.kind" {
		t.Errorf("got constant name %q, want span.kind", got[0].name)
	}
}
