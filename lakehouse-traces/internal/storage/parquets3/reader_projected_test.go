package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestReadRowGroupProjected_TracesSelectedColumns(t *testing.T) {
	rows := []schema.TraceRow{
		{
			TimestampUnixNano: 1000000000,
			TraceID:           "trace-1",
			SpanID:            "span-1",
			SpanName:          "GET /api",
			ServiceName:       "api-svc",
		},
		{
			TimestampUnixNano: 2000000000,
			TraceID:           "trace-2",
			SpanID:            "span-2",
			SpanName:          "POST /data",
			ServiceName:       "web-svc",
		},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	_, err := w.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	wantCols := map[string]bool{
		"timestamp_unix_nano": true,
		"trace_id":            true,
	}

	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}

	for i, row := range result {
		if len(row) != 2 {
			t.Errorf("row %d: expected 2 fields, got %d", i, len(row))
		}
		hasTimestamp := false
		hasTraceID := false
		for _, fld := range row {
			switch fld.name {
			case "timestamp_unix_nano":
				hasTimestamp = true
			case "trace_id":
				hasTraceID = true
			}
		}
		if !hasTimestamp {
			t.Errorf("row %d: missing timestamp_unix_nano", i)
		}
		if !hasTraceID {
			t.Errorf("row %d: missing trace_id", i)
		}
	}

	// Verify values
	for _, fld := range result[0] {
		if fld.name == "timestamp_unix_nano" {
			if ts, ok := fld.value.(int64); !ok || ts != 1000000000 {
				t.Errorf("row 0: expected timestamp 1000000000, got %v", fld.value)
			}
		}
		if fld.name == "trace_id" {
			if tid, ok := fld.value.(string); !ok || tid != "trace-1" {
				t.Errorf("row 0: expected trace_id 'trace-1', got %v", fld.value)
			}
		}
	}
	for _, fld := range result[1] {
		if fld.name == "timestamp_unix_nano" {
			if ts, ok := fld.value.(int64); !ok || ts != 2000000000 {
				t.Errorf("row 1: expected timestamp 2000000000, got %v", fld.value)
			}
		}
		if fld.name == "trace_id" {
			if tid, ok := fld.value.(string); !ok || tid != "trace-2" {
				t.Errorf("row 1: expected trace_id 'trace-2', got %v", fld.value)
			}
		}
	}
}

func TestReadRowGroupProjected_TracesNilWantCols(t *testing.T) {
	result, err := readRowGroupProjected(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("expected nil for nil wantCols")
	}
}

func TestReadRowGroupProjected_TracesNoMatchingColumns(t *testing.T) {
	rows := []schema.TraceRow{{TimestampUnixNano: 1000, TraceID: "test"}}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	_, _ = w.Write(rows)
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rgs := f.RowGroups()

	wantCols := map[string]bool{"nonexistent_column": true}
	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil result for non-matching columns, got %d rows", len(result))
	}
}

func TestReadRowGroupProjected_TracesEmptyWantCols(t *testing.T) {
	rows := []schema.TraceRow{{TimestampUnixNano: 1000, TraceID: "test"}}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	_, _ = w.Write(rows)
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rgs := f.RowGroups()

	wantCols := map[string]bool{}
	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty wantCols, got %d rows", len(result))
	}
}

func TestReadRowGroupProjected_TracesSingleColumn(t *testing.T) {
	rows := []schema.TraceRow{
		{TimestampUnixNano: 100, TraceID: "t1", SpanName: "op1"},
		{TimestampUnixNano: 200, TraceID: "t2", SpanName: "op2"},
		{TimestampUnixNano: 300, TraceID: "t3", SpanName: "op3"},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	_, _ = w.Write(rows)
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rgs := f.RowGroups()

	wantCols := map[string]bool{"trace_id": true}
	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	expected := []string{"t1", "t2", "t3"}
	for i, row := range result {
		if len(row) != 1 {
			t.Errorf("row %d: expected 1 field, got %d", i, len(row))
			continue
		}
		if row[0].name != "trace_id" {
			t.Errorf("row %d: expected field name 'trace_id', got %q", i, row[0].name)
		}
		if row[0].value != expected[i] {
			t.Errorf("row %d: expected %q, got %v", i, expected[i], row[0].value)
		}
	}
}
