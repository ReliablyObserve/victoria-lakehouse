package parquets3

import (
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// #274 on the traces side: the numeric span columns (start_time_unix_nano,
// duration_ns, status.code, span.kind) are nullable. A row that is not a span
// (a service-graph edge row, which hot VictoriaTraces stores with only parent,
// child and callCount) must not be given kind/status_code/duration/start_time
// "0", while a span whose status is UNSET (0) or whose kind is UNSPECIFIED (0)
// keeps its explicit 0.

func spanNumericAbsentRows(now time.Time) []schema.TraceRow {
	return []schema.TraceRow{
		// a span with explicit zeros
		{TimestampUnixNano: now.UnixNano(), TraceID: "t1", SpanID: "s1", SpanName: "unset",
			StartTimeUnixNano: schema.Int64Ptr(now.UnixNano() - 5), DurationNs: schema.Int64Ptr(5),
			StatusCode: schema.Int32Ptr(0), SpanKind: schema.Int32Ptr(0), Stream: `{svc="a"}`, StreamID: "st1"},
		// a normal span
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), TraceID: "t2", SpanID: "s2", SpanName: "ok",
			StartTimeUnixNano: schema.Int64Ptr(now.UnixNano()), DurationNs: schema.Int64Ptr(1000),
			StatusCode: schema.Int32Ptr(2), SpanKind: schema.Int32Ptr(3), Stream: `{svc="a"}`, StreamID: "st1"},
		// a service-graph edge row: no numeric span column
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Stream: `{trace_service_graph_stream="-"}`, StreamID: "sg",
			ServiceGraphParent: "frontend", ServiceGraphChild: "api", ServiceGraphCallCount: "17"},
	}
}

func spanNumericFields() []string {
	return []string{"duration", "kind", "start_time_unix_nano", "status_code"}
}

func TestSpanNumericAbsent_ParquetColumnsAreNullable(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	res, err := writeTracesParquet(spanNumericAbsentRows(now), 1000, 3)
	if err != nil {
		t.Fatal(err)
	}
	f := openParquetBytes(t, res.Data)
	for _, name := range []string{"start_time_unix_nano", "duration_ns", "status.code", "span.kind"} {
		col := f.Root().Column(name)
		if col == nil || !col.Optional() {
			t.Fatalf("%s must be an optional column so an absent value is NULL, not 0", name)
		}
		nulls, zeros := 0, 0
		for _, rg := range f.RowGroups() {
			pages := rg.ColumnChunks()[col.Index()].Pages()
			for {
				p, err := pages.ReadPage()
				if err != nil {
					break
				}
				nulls += int(p.NumNulls())
				buf := make([]parquet.Value, int(p.NumValues()))
				n, _ := p.Values().ReadValues(buf)
				for _, v := range buf[:n] {
					if !v.IsNull() && v.Kind() == parquet.Int32 && v.Int32() == 0 {
						zeros++
					}
					if !v.IsNull() && v.Kind() == parquet.Int64 && v.Int64() == 0 {
						zeros++
					}
				}
			}
			_ = pages.Close()
		}
		wantZeros := 0
		if name == "status.code" || name == "span.kind" {
			wantZeros = 1
		}
		if nulls != 1 || zeros != wantZeros {
			t.Errorf("%s: %d NULL and %d zero cells, want 1 NULL (the service-graph row) and %d zero", name, nulls, zeros, wantZeros)
		}
	}
}

func TestSpanNumericAbsent_ScanPathsKeepAbsentAbsent(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
	for _, rgSize := range []int{1, 1000} {
		res, err := writeTracesParquet(spanNumericAbsentRows(now), rgSize, 3)
		if err != nil {
			t.Fatal(err)
		}
		f := openParquetBytes(t, res.Data)
		s := tracesTestStorage()
		for name, rows := range map[string][]map[string]string{
			"scan":  scanRowGroups(t, s, f, startNs, endNs, nil),
			"typed": scanRowGroupsTyped(t, s, f, startNs, endNs),
		} {
			if len(rows) != 3 {
				t.Fatalf("rowGroupSize=%d %s: %d rows, want 3", rgSize, name, len(rows))
			}
			for _, r := range rows {
				switch {
				case r["parent"] != "": // the service-graph row
					for _, k := range spanNumericFields() {
						if v, ok := r[k]; ok {
							t.Errorf("rowGroupSize=%d %s: service-graph row carries %s=%q, hot has none", rgSize, name, k, v)
						}
					}
				case r["name"] == "unset":
					for _, k := range []string{"kind", "status_code"} {
						if r[k] != "0" {
							t.Errorf("rowGroupSize=%d %s: explicit-0 span has %s=%q, want \"0\"", rgSize, name, k, r[k])
						}
					}
				default:
					if r["kind"] != "3" || r["status_code"] != "2" || r["duration"] != "1000" {
						t.Errorf("rowGroupSize=%d %s: span fields %v", rgSize, name, r)
					}
				}
			}
		}
	}
}

func TestSpanNumericAbsent_BridgeBlockMatchesFileRows(t *testing.T) {
	s := tracesTestStorage()
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := spanNumericAbsentRows(now)
	db := s.traceRowsToDataBlock(tenantScope{account: "0", project: "0"}, "test", rows)
	if db == nil {
		t.Fatal("no block")
	}
	cols := map[string][]string{}
	for _, c := range db.GetColumns(false) {
		cols[c.Name] = c.Values
	}
	for _, k := range spanNumericFields() {
		v := cols[k]
		if len(v) != 3 || v[2] != "" {
			t.Errorf("bridge %s per row = %q, want the service-graph row (3rd) empty", k, v)
		}
	}
	if strings.Join([]string{cols["kind"][0], cols["status_code"][0]}, ",") != "0,0" {
		t.Errorf("bridge lost the explicit zeros: kind %q status_code %q", cols["kind"], cols["status_code"])
	}
}
