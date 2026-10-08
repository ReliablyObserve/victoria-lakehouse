package parquets3

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// Every builder of raw-row blocks emits its columns in upstream's order, the
// same on every call (#427): a sort over all columns keys rows on the block's
// column order.

func blockColumnNames(db *logstorage.DataBlock) string {
	var out []string
	for _, c := range db.GetColumns(false) {
		out = append(out, c.Name)
	}
	return strings.Join(out, ",")
}

// requireUpstreamOrder fails unless db's columns are already in the order
// storage.OrderColumnsLikeUpstream gives them.
func requireUpstreamOrder(t *testing.T, what string, db *logstorage.DataBlock) string {
	t.Helper()
	if db == nil || db.RowsCount() == 0 {
		t.Fatalf("%s: no block", what)
	}
	got := blockColumnNames(db)
	ref := &logstorage.DataBlock{}
	ref.SetColumns(append([]logstorage.BlockColumn(nil), db.GetColumns(false)...))
	storage.OrderColumnsLikeUpstream(ref)
	if want := blockColumnNames(ref); got != want {
		t.Fatalf("%s: columns %s, want upstream's order %s", what, got, want)
	}
	if !strings.HasPrefix(got, "_time,") {
		t.Fatalf("%s: _time is not first: %s", what, got)
	}
	return got
}

func orderTestLogRows(n int) []schema.LogRow {
	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC).UnixNano()
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: base + int64(i%2)*int64(time.Millisecond),
			Body:              fmt.Sprintf("message %d", i),
			SeverityText:      "INFO",
			SeverityNumber:    schema.Int32Ptr(9),
			ServiceName:       fmt.Sprintf("svc-%d", i%3),
			TraceID:           fmt.Sprintf("trace-%d", i),
			SpanID:            "span",
			Stream:            fmt.Sprintf(`{service.name="svc-%d"}`, i%3),
			StreamID:          fmt.Sprintf("sid-%d", i%3),
			K8sNamespaceName:  "prod",
			HostName:          fmt.Sprintf("host-%d", i%2),
		}
	}
	return rows
}

func TestBlockBuilders_EmitUpstreamColumnOrder(t *testing.T) {
	rows := orderTestLogRows(12)
	maxNs := int64(^uint64(0) >> 1)

	t.Run("columnar", func(t *testing.T) {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[schema.LogRow](&buf)
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
		reg := schema.NewRegistry(schema.LogsProfile)
		want := map[string]bool{}
		for _, c := range f.Schema().Columns() {
			want[c[0]] = true
		}
		var first string
		for i := 0; i < 30; i++ {
			db := readRowGroupColumnar(f, f.RowGroups()[0], want, reg, 0, maxNs, nil, nil)
			got := requireUpstreamOrder(t, "readRowGroupColumnar", db)
			if i == 0 {
				first = got
			} else if got != first {
				t.Fatalf("read %d: columns %s, first read %s", i, got, first)
			}
		}
	})

	t.Run("projected", func(t *testing.T) {
		base := time.Now().UnixNano()
		in := make([][]field, 6)
		for i := range in {
			in[i] = []field{
				{name: "service.name", value: fmt.Sprintf("svc-%d", i%2)},
				{name: "level", value: "info"},
				{name: "body", value: fmt.Sprintf("m%d", i)},
				{name: "timestamp_unix_nano", value: base},
			}
		}
		s := testStorage()
		requireUpstreamOrder(t, "projectedFieldsToDataBlock", s.projectedFieldsToDataBlock(in, base-1, base+1, nil))
	})

	t.Run("typed logs", func(t *testing.T) {
		s := testStorage()
		requireUpstreamOrder(t, "typedRowsToDataBlock(logs)", typedRowsToDataBlock(s, rows, 0, maxNs, logRowToFields))
	})

	t.Run("typed traces", func(t *testing.T) {
		s := testTracesStorage()
		trows := []schema.TraceRow{
			{TimestampUnixNano: 1, StartTimeUnixNano: 1, TraceID: "t2", SpanID: "s2", SpanName: "op", ServiceName: "b", DurationNs: 5},
			{TimestampUnixNano: 1, StartTimeUnixNano: 1, TraceID: "t1", SpanID: "s1", SpanName: "op", ServiceName: "a", DurationNs: 7},
		}
		requireUpstreamOrder(t, "typedRowsToDataBlock(traces)", typedRowsToDataBlock(s, trows, 0, maxNs, traceRowToFields))
	})

	t.Run("bridge logs", func(t *testing.T) {
		s := testStorage()
		requireUpstreamOrder(t, "logRowsToDataBlock", s.logRowsToDataBlock(tenantScope{all: true}, "test", rows))
	})

	t.Run("bridge traces", func(t *testing.T) {
		s := testTracesStorage()
		trows := []schema.TraceRow{
			{TimestampUnixNano: 1, TraceID: "t2", SpanID: "s2", SpanName: "op", ServiceName: "b"},
			{TimestampUnixNano: 1, TraceID: "t1", SpanID: "s1", SpanName: "op", ServiceName: "a"},
		}
		requireUpstreamOrder(t, "traceRowsToDataBlock", s.traceRowsToDataBlock(tenantScope{all: true}, "test", trows))
	})
}
