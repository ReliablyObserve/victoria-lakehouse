package parquets3

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

// The flush writer keeps DELTA_BINARY_PACKED on the optional
// start_time_unix_nano column (parquet-go rejects the delta struct tag on a
// pointer field, so schema.NewTraceWriter attaches it to the schema node).
func TestWriteTracesParquet_StartTimeKeepsDeltaEncoding(t *testing.T) {
	rows := make([]schema.TraceRow, 500)
	for i := range rows {
		ts := int64(1700000000000000000) + int64(i)*1000
		rows[i] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts), TraceID: fmt.Sprintf("%032x", i/4), SpanID: fmt.Sprintf("%016x", i), ServiceName: "svc"}
	}
	res, err := writeTracesParquet(rows, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	encs := testutil.ColumnEncodings(t, res.Data)
	if !testutil.HasEncoding(encs, "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatalf("flushed start_time_unix_nano encodings: %v", encs["start_time_unix_nano"])
	}
}
