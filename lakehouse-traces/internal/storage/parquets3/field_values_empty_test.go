package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Hot VT: stream_field_values comes from stream tags; a span with an empty
// name (or no service) has no such tag and never yields an empty value.
// VT's Jaeger services/operations read exactly this endpoint.
func TestFieldValuesEmpty_Traces_StreamFieldValues_NoEmptyBucket(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.TraceRow
	for i, n := range []string{"op", "op", "", ""} {
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			StartTimeUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID:           fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("%016x", i+1),
			SpanName: n, ServiceName: "svc",
		})
	}
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	for _, q := range []string{"*", `_stream:{resource_attr:service.name="svc"}`} {
		got, err := s.GetStreamFieldValues(context.Background(), nil, mustParseQueryWithTime(t, q, start, end), "name", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range got {
			if v.Value == "" {
				t.Errorf("stream_field_values(name) q=%s lists the empty value (hits %d): %v", q, v.Hits, got)
			}
		}
	}
}
