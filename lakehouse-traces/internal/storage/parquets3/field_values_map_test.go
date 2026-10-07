package parquets3

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// field_values over MAP attributes and filters on them must read the attribute's own leaf.
func TestFieldValuesMapAttributes_Traces(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.TraceRow
	for i, k := range []string{"A", "B", "C", "A"} {
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			StartTimeUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID:           fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("%016x", i+1),
			SpanName: "op-" + k, ServiceName: "svc",
			SpanAttributes:     map[string]string{"sa": "s" + k},
			ResourceAttributes: map[string]string{"ra": "r" + k},
			ScopeAttributes:    map[string]string{"sc": "c" + k},
		})
	}
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	for _, tc := range []struct {
		q, field string
		want     []string
	}{
		{"*", "span_attr:sa", []string{"sA", "sB", "sC"}},
		{"*", "resource_attr:ra", []string{"rA", "rB", "rC"}},
		{"*", "scope_attr:sc", []string{"cA", "cB", "cC"}},
		{"`span_attr:sa`:=sB", "name", []string{"op-B"}},
		{`name:=op-A`, "span_attr:sa", []string{"sA"}},
	} {
		got, err := s.GetFieldValues(context.Background(), nil, mustParseQueryWithTime(t, tc.q, start, end), tc.field, 0)
		if err != nil {
			t.Fatalf("%s/%s: %v", tc.q, tc.field, err)
		}
		var out []string
		for _, v := range got {
			out = append(out, v.Value)
		}
		sort.Strings(out)
		if fmt.Sprint(out) != fmt.Sprint(tc.want) {
			t.Errorf("field_values(%s) q=%s = %v, want %v", tc.field, tc.q, out, tc.want)
		}
	}
}
