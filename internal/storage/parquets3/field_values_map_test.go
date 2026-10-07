package parquets3

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// field_values over MAP attributes and filters on them must read the attribute's own leaf.

func TestFieldValuesMapAttributes_Logs(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	at := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	var rows []schema.LogRow
	for i, lvl := range []string{"INFO", "ERROR", "WARN", "INFO"} {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "row", ServiceName: "svc", SeverityText: lvl,
			ResourceAttributes: map[string]string{"rk": "r" + lvl},
			LogAttributes:      map[string]string{"lk": "l" + lvl},
		})
	}
	bw.stageLogRows(rows)
	bw.flushStagedNow()
	lo, hi := at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano()
	for _, tc := range []struct {
		q, field string
		want     []string
	}{
		{"*", "lk", []string{"lERROR", "lINFO", "lWARN"}},
		{"*", "rk", []string{"rERROR", "rINFO", "rWARN"}},
		{`lk:=lERROR`, "level", []string{"ERROR"}},
		{`rk:=rINFO`, "level", []string{"INFO"}},
		{`level:=ERROR`, "lk", []string{"lERROR"}},
	} {
		q := mustParseQueryWithTime(t, tc.q, lo, hi)
		got, err := s.GetFieldValues(context.Background(), nil, q, tc.field, 0)
		if err != nil {
			t.Fatalf("%s/%s: %v", tc.q, tc.field, err)
		}
		var out []string
		for _, v := range got {
			out = append(out, v.Value)
		}
		sort.Strings(out)
		if !equalStrings(out, tc.want) {
			t.Errorf("field_values(%s) q=%s = %v, want %v", tc.field, tc.q, out, tc.want)
		}
	}
}
