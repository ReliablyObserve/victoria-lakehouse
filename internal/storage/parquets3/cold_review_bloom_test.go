package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestColdReview_NotOrIfAroundBloomColumn (#289, logs): a NOT, an OR or a stats
// `if (...)` around a footer-bloom column (trace_id) must not prune row groups.
func TestColdReview_NotOrIfAroundBloomColumn(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.LogRow
	for i := 0; i < 40; i++ {
		rows = append(rows, schema.LogRow{TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), Body: "m", ServiceName: "svc",
			TraceID: fmt.Sprintf("trace-%d", i/10), Stream: `{service.name="svc"}`, StreamID: fmt.Sprintf("%048x", 3)})
	}
	res, err := writeLogsParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/b.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := reviewRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	bg := context.Background()
	for _, c := range []struct {
		q     string
		want  int
		stats bool
	}{
		{`NOT trace_id:="trace-1"`, 30, false},
		{`NOT trace_id:="trace-1" | stats count() n`, 30, true},
		{`trace_id:="trace-1" OR trace_id:="trace-2"`, 20, false},
		{`trace_id:="trace-1" OR trace_id:="trace-2" | stats count() n`, 20, true},
		{`NOT trace_id:="trace-1" | fields trace*`, 30, false},
		{`NOT trace_id:="trace-1" | sort by (_time) | limit 100`, 30, false},
		{`* | stats count() if (trace_id:="trace-1") n`, 10, true},
		{`* | stats count() if (NOT trace_id:="trace-1") n`, 30, true},
		{`trace_id:="trace-1"`, 10, false},
	} {
		out := run(bg, c.q)
		got := len(out)
		if c.stats {
			got = sumN(out)
		}
		if got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
}
