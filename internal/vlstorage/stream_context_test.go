package vlstorage

import (
	"fmt"
	"testing"
	"time"
)

// Probe for registry row vl.pipe.stream_context.basic: the row's exact query
// through the logs adapter. stream_context fetches each match's neighbours with
// a query of its own (a _stream_id filter over a time window), so on the cold
// tier it depends on the store honouring that filter.
func TestRow_VLPipeStreamContextBasic(t *testing.T) {
	base := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	var rows []map[string]string
	for i := 0; i < 9; i++ {
		level := "INFO"
		if i == 4 {
			level = "ERROR"
		}
		rows = append(rows, map[string]string{
			"_time":      base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			"_stream_id": "0000000000000000000000000000000000000000000000aa",
			"_msg":       fmt.Sprintf("line %d", i),
			"level":      level,
		})
	}
	s := &filterStore{rows: rows}
	got := runAdapter(t, s, `level:="ERROR" | stream_context before 3 after 3 | limit 100`)
	t.Logf("stream_context returned %d rows", len(got))
	for _, g := range got {
		t.Log(g)
	}
	if len(got) != 7 {
		t.Fatalf("want 1 match + 3 before + 3 after = 7 rows, got %d", len(got))
	}
}
