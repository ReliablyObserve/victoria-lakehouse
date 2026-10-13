package vlstorage

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

func idxBlockOf(cols map[string][]string) *logstorage.DataBlock {
	var bc []logstorage.BlockColumn
	for name, vs := range cols {
		bc = append(bc, logstorage.BlockColumn{Name: name, Values: vs})
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(bc)
	return db
}

// The index stream rows of a block come out as table rows, every other row is left to
// the span converter, and a row that cannot be stored is counted, not stored.
func TestIdxRowsOf_PicksTheIndexStreamRowsOfABlock(t *testing.T) {
	text, _ := IdxStream(7)
	start := time.Date(2026, 10, 13, 10, 0, 0, 123456789, time.UTC)
	id := "0123456789abcdef0123456789abcdef"
	db := idxBlockOf(map[string][]string{
		"_stream":                             {text, `{resource_attr:service.name="api"}`, text, text, text},
		"_time":                               {start.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano), "", start.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano)},
		otelpb.TraceIDIndexFieldName:          {id, "a-span", id, "", id},
		otelpb.TraceIDIndexStartTimeFieldName: {strconv.FormatInt(start.UnixNano(), 10), "", "100", "100", "not-a-number"},
		otelpb.TraceIDIndexEndTimeFieldName:   {strconv.FormatInt(start.UnixNano()+500, 10), "", "250", "250", "250"},
		otelpb.TraceIDIndexDuration:           {"500", "", "", "150", "150"},
	})
	before := idxDropped(kindIdxMalformed)
	rows := IdxRowsOf(db)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want the 2 well-formed index rows: %+v", len(rows), rows)
	}
	want0 := tracesidx.Row{Time: start.UnixNano(), Stream: 7, TraceID: id, Start: start.UnixNano(), End: start.UnixNano() + 500, Duration: 500}
	if rows[0] != want0 {
		t.Errorf("row 0 = %+v, want %+v", rows[0], want0)
	}
	// no _time text: the start time stands in; no duration: end - start
	if rows[1].Time != 100 || rows[1].Duration != 150 || rows[1].Start != 100 || rows[1].End != 250 {
		t.Errorf("row 1 = %+v, want time 100, duration 150", rows[1])
	}
	if got := idxDropped(kindIdxMalformed) - before; got != 2 {
		t.Errorf("malformed rows counted = %d, want 2 (no id, unparsable start)", got)
	}
}

func TestIdxRowsOf_NothingToReturn(t *testing.T) {
	if IdxRowsOf(nil) != nil || IdxRowsOf(&logstorage.DataBlock{}) != nil {
		t.Error("a nil or empty block must give no rows")
	}
	if got := IdxRowsOf(idxBlockOf(map[string][]string{"_time": {"x"}, "_msg": {"y"}})); got != nil {
		t.Errorf("a block with no _stream/trace_id_idx columns gave %v", got)
	}
}

// The stream number is read from the stream text; with any other text it is computed
// from the id, as upstream does.
func TestStreamValueOf(t *testing.T) {
	id := "0123456789abcdef0123456789abcdef"
	text, _ := IdxStream(1023)
	if got := streamValueOf(text, id); got != 1023 {
		t.Errorf("from text: %d, want 1023", got)
	}
	for _, bad := range []string{"", `{}`, `{` + otelpb.TraceIDIndexStreamName + `=""}`, `{` + otelpb.TraceIDIndexStreamName + `="-3"}`, `{` + otelpb.TraceIDIndexStreamName + `="x"}`} {
		if got := streamValueOf(bad, id); got != tracesidx.StreamOf(id) {
			t.Errorf("text %q: %d, want the computed %d", bad, got, tracesidx.StreamOf(id))
		}
	}
}

// IdxStream is upstream's own StreamTags rendering and StreamIDFor is the stream-id
// rule the cold tier already uses.
func TestIdxStreamAndStreamIDFor(t *testing.T) {
	text, canonical := IdxStream(42)
	if text != `{`+otelpb.TraceIDIndexStreamName+`="42"}` {
		t.Errorf("text = %q", text)
	}
	if !strings.Contains(canonical, otelpb.TraceIDIndexStreamName) || !strings.Contains(canonical, "42") {
		t.Errorf("canonical = %q", canonical)
	}
	tenant := logstorage.TenantID{AccountID: 3, ProjectID: 4}
	got := StreamIDFor(tenant, canonical)
	if len(got) != 48 || got != computeStreamID(tenant, canonical) {
		t.Errorf("stream id %q", got)
	}
	if !strings.HasPrefix(got, "0000000300000004") {
		t.Errorf("stream id %q lacks the tenant prefix", got)
	}
	if StreamIDFor(tenant, "") != "" {
		t.Error("an empty stream must give an empty id")
	}
}
