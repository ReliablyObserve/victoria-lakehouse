package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// #274 (traces): the numeric span columns are nullable. A field the row did not
// carry stays absent, an explicit 0 (status UNSET, kind UNSPECIFIED) is kept.
func TestMapFieldToTraceRow_NumericColumnsAbsentVsZero(t *testing.T) {
	var absent schema.TraceRow
	mapFieldToTraceRow(&absent, "parent", "frontend")
	if absent.StartTimeUnixNano != nil || absent.DurationNs != nil || absent.StatusCode != nil || absent.SpanKind != nil {
		t.Fatalf("a row that carried none of the numeric span fields must leave them absent: %+v", absent)
	}
	var zero schema.TraceRow
	for name, v := range map[string]string{"duration": "0", "start_time_unix_nano": "0", "status_code": "0", "kind": "0"} {
		mapFieldToTraceRow(&zero, name, v)
	}
	if zero.DurationNs == nil || *zero.DurationNs != 0 || zero.StartTimeUnixNano == nil || *zero.StartTimeUnixNano != 0 ||
		zero.StatusCode == nil || *zero.StatusCode != 0 || zero.SpanKind == nil || *zero.SpanKind != 0 {
		t.Fatalf("explicit zeros must stay present: %+v", zero)
	}
}
