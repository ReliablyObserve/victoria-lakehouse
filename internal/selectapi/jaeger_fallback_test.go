package selectapi

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// field_values now lists rows without the field as an empty value: an answer
// holding only that is no answer, so the Jaeger fallback field is still tried.
func TestOnlyEmptyValue(t *testing.T) {
	for _, tc := range []struct {
		in   []logstorage.ValueWithHits
		want bool
	}{
		{nil, true},
		{[]logstorage.ValueWithHits{{Value: "", Hits: 4}}, true},
		{[]logstorage.ValueWithHits{{Value: "", Hits: 4}, {Value: "svc", Hits: 1}}, false},
		{[]logstorage.ValueWithHits{{Value: "svc", Hits: 1}}, false},
	} {
		if got := onlyEmptyValue(tc.in); got != tc.want {
			t.Errorf("onlyEmptyValue(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
