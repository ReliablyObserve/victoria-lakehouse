package storage

import (
	"math"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// The Lakehouse time domain starts at the Unix epoch: the epoch itself is in,
// one nanosecond before it is out.
func TestBeforeEpoch_Boundary(t *testing.T) {
	for _, tc := range []struct {
		ts   int64
		want bool
	}{
		{math.MinInt64, true}, {-1, true}, {0, false}, {1, false}, {math.MaxInt64, false},
	} {
		if got := BeforeEpoch(tc.ts); got != tc.want {
			t.Errorf("BeforeEpoch(%d) = %v, want %v", tc.ts, got, tc.want)
		}
	}
}

// NoteBeforeEpoch counts one refused row under the upstream-named reason.
func TestNoteBeforeEpoch_CountsTheRow(t *testing.T) {
	before := metrics.InsertRowsDropped.Get(ReasonTooSmallTimestamp)
	NoteBeforeEpoch(&logstorage.InsertRow{Timestamp: -1, Fields: []logstorage.Field{{Name: "_msg", Value: "old"}}})
	NoteBeforeEpoch(&logstorage.InsertRow{Timestamp: math.MinInt64})
	if got := metrics.InsertRowsDropped.Get(ReasonTooSmallTimestamp) - before; got != 2 {
		t.Fatalf("lakehouse_insert_rows_dropped_total{reason=%q} rose by %d, want 2", ReasonTooSmallTimestamp, got)
	}
}
