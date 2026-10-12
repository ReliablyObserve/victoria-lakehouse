package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A stream's rows of two UTC days are two blocks, as on hot storage (a block
// never spans two day partitions). An insert peer's unflushed rows can cross
// midnight: its row before midnight belongs to the day the objects are on, its
// row after midnight, the only one with a trace id, to the next day, and the
// trace id must not be credited to the objects' day (hot VictoriaLogs credits
// the later day's block alone: 1 row).
func TestFieldNames_APeerStreamCrossingUTCMidnightIsOneBlockPerDay(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{fvcRow(1*time.Minute, "INFO"), fvcRow(2*time.Minute, "INFO")})
	s.bufferBridge = fvcPeer(t, []schema.LogRow{
		fvcRow(13*time.Hour+59*time.Minute, "INFO"),         // 23:59 UTC, the objects' day
		fnRow(14*time.Hour+1*time.Minute, "INFO", "t-next"), // 00:01 UTC, the next day
	})
	window := func(query string) map[string]uint64 {
		got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, query, fvcBase.UnixNano(), fvcBase.Add(24*time.Hour).UnixNano()))
		if err != nil {
			t.Fatal(err)
		}
		return namesToHits(got)
	}
	if m := window("*"); m["_msg"] != 4 || m["trace_id"] != 1 {
		t.Errorf("* _msg/trace_id hits = %d/%d, want 4/1 (trace_id only in the next day's block): %v", m["_msg"], m["trace_id"], m)
	}
	// The rows of the objects' day: no trace id in their block, so none listed.
	if m := window("-trace_id:*"); m["_msg"] != 3 || m["trace_id"] != 0 {
		t.Errorf("-trace_id:* _msg/trace_id hits = %d/%d, want 3/0 (trace_id is not listed): %v", m["_msg"], m["trace_id"], m)
	}
}
