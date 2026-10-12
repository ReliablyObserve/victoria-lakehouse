package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A stream's spans of two UTC days are two blocks, as on hot storage (a block
// never spans two day partitions). An insert peer's unflushed spans can cross
// midnight: its span before midnight belongs to the day the objects are on, its
// span after midnight, the only one with the attribute, to the next day, and the
// attribute must not be credited to the objects' day (hot VictoriaTraces credits
// the later day's block alone: 1 span).
func TestTraceFieldNames_APeerStreamCrossingUTCMidnightIsOneBlockPerDay(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{fvcSpan(1*time.Minute, "GET"), fvcSpan(2*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{
		fvcSpan(13*time.Hour+59*time.Minute, "GET"),           // 23:59 UTC, the objects' day
		fnSpan(14*time.Hour+1*time.Minute, "GET", "next-day"), // 00:01 UTC, the next day
	})
	window := func(query string) map[string]uint64 {
		got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, query, fvcBase.UnixNano(), fvcBase.Add(24*time.Hour).UnixNano()))
		if err != nil {
			t.Fatal(err)
		}
		return namesToHits(got)
	}
	if m := window("*"); m["trace_id"] != 4 || m["span_attr:custom.attr"] != 1 {
		t.Errorf("* trace_id/span_attr:custom.attr hits = %d/%d, want 4/1 (the attribute only in the next day's block): %v", m["trace_id"], m["span_attr:custom.attr"], m)
	}
	// The spans of the objects' day: no attribute in their block, so none listed.
	if m := window(`-"span_attr:custom.attr":*`); m["trace_id"] != 3 || m["span_attr:custom.attr"] != 0 {
		t.Errorf("-span_attr:custom.attr:* trace_id/span_attr:custom.attr hits = %d/%d, want 3/0: %v", m["trace_id"], m["span_attr:custom.attr"], m)
	}
}

// The spans an insert peer holds and the spans of the same stream in objects
// are one block, as on hot storage once its parts are merged: an attribute only
// the peer's spans carry credits every span of the stream-day, wherever it is held.
func TestTraceFieldNames_PeerBufferAndObjectsOfOneStreamAreOneBlock(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{fvcSpan(1*time.Minute, "GET"), fvcSpan(2*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{fnSpan(20*time.Minute, "GET", "v"), fvcSpan(21*time.Minute, "GET")})
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["trace_id"] != 4 || m["span_attr:custom.attr"] != 4 {
		t.Errorf("trace_id/span_attr:custom.attr hits = %d/%d, want 4/4: %v", m["trace_id"], m["span_attr:custom.attr"], m)
	}
	// A filter that selects the spans without the attribute still credits them
	// with the column the peer's other span brings to the stream-day.
	got, err = s.GetFieldNames(context.Background(), nil, fnWindow(t, `-"span_attr:custom.attr":*`))
	if err != nil {
		t.Fatal(err)
	}
	if m = namesToHits(got); m["trace_id"] != 3 || m["span_attr:custom.attr"] != 3 {
		t.Errorf("filtered trace_id/span_attr:custom.attr hits = %d/%d, want 3/3: %v", m["trace_id"], m["span_attr:custom.attr"], m)
	}
}
