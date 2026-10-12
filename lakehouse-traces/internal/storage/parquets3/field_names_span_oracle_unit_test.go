package parquets3

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// hotSpanFieldNames are the names a hot VictoriaTraces v0.12.0 lists for an OTLP
// span with a trace id, a span id, a name, a service.name resource attribute and
// a start and end time (measured on a hot instance on 2026-10-12, the same span
// in 60 copies of three services: every name credited with all 60). It has no
// service.name: a filter on it matches no span, the field is
// resource_attr:service.name.
func hotSpanFieldNames() map[string]bool {
	return map[string]bool{
		"_msg": true, "_stream": true, "_stream_id": true, "_time": true,
		"dropped_attributes_count": true, "dropped_events_count": true, "dropped_links_count": true,
		"duration": true, "end_time_unix_nano": true, "flags": true, "kind": true, "name": true,
		"resource_attr:service.name": true, "span_id": true, "start_time_unix_nano": true,
		"status_code": true, "trace_id": true,
	}
}

func sortedNames(m map[string]uint64) []string {
	out := make([]string, 0, len(m))
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func TestFmtSpanNamesTruth_ListsOnlyNamesHotVictoriaTracesLists(t *testing.T) {
	rows := append(fmtSlotRows(0, 0), fmtSlotRows(1, 11)...)
	truth := fmtSpanNamesTruth(rows, "none", fmtWindow{lo: fmtBase.UnixNano(), hi: fmtBase.Add(3 * time.Hour).UnixNano()})
	if len(truth) == 0 {
		t.Fatal("no names")
	}
	for name := range truth {
		if !hotSpanFieldNames()[name] {
			t.Errorf("%s is not a name hot VictoriaTraces lists for such a span", name)
		}
	}
	for _, alias := range []string{"service.name", "http.method", "span.name", "duration_ns"} {
		if _, ok := truth[alias]; ok {
			t.Errorf("the oracle lists %s, which no hot VictoriaTraces span has", alias)
		}
	}
	// Every span of the generator carries all of them: each is credited with every span.
	for name, hits := range truth {
		if hits != uint64(len(rows)) {
			t.Errorf("%s hits = %d, want %d", name, hits, len(rows))
		}
	}
}

// The oracle takes the table for the fields of a span the generator sets; a
// field the generator starts to set has to be added to it first.
func TestFmtSpanNamesTruth_CoversEveryFieldTheGeneratorSets(t *testing.T) {
	set := make(map[string]bool)
	for _, hs := range [][2]int{{0, 0}, {0, 1}, {1, 0}, {1, 11}} {
		for _, r := range fmtSlotRows(hs[0], hs[1]) {
			rv := reflect.ValueOf(r)
			for i := 0; i < rv.NumField(); i++ {
				if !rv.Field(i).IsZero() {
					set[rv.Type().Field(i).Name] = true
				}
			}
		}
	}
	for name := range set {
		if !fmtGeneratedSpanFields()[name] {
			t.Errorf("the generator sets TraceRow.%s, which fmtSpanFields has no name for", name)
		}
	}
}

// The oracle credits a name with the matching spans of the blocks that list it:
// a block is a stream-day, it lists what any span of it has in the window (also a
// span the filter does not select), a block without a matching span lists nothing.
func TestFmtSpanNamesTruth_CreditsStreamDayBlocks(t *testing.T) {
	w := fmtWindow{lo: fmtBase.UnixNano(), hi: fmtBase.Add(30 * time.Hour).UnixNano()}
	span := func(stream, svc string, at time.Duration, traceID string) schema.TraceRow {
		ts := fmtBase.Add(at).UnixNano()
		return schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: ts, TraceID: traceID, SpanID: "s", SpanName: "n",
			ServiceName: svc, Stream: stream, StreamID: "id-" + stream,
		}
	}
	rows := []schema.TraceRow{
		span("A", "svc-a", 1*time.Minute, "t1"), span("A", "other", 2*time.Minute, ""), // trace_id only on a span the filter does not select
		span("B", "other", 3*time.Minute, "t3"), // a block without a matching span
		span("A", "svc-a", 25*time.Hour, ""),    // the next day: its own block, no trace_id
	}
	got := fmtSpanNamesTruth(rows, "svc", w)
	// Day 1 of A: 1 matching span, lists trace_id; day 2 of A: 1 matching span, no trace_id.
	if got["trace_id"] != 1 {
		t.Errorf("trace_id = %d, want 1 (the block of the first day lists it, the second day's does not)", got["trace_id"])
	}
	if got["span_id"] != 2 || got["_time"] != 2 {
		t.Errorf("span_id/_time = %d/%d, want 2/2 (a matching span of each day): %v", got["span_id"], got["_time"], got)
	}
	if got := fmtSpanNamesTruth(rows, "none", w); got["span_id"] != 4 || got["trace_id"] != 3 {
		t.Errorf("unfiltered span_id/trace_id = %d/%d, want 4/3: %v", got["span_id"], got["trace_id"], got)
	}
	narrow := fmtWindow{lo: fmtBase.Add(2*time.Minute + 1).UnixNano(), hi: fmtBase.Add(10 * time.Minute).UnixNano()}
	if got := sortedNames(fmtSpanNamesTruth(rows, "svc", narrow)); len(got) != 0 {
		t.Errorf("no matching span in the window, names listed: %v", got)
	}
}

// The traces harness's three layouts of the same spans (all flushed, all in a
// peer insert instance's buffer, split between them) give the oracle's answer,
// also with the filter as a hot instance spells it. The flushed layout is held
// against hot VictoriaTraces by TestParity_FieldNames_Layers; the peer layouts
// need a second Lakehouse instance that the parity stack does not have, so this
// is their exactness proof (#504).
func TestTraceFieldNames_PeerLayoutsEqualTheOracleAndTheFlushedLayout(t *testing.T) {
	first, second := fmtSlotRows(1, 0), fmtSlotRows(1, 1)
	all := append(append([]schema.TraceRow{}, first...), second...)
	layouts := map[string]*Storage{}
	layouts["flushed"] = fvcStorage(t, first, second)
	peerOnly := fvcStorage(t)
	peerOnly.bufferBridge = fmtPeer(t, all)
	layouts["peer only"] = peerOnly
	split := fvcStorage(t, first)
	split.bufferBridge = fmtPeer(t, second)
	layouts["flushed + peer"] = split

	w := fmtWindow{name: "hour", lo: fmtBase.Add(time.Hour).UnixNano(), hi: fmtBase.Add(2*time.Hour).UnixNano() - 1}
	for _, filter := range []string{"none", "svc"} {
		want := fmtSpanNamesTruth(all, filter, w)
		for name, s := range layouts {
			vals, err := s.GetFieldNames(context.Background(), nil, fmtQuery(t, filter, w))
			if err != nil {
				t.Fatal(err)
			}
			got := namesToHits(vals)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s, filter %s:\n got  %v\n want %v", name, filter, got, want)
			}
			for _, alias := range aliasNames() {
				if _, ok := got[alias]; ok {
					t.Errorf("%s, filter %s: lists %s", name, filter, alias)
				}
			}
		}
	}
}
