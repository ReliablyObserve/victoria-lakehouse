package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A column carried only by tombstoned spans is not listed (see the logs twin).
func TestTraceFieldNames_ColumnOfOnlyTombstonedSpansIsNotListed(t *testing.T) {
	del := func(at time.Duration) schema.TraceRow {
		r := fnSpan(at, "GET", "GET") // span_attr:custom.attr
		r.TraceID = "t-del"
		return r
	}
	plain := func(at time.Duration) schema.TraceRow {
		r := fvcSpan(at, "GET")
		r.Stream, r.StreamID, r.ServiceName = del(0).Stream, del(0).StreamID, del(0).ServiceName
		return r
	}
	s := fvcStorage(t,
		[]schema.TraceRow{del(1 * time.Minute), plain(2 * time.Minute)},
		[]schema.TraceRow{plain(3 * time.Minute)},
	)
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{del(20 * time.Minute), plain(21 * time.Minute)})

	before, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if m := namesToHits(before); m["span_attr:custom.attr"] != 5 || m["trace_id"] != 5 {
		t.Fatalf("fixture: span_attr:custom.attr/trace_id hits before the delete = %d/%d, want 5/5: %v", m["span_attr:custom.attr"], m["trace_id"], m)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		Tenants: []delete.TenantRef{{}}, ID: "ts-col", Query: `trace_id:="t-del"`,
		StartNs: fvcBase.UnixNano(), EndNs: fvcBase.Add(time.Hour).UnixNano(), Mode: "hide",
	})
	s.SetTombstoneStore(store)

	for _, query := range []string{"*", "name:=GET"} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, query))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if _, listed := m["span_attr:custom.attr"]; listed {
			t.Errorf("%s: span_attr:custom.attr listed although only deleted spans carry it: %v", query, m)
		}
		if m["trace_id"] != 3 {
			t.Errorf("%s: trace_id hits = %d, want 3 (the surviving spans): %v", query, m["trace_id"], m)
		}
	}
}

// A sparse column whose first surviving span is empty is still a column of the
// block (see the logs twin): span_attr:custom.attr stays listed and is credited
// with both surviving spans of the stream.
func TestTraceFieldNames_SparseColumnWhoseFirstSurvivingSpanIsEmptyIsStillListed(t *testing.T) {
	del := fnSpan(1*time.Minute, "GET", "x")
	del.TraceID = "t-del"
	keep := fnSpan(3*time.Minute, "GET", "y")
	s := fvcStorage(t, []schema.TraceRow{del, fvcSpan(2*time.Minute, "GET"), keep})
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		Tenants: []delete.TenantRef{{}}, ID: "ts-sparse", Query: `trace_id:="t-del"`,
		StartNs: fvcBase.UnixNano(), EndNs: fvcBase.Add(time.Hour).UnixNano(), Mode: "hide",
	})
	s.SetTombstoneStore(store)
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if m := namesToHits(got); m["span_attr:custom.attr"] != 2 || m["trace_id"] != 2 {
		t.Errorf("span_attr:custom.attr/trace_id hits = %d/%d, want 2/2 (both surviving spans of the stream): %v", m["span_attr:custom.attr"], m["trace_id"], m)
	}
}
