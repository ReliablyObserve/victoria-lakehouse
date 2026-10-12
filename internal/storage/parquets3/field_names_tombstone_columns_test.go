package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A column carried only by tombstoned rows is not listed: the deleted rows are
// suppressed before their block's columns are observed, so they leave no trace
// in the stream's column set (hot storage rewrites its parts without them). The
// row is deleted in an object and in the insert peer's buffer; the columns of the
// surviving rows are credited with the surviving rows only.
func TestFieldNames_ColumnOfOnlyTombstonedRowsIsNotListed(t *testing.T) {
	s := fvcStorage(t,
		[]schema.LogRow{fnRow(1*time.Minute, "INFO", "t-del"), fvcRow(2*time.Minute, "INFO")},
		[]schema.LogRow{fvcRow(3*time.Minute, "INFO")},
	)
	s.bufferBridge = fvcPeer(t, []schema.LogRow{fnRow(20*time.Minute, "INFO", "t-del"), fvcRow(21*time.Minute, "INFO")})

	before, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if m := namesToHits(before); m["trace_id"] != 5 || m["_msg"] != 5 {
		t.Fatalf("fixture: trace_id/_msg hits before the delete = %d/%d, want 5/5: %v", m["trace_id"], m["_msg"], m)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		Tenants: []delete.TenantRef{{}}, ID: "ts-col", Query: `trace_id:="t-del"`,
		StartNs: fvcBase.UnixNano(), EndNs: fvcBase.Add(time.Hour).UnixNano(), Mode: "hide",
	})
	s.SetTombstoneStore(store)

	for _, query := range []string{"*", "level:=INFO"} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, query))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if _, listed := m["trace_id"]; listed {
			t.Errorf("%s: trace_id listed although only deleted rows carry it: %v", query, m)
		}
		if m["_msg"] != 3 {
			t.Errorf("%s: _msg hits = %d, want 3 (the surviving rows): %v", query, m["_msg"], m)
		}
	}
}

// A sparse column whose first surviving row is empty is still a column of the
// block: only a column none of the surviving rows carries is dropped. One object
// holds a deleted row with a trace id, a surviving row without one, and a
// surviving row with one: trace_id stays listed and is credited with both
// surviving rows of the stream.
func TestFieldNames_SparseColumnWhoseFirstSurvivingRowIsEmptyIsStillListed(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{
		fnRow(1*time.Minute, "INFO", "t-del"), fvcRow(2*time.Minute, "INFO"), fnRow(3*time.Minute, "INFO", "t-keep"),
	})
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
	if m := namesToHits(got); m["trace_id"] != 2 || m["_msg"] != 2 {
		t.Errorf("trace_id/_msg hits = %d/%d, want 2/2 (both surviving rows of the stream): %v", m["trace_id"], m["_msg"], m)
	}
}
