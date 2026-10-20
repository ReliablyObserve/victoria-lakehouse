package parquets3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// gateSurfaces is every storage read the tombstone gate guards. Each one shows
// rows or values of the cold tier or the buffer bridge, so each must answer 503
// instead of showing something a delete may have hidden. Twin of
// lakehouse-traces/internal/storage/parquets3/tombstone_gate_test.go, which also
// covers the trace-by-ID index lookup.
func gateSurfaces(f *tsFixture, q *logstorage.Query) map[string]func(ctx context.Context, ids []logstorage.TenantID) error {
	return map[string]func(context.Context, []logstorage.TenantID) error{
		"RunQuery": func(ctx context.Context, ids []logstorage.TenantID) error {
			return f.s.RunQuery(ctx, ids, q, func(uint, *logstorage.DataBlock) {})
		},
		"GetFieldNames": func(ctx context.Context, ids []logstorage.TenantID) error {
			_, err := f.s.GetFieldNames(ctx, ids, q)
			return err
		},
		"GetFieldValues": func(ctx context.Context, ids []logstorage.TenantID) error {
			_, err := f.s.GetFieldValues(ctx, ids, q, "service.name", 0)
			return err
		},
		"GetStreamFieldNames": func(ctx context.Context, ids []logstorage.TenantID) error {
			_, err := f.s.GetStreamFieldNames(ctx, ids, q)
			return err
		},
		"GetStreamFieldValues": func(ctx context.Context, ids []logstorage.TenantID) error {
			_, err := f.s.GetStreamFieldValues(ctx, ids, q, "service.name", 0)
			return err
		},
		"GetStreams": func(ctx context.Context, ids []logstorage.TenantID) error {
			_, err := f.s.GetStreams(ctx, ids, q, 0)
			return err
		},
		"GetStreamIDs": func(ctx context.Context, ids []logstorage.TenantID) error {
			_, err := f.s.GetStreamIDs(ctx, ids, q, 0)
			return err
		},
	}
}

// attachGatedView attaches a bucket-backed view with a short poll interval to the
// fixture's storage (the gate is on) and runs its first sync.
func attachGatedView(t *testing.T, f *tsFixture, gateOff bool) (*deletetest.Bucket, *deletetest.Pod) {
	t.Helper()
	b := deletetest.NewBucket()
	pod := b.Pod("gate")
	store := delete.NewTombstoneStore()
	if err := store.Attach(delete.Options{
		Backend: pod, Prefix: "logs/", Interval: 40 * time.Millisecond, Now: pod.Now, GateOff: gateOff,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncNow(context.Background(), true); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	f.s.SetTombstoneStore(store)
	return b, pod
}

func requireStatus(t *testing.T, name string, err error, want int) {
	t.Helper()
	var esc *httpserver.ErrorWithStatusCode
	switch {
	case want == 0 && err != nil:
		t.Errorf("%s: %v, want it served", name, err)
	case want != 0 && err == nil:
		t.Errorf("%s was served, want a %d: a stale view must not show rows a delete may have hidden", name, want)
	case want != 0 && (!errors.As(err, &esc) || esc.StatusCode != want):
		t.Errorf("%s: %v, want a %d", name, err, want)
	}
}

// A view that cannot be refreshed fails EVERY gated surface closed (503) and a
// healthy view serves them all: no enumeration (field names and values, streams,
// stream ids) is left showing a deleted value.
func TestTombstoneGate_StaleViewFailsEverySurfaceClosed(t *testing.T) {
	f := newTenantScopeFixture(t)
	_, pod := attachGatedView(t, f, false)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	surfaces := gateSurfaces(f, q)

	for name, call := range surfaces {
		requireStatus(t, name+" (fresh view)", call(f.ctx(true), tenant00), 0)
	}

	pod.FailNext("list", 1_000_000)
	time.Sleep(250 * time.Millisecond) // more than three intervals without a sync
	if err := f.s.tombstones.SyncNow(context.Background(), false); err == nil {
		t.Fatal("fixture: the sync should fail")
	}
	for name, call := range surfaces {
		requireStatus(t, name+" (stale view, one tenant)", call(f.ctx(false), tenant1001), http.StatusServiceUnavailable)
		requireStatus(t, name+" (stale view, global read)", call(f.ctx(true), tenant00), http.StatusServiceUnavailable)
	}

	pod.FailNext("list", 0)
	if err := f.s.tombstones.SyncNow(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	for name, call := range surfaces {
		requireStatus(t, name+" (recovered)", call(f.ctx(true), tenant00), 0)
	}
}

// With delete.enabled false nothing is gated: even a view that cannot be refreshed
// serves every surface, and the tombstones it already holds are still enforced.
func TestTombstoneGate_DeleteDisabledNeverRefuses(t *testing.T) {
	f := newTenantScopeFixture(t)
	_, pod := attachGatedView(t, f, true)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	pod.FailNext("list", 1_000_000)
	time.Sleep(250 * time.Millisecond)
	_ = f.s.tombstones.SyncNow(context.Background(), false)
	for name, call := range gateSurfaces(f, q) {
		requireStatus(t, name+" (delete disabled, stale view)", call(f.ctx(true), tenant00), 0)
	}
}

// A definition that cannot be decoded closes the gate for the tenants its body
// names and for no others; one that names none closes it for every tenant.
func TestTombstoneGate_UndecodableDefinitionClosesOnlyItsTenants(t *testing.T) {
	f := newTenantScopeFixture(t)
	b, _ := attachGatedView(t, f, false)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	surfaces := gateSurfaces(f, q)

	key := func(id string) string {
		return fmt.Sprintf("%st%016x-%s.json", delete.TombstonePrefix("logs/"), time.Now().UnixNano(), id)
	}
	b.Put(key("bad"), []byte(`{"v":99,"tenants":[{"a":1001,"p":0}],"filter":"x"}`)) // a version this pod cannot read; names 1001
	if err := f.s.tombstones.SyncNow(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	for name, call := range surfaces {
		requireStatus(t, name+" (tenant named by the bad definition)", call(f.ctx(false), tenant1001), http.StatusServiceUnavailable)
		requireStatus(t, name+" (another tenant)", call(f.ctx(false), tenant00), 0)
		// A global-read request reads every tenant's rows, so a definition naming
		// any tenant closes it, whatever tenant its headers name.
		requireStatus(t, name+" (global read, definition names another tenant)", call(f.ctx(true), tenant00), http.StatusServiceUnavailable)
	}

	b.Put(key("nameless"), []byte(`not json at all`))
	if err := f.s.tombstones.SyncNow(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	for name, call := range surfaces {
		requireStatus(t, name+" (body that is not JSON names no tenant)", call(f.ctx(false), tenant00), http.StatusServiceUnavailable)
	}
}

// A delete hides a matching row as a whole: no query returns it, and none returns
// a husk of it holding only its _time and _stream fields (what upstream's delete
// can leave behind, VictoriaLogs issue 825). Registry row
// lh.delete.no_empty_row_residue. The rows of the tenant that was not deleted
// still carry their fields.
func TestTombstoneHidesWholeRows_NoEmptyResidue(t *testing.T) {
	f := newTenantScopeFixture(t)
	withTombstones(f, scopedTombstone(f, "del-1001", delete.TenantRef{AccountID: 1001}))
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

	husks := func(ids []logstorage.TenantID) (rows, empty int) {
		t.Helper()
		var mu sync.Mutex
		err := f.s.RunQuery(f.ctx(false), ids, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			cols := db.GetColumns(false)
			for r := 0; r < db.RowsCount(); r++ {
				rows++
				named := 0
				for _, c := range cols {
					if c.Name == "_time" || c.Name == "_stream" || c.Name == "_stream_id" {
						continue
					}
					if r < len(c.Values) && c.Values[r] != "" {
						named++
					}
				}
				if named == 0 {
					empty++
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		return rows, empty
	}
	if rows, empty := husks(tenant1001); rows != 0 || empty != 0 {
		t.Errorf("the deleted tenant gets %d rows, %d of them without fields: a delete hides whole rows", rows, empty)
	}
	if rows, empty := husks(tenant00); rows == 0 || empty != 0 {
		t.Errorf("an untouched tenant gets %d rows, %d of them without fields, want its rows whole", rows, empty)
	}
}
