package parquets3

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// ---------------------------------------------------------------------------
// Tenant-scoped tombstones on the read path: invariant + regression suite.
//
// THE invariant: a tombstone scoped to tenant A never hides, and never costs a
// fast path to, tenant B. Every query class that applies tombstones is checked
// with the tenant-scope fixture (three tenants plus the legacy untenanted
// object in one partition and window), for single-tenant, multi-tenant and
// global-read requests. A legacy record without tenants keeps hiding rows of
// every tenant.
//
// Twin of lakehouse-traces/internal/storage/parquets3/tombstone_scope_test.go.
// ---------------------------------------------------------------------------

// scopedTombstone hides every row of the fixture window for the given tenants.
func scopedTombstone(f *tsFixture, id string, tenants ...delete.TenantRef) delete.Tombstone {
	return delete.Tombstone{
		ID: id, Query: "*", StartNs: f.startNs, EndNs: f.endNs, Mode: "hide",
		CreatedAt: time.Now(), Tenants: tenants,
	}
}

func withTombstones(f *tsFixture, tss ...delete.Tombstone) {
	store := delete.NewTombstoneStore()
	for _, ts := range tss {
		store.Add(ts)
	}
	f.s.SetTombstoneStore(store)
}

var (
	tenant1001 = []logstorage.TenantID{{AccountID: 1001}}
	tenant2002 = []logstorage.TenantID{{AccountID: 2002, ProjectID: 7}}
	tenant00   = []logstorage.TenantID{{}}
)

func TestTombstoneScope_ScanHidesOnlyTheScopedTenant(t *testing.T) {
	for _, layout := range tsLayouts() {
		t.Run(string(layout), func(t *testing.T) {
			f := newTenantScopeFixtureLayout(t, layout)
			withTombstones(f, scopedTombstone(f, "del-1001", delete.TenantRef{AccountID: 1001}))
			ctx := context.Background()

			if rows, _ := f.runQuery(ctx, tenant1001, "*"); rows != 0 {
				t.Errorf("tenant 1001:0 still sees %d rows its own delete hides", rows)
			}
			if rows, svcs := f.runQuery(ctx, tenant00, "*"); rows != tsRowsT0 || svcs["svc-tenant-0-0"] != tsRowsT0 {
				t.Errorf("tenant 0:0 lost rows to tenant 1001's delete: rows=%d svcs=%v, want %d", rows, svcs, tsRowsT0)
			}
			if rows, _ := f.runQuery(ctx, tenant2002, "*"); rows != tsRowsT2002 {
				t.Errorf("tenant 2002:7 lost rows to tenant 1001's delete: rows=%d, want %d", rows, tsRowsT2002)
			}

			// Multi-tenant request (VL's internal select protocol): each object is
			// filtered by its own tenant's tombstones.
			rows, svcs := f.runQuery(ctx, []logstorage.TenantID{{AccountID: 1001}, {AccountID: 2002, ProjectID: 7}}, "*")
			if rows != tsRowsT2002 || svcs["svc-tenant-1001-0"] != 0 || svcs["svc-tenant-2002-7"] != tsRowsT2002 {
				t.Errorf("multi-tenant request: rows=%d svcs=%v, want only 2002:7's %d rows", rows, svcs, tsRowsT2002)
			}

			// Validated global read: every tenant except the deleted rows.
			rows, svcs = f.runQuery(f.ctx(true), tenant00, "*")
			if want := tsRowsT0 + tsRowsT2002; rows != want || svcs["svc-tenant-1001-0"] != 0 {
				t.Errorf("global read: rows=%d svcs=%v, want %d with nothing of 1001:0", rows, svcs, want)
			}
		})
	}
}

func TestTombstoneScope_DefaultTenantCoversLegacyObjects(t *testing.T) {
	f := newTenantScopeFixture(t)
	f.addLegacyObject()
	withTombstones(f, scopedTombstone(f, "del-00", delete.TenantRef{}))
	ctx := context.Background()

	// Legacy untenanted objects are the default tenant's data on the read path,
	// so a delete by 0:0 covers them too.
	if rows, _ := f.runQuery(ctx, tenant00, "*"); rows != 0 {
		t.Errorf("tenant 0:0 still sees %d rows (0/0 object + legacy object) after its delete", rows)
	}
	if rows, _ := f.runQuery(ctx, tenant1001, "*"); rows != tsRowsT1001 {
		t.Errorf("tenant 1001:0 lost rows to tenant 0:0's delete: rows=%d, want %d", rows, tsRowsT1001)
	}
	rows, svcs := f.runQuery(f.ctx(true), tenant00, "*")
	if want := tsRowsT1001 + tsRowsT2002; rows != want || svcs["svc-legacy"] != 0 || svcs["svc-tenant-0-0"] != 0 {
		t.Errorf("global read: rows=%d svcs=%v, want %d with neither the 0/0 nor the legacy object", rows, svcs, want)
	}
}

func TestTombstoneScope_LegacyUnscopedRecordStaysInstanceWide(t *testing.T) {
	f := newTenantScopeFixture(t)
	withTombstones(f, scopedTombstone(f, "legacy-wide")) // no tenants: a record from before tenant scope
	for _, tc := range tsCases() {
		if rows, _ := f.runQuery(f.ctx(tc.globalRead), tc.tenantIDs, "*"); rows != 0 {
			t.Errorf("%s: an instance-wide legacy tombstone must keep hiding every tenant's rows, got %d", tc.name, rows)
		}
	}
}

// The metadata fast paths are given up while a tombstone overlaps the window.
// A tombstone of ANOTHER tenant must not cost a request its fast path — and,
// more importantly, the answer the fast path gives must still be exact.
func TestTombstoneScope_FastPathsForOtherTenantsStayExact(t *testing.T) {
	f := newTenantScopeFixture(t)
	withTombstones(f, scopedTombstone(f, "del-1001", delete.TenantRef{AccountID: 1001}))

	if got := f.s.scopeTombstones(resolveTenantScope(tenant00), f.startNs, f.endNs); got != nil {
		t.Fatalf("tenant 0:0 must see no tombstone of tenant 1001:0, got %+v", got)
	}
	if got := f.s.scopeTombstones(resolveTenantScope(tenant1001), f.startNs, f.endNs); len(got) != 1 {
		t.Fatalf("tenant 1001:0 must see its own tombstone, got %+v", got)
	}

	tsOnly := storage.WithTimestampOnlyHint(context.Background())
	if rows, _ := f.runQuery(tsOnly, tenant00, "*"); rows != tsRowsT0 {
		t.Errorf("timestamp-only fast path, tenant 0:0: want %d rows, got %d", tsRowsT0, rows)
	}
	if rows, _ := f.runQuery(tsOnly, tenant1001, "*"); rows != 0 {
		t.Errorf("timestamp-only path, tenant 1001:0: its deleted rows were counted (%d)", rows)
	}
	_, svcs := f.runQuery(context.Background(), tenant00, "* | stats by (service.name) count()")
	if svcs["svc-tenant-0-0"] == 0 {
		t.Errorf("count pushdown, tenant 0:0: want svc-tenant-0-0 counted, got %v", svcs)
	}
	_, svcs = f.runQuery(context.Background(), tenant1001, "* | stats by (service.name) count()")
	if svcs["svc-tenant-1001-0"] != 0 {
		t.Errorf("count path, tenant 1001:0: deleted rows were counted: %v", svcs)
	}
}

func TestTombstoneScope_FieldEnumerationHidesOnlyTheScopedTenant(t *testing.T) {
	f := newTenantScopeFixture(t)
	withTombstones(f, scopedTombstone(f, "del-1001", delete.TenantRef{AccountID: 1001}))
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	ctx := context.Background()

	values := func(ctx context.Context, ids []logstorage.TenantID) map[string]int {
		t.Helper()
		vals, err := f.s.GetFieldValues(ctx, ids, q, "service.name", 0)
		if err != nil {
			t.Fatalf("GetFieldValues: %v", err)
		}
		return valuesToCounts(vals)
	}
	if got := values(ctx, tenant1001); got["svc-tenant-1001-0"] != 0 {
		t.Errorf("tenant 1001:0 still enumerates its deleted value: %v", got)
	}
	if got := values(ctx, tenant00); got["svc-tenant-0-0"] == 0 {
		t.Errorf("tenant 0:0 lost its value to tenant 1001's delete: %v", got)
	}
	got := values(f.ctx(true), tenant00)
	if got["svc-tenant-1001-0"] != 0 || got["svc-tenant-0-0"] == 0 || got["svc-tenant-2002-7"] == 0 {
		t.Errorf("global read field_values: want every tenant's value except 1001:0's, got %v", got)
	}
	got = values(ctx, []logstorage.TenantID{{AccountID: 1001}, {}})
	if got["svc-tenant-1001-0"] != 0 || got["svc-tenant-0-0"] == 0 {
		t.Errorf("multi-tenant field_values: want 0:0's value only, got %v", got)
	}

	// streams / stream_ids go through the same per-object selection.
	if _, err := f.s.GetStreams(f.ctx(true), tenant00, q, 0); err != nil {
		t.Fatalf("GetStreams: %v", err)
	}
	if _, err := f.s.GetStreamIDs(ctx, tenant1001, q, 0); err != nil {
		t.Fatalf("GetStreamIDs: %v", err)
	}
}

// The pure-buffer path and the raw-row buffer path attribute buffered rows by
// the tenant the buffer stores them under.
func TestTombstoneScope_PureBufferWindow(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer bs.Close()

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 4; i++ {
		lr.MustAdd(logstorage.TenantID{}, now, []logstorage.Field{
			{Name: "service.name", Value: "svc-tenant-0-0"}, {Name: "_msg", Value: "a-" + strconv.Itoa(i)},
		}, 1)
		lr.MustAdd(logstorage.TenantID{AccountID: 1001}, now, []logstorage.Field{
			{Name: "service.name", Value: "svc-tenant-1001-0"}, {Name: "_msg", Value: "b-" + strconv.Itoa(i)},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.SetLocalBuffer(bs)
	startNs, endNs := now-int64(time.Hour), now+int64(time.Hour)

	count := func(ctx context.Context, ids []logstorage.TenantID) (int64, map[string]int) {
		t.Helper()
		q := mustParseQueryWithTime(t, "* | stats count() as rows", startNs, endNs)
		var n int64
		svcs := map[string]int{}
		var mu sync.Mutex
		if err := s.RunQuery(ctx, ids, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			if c := db.GetColumnByName("rows"); c != nil {
				for _, v := range c.Values {
					x, _ := strconv.ParseInt(v, 10, 64)
					n += x
				}
				return
			}
			n += int64(db.RowsCount())
			for _, v := range tsColumnValues(db, "service.name") {
				svcs[v]++
			}
		}); err != nil {
			t.Fatalf("RunQuery: %v", err)
		}
		return n, svcs
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{ID: "del-1001", Query: "*", StartNs: startNs, EndNs: endNs, Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 1001}}})
	s.SetTombstoneStore(store)

	if got, _ := count(context.Background(), tenant00); got != 4 {
		t.Errorf("tenant 0:0 pure-buffer count = %d, want 4: tenant 1001's delete must not touch it", got)
	}
	if got, _ := count(context.Background(), tenant1001); got != 0 {
		t.Errorf("tenant 1001:0 pure-buffer count = %d, want 0 after its delete", got)
	}
	got, svcs := count(storage.WithGlobalRead(context.Background()), tenant00)
	if got != 4 || svcs["svc-tenant-1001-0"] != 0 {
		t.Errorf("global read over the buffer: %d rows %v, want tenant 0:0's 4 only", got, svcs)
	}
}

func TestTombstoneScope_BridgeRowsAttributedPerTenant(t *testing.T) {
	s := testStorage()
	now := time.Now().UnixNano()
	var logs []schema.LogRow
	var spans []schema.TraceRow
	for i := 0; i < 3; i++ {
		logs = append(logs,
			schema.LogRow{TimestampUnixNano: now + int64(i), ServiceName: "svc-a", Body: "a", AccountID: 0},
			schema.LogRow{TimestampUnixNano: now + int64(i), ServiceName: "svc-b", Body: "b", AccountID: 1001})
		spans = append(spans,
			schema.TraceRow{TimestampUnixNano: now + int64(i), ServiceName: "svc-a", SpanName: "op", AccountID: 0},
			schema.TraceRow{TimestampUnixNano: now + int64(i), ServiceName: "svc-b", SpanName: "op", AccountID: 1001})
	}
	tss := []delete.Tombstone{{ID: "d", Query: "*", StartNs: math.MinInt64, EndNs: math.MaxInt64, Tenants: []delete.TenantRef{{AccountID: 1001}}}}

	collect := func() (*tombstoneSink, *int) {
		n := new(int)
		build := func(tss []tombstone) logstorage.WriteDataBlockFunc {
			return func(_ uint, db *logstorage.DataBlock) {
				if db = suppressTombstonedRows(db, tss); db != nil {
					*n += db.RowsCount()
				}
			}
		}
		return newTombstoneSink(tenantScope{all: true}, tss, delete.DefaultKeyTenant, build), n
	}

	sink, n := collect()
	if !sink.perTenant {
		t.Fatal("a global read with a tenant-scoped tombstone must attribute per tenant")
	}
	s.emitBridgeLogRows(tenantScope{all: true}, logs, sink)
	if *n != 3 {
		t.Errorf("bridged log rows kept = %d, want tenant 0:0's 3 (tenant 1001's hidden)", *n)
	}
	sink, n = collect()
	s.emitBridgeTraceRows(tenantScope{all: true}, spans, sink)
	if *n != 3 {
		t.Errorf("bridged spans kept = %d, want tenant 0:0's 3 (tenant 1001's hidden)", *n)
	}
	s.emitBridgeLogRows(tenantScope{all: true}, nil, sink) // no rows: nothing written
	if *n != 3 {
		t.Errorf("an empty bridge answer wrote rows: %d", *n)
	}
}

func TestTombstoneSink_UniformWhenAttributionCannotMatter(t *testing.T) {
	builds := 0
	build := func([]tombstone) logstorage.WriteDataBlockFunc {
		builds++
		return func(uint, *logstorage.DataBlock) {}
	}
	scoped := []delete.Tombstone{{ID: "s", Tenants: []delete.TenantRef{{AccountID: 5}}}}
	wide := []delete.Tombstone{{ID: "w"}}

	for name, sk := range map[string]*tombstoneSink{
		"single-tenant scope": newTombstoneSink(resolveTenantScope(tenant1001), scoped, delete.DefaultKeyTenant, build),
		"no scoped tombstone": newTombstoneSink(tenantScope{all: true}, wide, delete.DefaultKeyTenant, build),
		"uniformSink":         uniformSink(func(uint, *logstorage.DataBlock) {}),
	} {
		if sk.perTenant {
			t.Errorf("%s: per-tenant attribution built where it cannot change the answer", name)
		}
		if sk.forKey("1/0/logs/x.parquet") == nil || sk.forTenant(logstorage.TenantID{AccountID: 1}) == nil {
			t.Errorf("%s: no write function handed out", name)
		}
	}

	builds = 0
	sk := newTombstoneSink(tenantScope{all: true}, scoped, delete.DefaultKeyTenant, build)
	for i := 0; i < 3; i++ {
		sk.forKey("5/0/logs/" + strconv.Itoa(i) + ".parquet")
		sk.forKey("logs/legacy-" + strconv.Itoa(i) + ".parquet")
		sk.forTenant(logstorage.TenantID{AccountID: 5})
	}
	// uniform + one per distinct tenant source (key 5/0, legacy key, tenant 5:0)
	if builds != 4 {
		t.Errorf("write functions built = %d, want 4 (cached per tenant source)", builds)
	}
}

func TestTombstoneActsOnScope(t *testing.T) {
	a := delete.Tombstone{Tenants: []delete.TenantRef{{AccountID: 1001}}}
	wide := delete.Tombstone{}
	cases := []struct {
		name  string
		ts    delete.Tombstone
		scope tenantScope
		want  bool
	}{
		{"own tenant", a, resolveTenantScope(tenant1001), true},
		{"other tenant", a, resolveTenantScope(tenant00), false},
		{"list containing it", a, resolveTenantScope([]logstorage.TenantID{{}, {AccountID: 1001}}), true},
		{"global read", a, tenantScope{all: true}, true},
		{"unscoped legacy acts everywhere", wide, resolveTenantScope(tenant2002), true},
		{"non-numeric scope pair ignored", a, tenantScope{account: "acme", project: "0"}, false},
		{"non-numeric project ignored", a, tenantScope{account: "1001", project: "x"}, false},
	}
	for _, tc := range cases {
		if got := tombstoneActsOnScope(&tc.ts, tc.scope); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestKeyTenantParser_NilManifestUsesDefaultLayout(t *testing.T) {
	s := &Storage{}
	a, p, ok := s.keyTenantParser()("1001/2/logs/x.parquet")
	if !ok || a != "1001" || p != "2" {
		t.Errorf("default parser: got (%q,%q,%v)", a, p, ok)
	}
}

// Deletes land and are un-done while multi-tenant reads run: whatever the
// interleaving, the tenants a tombstone does not name never lose a row. Run
// under -race this also covers the per-query sink cache the file workers share.
func TestTombstoneScope_ConcurrentDeletesNeverTouchOtherTenants(t *testing.T) {
	f := newTenantScopeFixture(t)
	store := delete.NewTombstoneStore()
	f.s.SetTombstoneStore(store)

	stop := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			store.Add(scopedTombstone(f, "del-"+strconv.Itoa(i%4), delete.TenantRef{AccountID: 1001}))
			store.Remove("del-" + strconv.Itoa((i+2)%4))
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 20; i++ {
				_, svcs := f.runQuery(f.ctx(true), tenant00, "*")
				if svcs["svc-tenant-0-0"] != tsRowsT0 || svcs["svc-tenant-2002-7"] != tsRowsT2002 {
					t.Errorf("a delete of tenant 1001 hid other tenants' rows: %v", svcs)
					return
				}
				if n := svcs["svc-tenant-1001-0"]; n != 0 && n != tsRowsT1001 {
					t.Errorf("tenant 1001's rows were half hidden within one object: %d", n)
					return
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	writers.Wait()
}
