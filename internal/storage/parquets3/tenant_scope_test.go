package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// ---------------------------------------------------------------------------
// Tenant read-scoping: regression + invariant suite.
//
// The rule under test is the one upstream VL applies: a request is answered
// from EXACTLY ONE tenant's data — the tenant in its AccountID/ProjectID
// headers, or 0:0 when there are none — unless it presented a valid global-read
// credential. Every query class that turns a time range into cold-tier objects
// must obey it, not just the row scan.
//
// Both tenant layouts are covered:
//
//   - prefix: every tenant in one bucket under {AccountID}/{ProjectID}/.
//   - bucket: dedicated buckets per tenant, routed the way the binaries route
//     them (a key-derived bucket router on the client pool). Here the
//     invariant is stronger than "no foreign rows": a scoped request must not
//     issue a single request against another tenant's bucket, an unscoped
//     request only touches the default bucket, and an unknown tenant touches
//     none at all.
//
// Each tenant's rows carry a service name unique to that tenant, so a leak
// shows up as a foreign value in whatever a query class emits.
//
// Twin of lakehouse-traces/internal/storage/parquets3/tenant_scope_test.go.
// ---------------------------------------------------------------------------

const (
	tsPartition     = "dt=2026-05-10/hour=14"
	tsDefaultBucket = "test-bucket"
	tsRowsT0        = 3
	tsRowsT1001     = 5
	tsRowsT2002     = 2
	tsLegacyRows    = 4
)

type tsLayout string

const (
	tsLayoutPrefix tsLayout = "prefix"
	tsLayoutBucket tsLayout = "bucket"
)

func tsLayouts() []tsLayout { return []tsLayout{tsLayoutPrefix, tsLayoutBucket} }

// tsTenant describes one tenant's slice of the fixture.
type tsTenant struct {
	tenant  logstorage.TenantID
	key     string // S3 object key
	bucket  string // bucket the object lives in
	service string // service.name value unique to this tenant
	rows    int
}

type tsFixture struct {
	t       *testing.T
	s       *Storage
	mock    *bucketRecordingMock
	layout  tsLayout
	startNs int64
	endNs   int64
	svcCol  string
	tenants []tsTenant
	// legacy is the pre-tenant-layout object (no tenant segment in its key).
	legacy tsTenant
}

// tsBucketRouter mirrors applyTenantStorageOverrides in both binaries: the
// bucket is derived from the object key's numeric tenant segments, and keys
// without them resolve to the default bucket. In the bucket layout every tenant
// except 0:0 has a dedicated bucket; 0:0 and legacy objects stay in the default
// bucket, exactly like a mixed deployment where the other tenants carry an
// s3.bucket override.
func tsBucketRouter(key string) string {
	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 3 {
		return ""
	}
	if _, err := strconv.ParseUint(parts[0], 10, 32); err != nil {
		return ""
	}
	if _, err := strconv.ParseUint(parts[1], 10, 32); err != nil {
		return ""
	}
	if parts[0] == "0" && parts[1] == "0" {
		return ""
	}
	return "bucket-tenant-" + parts[0] + "-" + parts[1]
}

func tsBucketOf(layout tsLayout, key string) string {
	if layout == tsLayoutBucket {
		if b := tsBucketRouter(key); b != "" {
			return b
		}
	}
	return tsDefaultBucket
}

// newTenantScopeFixture builds the prefix layout (the regression shape).
func newTenantScopeFixture(t *testing.T) *tsFixture {
	t.Helper()
	return newTenantScopeFixtureLayout(t, tsLayoutPrefix)
}

// newTenantScopeFixtureLayout builds three tenants, all inside the same
// partition and time window, in the requested layout.
func newTenantScopeFixtureLayout(t *testing.T, layout tsLayout) *tsFixture {
	t.Helper()
	f := newEmptyTenantScopeFixture(t, layout, "{AccountID}/{ProjectID}/")
	now := tsNow()
	for _, tn := range []tsTenant{
		{tenant: logstorage.TenantID{}, key: "0/0/logs/" + tsPartition + "/t0.parquet", service: "svc-tenant-0-0", rows: tsRowsT0},
		{tenant: logstorage.TenantID{AccountID: 1001}, key: "1001/0/logs/" + tsPartition + "/t1001.parquet", service: "svc-tenant-1001-0", rows: tsRowsT1001},
		{tenant: logstorage.TenantID{AccountID: 2002, ProjectID: 7}, key: "2002/7/logs/" + tsPartition + "/t2002.parquet", service: "svc-tenant-2002-7", rows: tsRowsT2002},
	} {
		tn.bucket = tsBucketOf(layout, tn.key)
		f.put(now, tn.bucket, tn.key, tn.service, tn.rows)
		f.tenants = append(f.tenants, tn)
	}
	return f
}

func tsNow() time.Time { return time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC) }

func newEmptyTenantScopeFixture(t *testing.T, layout tsLayout, template string) *tsFixture {
	t.Helper()
	mock := newBucketRecordingMock()
	t.Cleanup(mock.srv.Close)
	s := testStorageWithS3(t, mock.srv.URL)
	s.manifest = manifest.New(tsDefaultBucket, "")
	if template != "" {
		s.manifest.SetPrefixTemplate(template)
	}
	if layout == tsLayoutBucket {
		s.pool.SetBucketRouter(tsBucketRouter)
	}
	now := tsNow()
	f := &tsFixture{
		t:       t,
		s:       s,
		mock:    mock,
		layout:  layout,
		startNs: now.Add(-time.Hour).UnixNano(),
		endNs:   now.Add(time.Hour).UnixNano(),
		svcCol:  "service.name",
	}
	if m := s.registry.ResolveFromParquet("service.name"); m != nil {
		f.svcCol = m.InternalName
	}
	return f
}

// addLegacyObject adds an object written under the pre-tenant static prefix
// (s3.prefix=logs/), i.e. with no tenant segment in its key.
func (f *tsFixture) addLegacyObject() {
	f.t.Helper()
	f.legacy = tsTenant{
		tenant:  logstorage.TenantID{},
		key:     "logs/" + tsPartition + "/legacy.parquet",
		bucket:  tsDefaultBucket,
		service: "svc-legacy",
		rows:    tsLegacyRows,
	}
	f.put(tsNow(), f.legacy.bucket, f.legacy.key, f.legacy.service, f.legacy.rows)
}

func (f *tsFixture) put(now time.Time, bucket, key, service string, n int) {
	f.t.Helper()
	rows := make([]logRow, n)
	for i := range rows {
		rows[i] = logRow{
			TimestampUnixNano: now.Add(time.Duration(i)).UnixNano(),
			ServiceName:       service,
			Body:              service,
			SeverityText:      "INFO",
		}
	}
	data := writeParquetToBytes(f.t, rows)
	f.mock.put(bucket, key, data)
	f.s.manifest.AddFile(tsPartition, manifest.FileInfo{
		Key:             key,
		Size:            int64(len(data)),
		RowCount:        int64(n),
		MinTimeNs:       rows[0].TimestampUnixNano,
		MaxTimeNs:       rows[n-1].TimestampUnixNano,
		LabelAggregates: map[string]map[string]int64{"service.name": {service: int64(n)}},
	})
}

// visible returns the fixture objects a request in this scope may read.
func (f *tsFixture) visible(tenantIDs []logstorage.TenantID, globalRead bool) []tsTenant {
	var out []tsTenant
	scope := resolveTenantScope(tenantIDs)
	for _, tn := range f.tenants {
		if globalRead || scopeForTenantID(tn.tenant) == scope {
			out = append(out, tn)
		}
	}
	if f.legacy.service != "" && (globalRead || scope.isDefault()) {
		out = append(out, f.legacy)
	}
	return out
}

func (f *tsFixture) expectedServices(tenantIDs []logstorage.TenantID, globalRead bool) map[string]bool {
	out := map[string]bool{}
	for _, tn := range f.visible(tenantIDs, globalRead) {
		out[tn.service] = true
	}
	return out
}

func (f *tsFixture) expectedRows(tenantIDs []logstorage.TenantID, globalRead bool) int {
	total := 0
	for _, tn := range f.visible(tenantIDs, globalRead) {
		total += tn.rows
	}
	return total
}

func (f *tsFixture) expectedBuckets(tenantIDs []logstorage.TenantID, globalRead bool) map[string]bool {
	out := map[string]bool{}
	for _, tn := range f.visible(tenantIDs, globalRead) {
		out[tn.bucket] = true
	}
	return out
}

func (f *tsFixture) expectedKeys(tenantIDs []logstorage.TenantID, globalRead bool) map[string]bool {
	out := map[string]bool{}
	for _, tn := range f.visible(tenantIDs, globalRead) {
		out[tn.key] = true
	}
	return out
}

// reloadManifestWithTemplate round-trips the manifest through a snapshot and
// loads it back under a different prefix template, which rebuilds the
// per-tenant aggregates from scratch under that template.
func (f *tsFixture) reloadManifestWithTemplate(template string) {
	f.t.Helper()
	path := filepath.Join(f.t.TempDir(), "manifest.snapshot")
	if err := f.s.manifest.SaveTo(path); err != nil {
		f.t.Fatalf("SaveTo: %v", err)
	}
	m := manifest.New(tsDefaultBucket, "")
	m.SetPrefixTemplate(template)
	if err := m.LoadFrom(path); err != nil {
		f.t.Fatalf("LoadFrom: %v", err)
	}
	if m.TotalFiles() != f.s.manifest.TotalFiles() {
		f.t.Fatalf("snapshot round-trip lost files: %d -> %d", f.s.manifest.TotalFiles(), m.TotalFiles())
	}
	f.s.manifest = m
}

func (f *tsFixture) ctx(globalRead bool) context.Context {
	if globalRead {
		return storage.WithGlobalRead(context.Background())
	}
	return context.Background()
}

// runQuery executes a LogsQL query and returns (rows emitted, service values).
func (f *tsFixture) runQuery(ctx context.Context, tenantIDs []logstorage.TenantID, queryStr string) (int, map[string]int) {
	f.t.Helper()
	q := mustParseQueryWithTime(f.t, queryStr, f.startNs, f.endNs)
	svcs := map[string]int{}
	rows := 0
	var mu sync.Mutex
	err := f.s.RunQuery(ctx, tenantIDs, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		rows += db.RowsCount()
		// The traces profile emits the service under two aliases
		// (`service.name` and `resource_attr:service.name`); count ONE of them
		// per block so the totals stay comparable across modules.
		var values []string
		var found bool
		for _, c := range db.GetColumns(false) {
			if c.Name == f.svcCol {
				values, found = c.Values, true
				break
			}
			if !found && c.Name == "service.name" {
				values, found = c.Values, true
			}
		}
		for _, v := range values {
			svcs[v]++
		}
	})
	if err != nil {
		f.t.Fatalf("RunQuery(%q): %v", queryStr, err)
	}
	return rows, svcs
}

// tsCase is one (name, tenantIDs, globalRead) request shape.
type tsCase struct {
	name       string
	tenantIDs  []logstorage.TenantID
	globalRead bool
}

// tsCases covers the request shapes the scoping rule distinguishes: unscoped
// (default), scoped, unknown, and global read.
func tsCases() []tsCase {
	return []tsCase{
		{name: "default/no-headers", tenantIDs: nil},
		{name: "default/explicit-0:0", tenantIDs: []logstorage.TenantID{{}}},
		{name: "scoped/1001:0", tenantIDs: []logstorage.TenantID{{AccountID: 1001}}},
		{name: "scoped/2002:7", tenantIDs: []logstorage.TenantID{{AccountID: 2002, ProjectID: 7}}},
		{name: "unknown/7:0", tenantIDs: []logstorage.TenantID{{AccountID: 7}}},
		{name: "global-read", tenantIDs: []logstorage.TenantID{{}}, globalRead: true},
	}
}

// assertNoForeignValues is THE invariant: nothing a query class emits may carry
// a value that belongs to a tenant other than the one the request is scoped to.
func assertNoForeignValues(t *testing.T, class string, got map[string]int, allowed map[string]bool) {
	t.Helper()
	for v := range got {
		if !strings.HasPrefix(v, "svc-") {
			continue // not a tenant marker (severity, body, field names, …)
		}
		if !allowed[v] {
			t.Errorf("%s leaked a value belonging to another tenant: %q (allowed: %v)", class, v, tsSortedKeys(allowed))
		}
	}
}

// assertBucketsWithin is the bucket-layout half of the invariant: the request
// must not have issued any S3 request against a bucket it does not own.
func assertBucketsWithin(t *testing.T, class string, touched map[string]int, allowed map[string]bool) {
	t.Helper()
	for b := range touched {
		if !allowed[b] {
			t.Errorf("%s touched bucket %q; allowed buckets for this request: %v (touched: %v)", class, b, tsSortedKeys(allowed), touched)
		}
	}
}

func tsSortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func valuesToCounts(vals []logstorage.ValueWithHits) map[string]int {
	out := map[string]int{}
	for _, v := range vals {
		out[v.Value] += int(v.Hits) + 1
	}
	return out
}

// ---------------------------------------------------------------------------
// Regression: the exact classes that were leaking.
// ---------------------------------------------------------------------------

func TestTenantScope_Regression_FullScan(t *testing.T) {
	f := newTenantScopeFixture(t)
	ctx := context.Background()

	if rows, svcs := f.runQuery(ctx, nil, "*"); rows != tsRowsT0 || svcs["svc-tenant-1001-0"] != 0 {
		t.Errorf("unscoped scan: want %d rows of tenant 0:0 only, got rows=%d svcs=%v", tsRowsT0, rows, svcs)
	}
	if rows, svcs := f.runQuery(ctx, []logstorage.TenantID{{AccountID: 1001}}, "*"); rows != tsRowsT1001 || svcs["svc-tenant-0-0"] != 0 {
		t.Errorf("scoped 1001:0 scan: want %d rows of tenant 1001:0 only, got rows=%d svcs=%v", tsRowsT1001, rows, svcs)
	}
	if rows, _ := f.runQuery(ctx, []logstorage.TenantID{{AccountID: 7}}, "*"); rows != 0 {
		t.Errorf("unknown tenant 7:0 scan: want 0 rows, got %d", rows)
	}
}

func TestTenantScope_Regression_ManifestTimestampFastPath(t *testing.T) {
	f := newTenantScopeFixture(t)
	ctx := storage.WithTimestampOnlyHint(context.Background())
	if rows, _ := f.runQuery(ctx, nil, "*"); rows != tsRowsT0 {
		t.Errorf("timestamp-only fast path, tenant 0:0: want %d rows, got %d", tsRowsT0, rows)
	}
	if rows, _ := f.runQuery(ctx, []logstorage.TenantID{{AccountID: 7}}, "*"); rows != 0 {
		t.Errorf("timestamp-only fast path, unknown tenant: want 0 rows, got %d", rows)
	}
}

func TestTenantScope_Regression_CountPushdown(t *testing.T) {
	f := newTenantScopeFixture(t)
	_, svcs := f.runQuery(context.Background(), nil, "* | stats by (service.name) count()")
	if svcs["svc-tenant-1001-0"] != 0 || svcs["svc-tenant-2002-7"] != 0 || svcs["svc-tenant-0-0"] == 0 {
		t.Errorf("count-pushdown fast path, tenant 0:0: want only svc-tenant-0-0, got %v", svcs)
	}
}

func TestTenantScope_Regression_FieldEnumeration(t *testing.T) {
	f := newTenantScopeFixture(t)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

	vals, err := f.s.GetFieldValues(context.Background(), []logstorage.TenantID{{}}, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	for _, v := range vals {
		if v.Value != "svc-tenant-0-0" && strings.HasPrefix(v.Value, "svc-") {
			t.Errorf("field_values for tenant 0:0 leaked %q (all: %+v)", v.Value, vals)
		}
	}

	names, err := f.s.GetFieldNames(context.Background(), []logstorage.TenantID{{AccountID: 7}}, q)
	if err != nil {
		t.Fatalf("GetFieldNames: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("field_names for unknown tenant 7:0: want none, got %d", len(names))
	}
}

// ---------------------------------------------------------------------------
// Invariant matrix: layout × query class × request shape.
// ---------------------------------------------------------------------------

type tsClass struct {
	name string
	run  func(f *tsFixture, ctx context.Context, tenantIDs []logstorage.TenantID) map[string]int
}

func tsQueryClasses() []tsClass {
	fieldQuery := func(f *tsFixture) *logstorage.Query {
		return mustParseQueryWithTime(f.t, "*", f.startNs, f.endNs)
	}
	return []tsClass{
		{"scan", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			_, svcs := f.runQuery(ctx, ids, "*")
			return svcs
		}},
		{"timestamp-only", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			_, svcs := f.runQuery(storage.WithTimestampOnlyHint(ctx), ids, "*")
			return svcs
		}},
		{"count-pushdown", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			_, svcs := f.runQuery(ctx, ids, "* | stats by (service.name) count()")
			return svcs
		}},
		{"stats-by-scan", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			_, svcs := f.runQuery(ctx, ids, `severity_text:INFO | stats by (service.name) count()`)
			return svcs
		}},
		{"hits", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			_, svcs := f.runQuery(ctx, ids, `* | stats by (service.name, _time:1h) count() hits`)
			return svcs
		}},
		{"field_names", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			vals, err := f.s.GetFieldNames(ctx, ids, fieldQuery(f))
			if err != nil {
				f.t.Fatalf("GetFieldNames: %v", err)
			}
			return valuesToCounts(vals)
		}},
		{"field_values", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			vals, err := f.s.GetFieldValues(ctx, ids, fieldQuery(f), "service.name", 100)
			if err != nil {
				f.t.Fatalf("GetFieldValues: %v", err)
			}
			return valuesToCounts(vals)
		}},
		{"stream_field_values", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			vals, err := f.s.GetStreamFieldValues(ctx, ids, fieldQuery(f), "service.name", 100)
			if err != nil {
				f.t.Fatalf("GetStreamFieldValues: %v", err)
			}
			return valuesToCounts(vals)
		}},
		{"streams", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			vals, err := f.s.GetStreams(ctx, ids, fieldQuery(f), 100)
			if err != nil {
				f.t.Fatalf("GetStreams: %v", err)
			}
			return valuesToCounts(vals)
		}},
		{"stream_ids", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			vals, err := f.s.GetStreamIDs(ctx, ids, fieldQuery(f), 100)
			if err != nil {
				f.t.Fatalf("GetStreamIDs: %v", err)
			}
			return valuesToCounts(vals)
		}},
		{"catalog-warm", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			f.s.cfg.Pmeta.Enabled = true
			f.s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
			f.s.WarmCatalog(context.Background())
			f.mock.reset() // the warm-up is startup work, not part of the request
			vals, err := f.s.GetFieldValues(ctx, ids, fieldQuery(f), "service.name", 100)
			if err != nil {
				f.t.Fatalf("GetFieldValues (catalog warm): %v", err)
			}
			return valuesToCounts(vals)
		}},
		{"labelIndex-warm", func(f *tsFixture, ctx context.Context, ids []logstorage.TenantID) map[string]int {
			f.s.WarmLabelIndex(context.Background())
			f.mock.reset() // the warm-up is startup work, not part of the request
			names, err := f.s.GetFieldNames(ctx, ids, fieldQuery(f))
			if err != nil {
				f.t.Fatalf("GetFieldNames (label index warm): %v", err)
			}
			vals, err := f.s.GetFieldValues(ctx, ids, fieldQuery(f), "service.name", 100)
			if err != nil {
				f.t.Fatalf("GetFieldValues (label index warm): %v", err)
			}
			out := valuesToCounts(names)
			for k, v := range valuesToCounts(vals) {
				out[k] += v
			}
			return out
		}},
	}
}

func TestTenantScope_Invariant_AllQueryClasses(t *testing.T) {
	for _, layout := range tsLayouts() {
		for _, cls := range tsQueryClasses() {
			for _, tc := range tsCases() {
				t.Run(string(layout)+"/"+cls.name+"/"+tc.name, func(t *testing.T) {
					f := newTenantScopeFixtureLayout(t, layout)
					f.addLegacyObject()
					f.mock.reset()

					got := cls.run(f, f.ctx(tc.globalRead), tc.tenantIDs)

					assertNoForeignValues(t, cls.name, got, f.expectedServices(tc.tenantIDs, tc.globalRead))
					assertBucketsWithin(t, cls.name, f.mock.bucketsTouched(), f.expectedBuckets(tc.tenantIDs, tc.globalRead))
				})
			}
		}
	}
}

// TestTenantScope_Invariant_ExactRowCounts pins the positive side of the
// invariant for the row-returning classes: every tenant gets ALL of its own
// rows (a scoping bug that dropped rows would be just as wrong as a leak).
func TestTenantScope_Invariant_ExactRowCounts(t *testing.T) {
	for _, layout := range tsLayouts() {
		for _, hint := range []string{"scan", "timestamp-only"} {
			for _, tc := range tsCases() {
				t.Run(string(layout)+"/"+hint+"/"+tc.name, func(t *testing.T) {
					f := newTenantScopeFixtureLayout(t, layout)
					f.addLegacyObject()
					ctx := f.ctx(tc.globalRead)
					if hint == "timestamp-only" {
						ctx = storage.WithTimestampOnlyHint(ctx)
					}
					rows, _ := f.runQuery(ctx, tc.tenantIDs, "*")
					if want := f.expectedRows(tc.tenantIDs, tc.globalRead); rows != want {
						t.Errorf("rows = %d, want exactly %d", rows, want)
					}
				})
			}
		}
	}
}

// TestTenantScope_Invariant_FileSelection asserts the property directly at the
// selection layer, for every named call site: the objects a scope is handed
// are exactly the objects it owns.
func TestTenantScope_Invariant_FileSelection(t *testing.T) {
	sites := []string{"query", "field_names", "field_values", "streams", "stream_ids",
		"catalog_field_names", "catalog_field_values"}

	for _, layout := range tsLayouts() {
		f := newTenantScopeFixtureLayout(t, layout)
		f.addLegacyObject()
		for _, site := range sites {
			for _, tc := range tsCases() {
				t.Run(string(layout)+"/"+site+"/"+tc.name, func(t *testing.T) {
					scope := scopeFor(f.ctx(tc.globalRead), tc.tenantIDs)
					files := f.s.filesForScope(site, f.startNs, f.endNs, scope)
					allowed := f.expectedKeys(tc.tenantIDs, tc.globalRead)
					for _, fi := range files {
						if !allowed[fi.Key] {
							t.Errorf("site %q, scope %s selected foreign object %q", site, scope, fi.Key)
						}
					}
					if len(files) != len(allowed) {
						t.Errorf("site %q, scope %s selected %d objects, want %d (%v)", site, scope, len(files), len(allowed), tsSortedKeys(allowed))
					}
				})
			}
		}
	}
}

// TestTenantScope_LabelIndex_GatedByTenantCount checks the one in-RAM structure
// that is NOT tenant-keyed: it may only answer while the manifest holds a single
// tenant scope.
func TestTenantScope_LabelIndex_GatedByTenantCount(t *testing.T) {
	multi := newTenantScopeFixture(t)
	if multi.s.tenantScopeAllowsGlobalIndex(tenantScope{account: "1001", project: "0"}) {
		t.Error("the global label index was allowed to answer a scoped query in a multi-tenant manifest")
	}
	if !multi.s.tenantScopeAllowsGlobalIndex(tenantScope{all: true}) {
		t.Error("a validated cross-tenant read may use the global label index")
	}

	single := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
	single.put(tsNow(), tsDefaultBucket, "0/0/logs/"+tsPartition+"/only.parquet", "svc-tenant-0-0", 2)
	if !single.s.tenantScopeAllowsGlobalIndex(tenantScope{account: "0", project: "0"}) {
		t.Error("a single-tenant manifest should keep the global label index fast path")
	}
}

// ---------------------------------------------------------------------------
// Legacy layout: untenanted objects belong to 0:0 and must not be dropped.
// ---------------------------------------------------------------------------

func TestTenantScope_LegacyLayout_BelongsToDefaultTenant(t *testing.T) {
	for _, layout := range tsLayouts() {
		t.Run(string(layout), func(t *testing.T) {
			f := newTenantScopeFixtureLayout(t, layout)
			f.addLegacyObject()

			rows, svcs := f.runQuery(context.Background(), nil, "*")
			if svcs["svc-legacy"] != tsLegacyRows {
				t.Errorf("tenant 0:0 must still see the legacy (untenanted) object: got %v", svcs)
			}
			if want := tsRowsT0 + tsLegacyRows; rows != want {
				t.Errorf("tenant 0:0 rows = %d, want %d (its own %d + legacy %d)", rows, want, tsRowsT0, tsLegacyRows)
			}

			_, svcs = f.runQuery(context.Background(), []logstorage.TenantID{{AccountID: 1001}}, "*")
			if svcs["svc-legacy"] != 0 {
				t.Errorf("tenant 1001:0 must NOT see the legacy object: got %v", svcs)
			}
		})
	}
}

func TestTenantScope_LegacyLayout_OnlyLegacyData(t *testing.T) {
	// A deployment that never used the tenant prefix template: every object is
	// untenanted, so 0:0 sees all of it and nobody else sees any of it.
	f := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "")
	f.put(tsNow(), tsDefaultBucket, "logs/"+tsPartition+"/a.parquet", "svc-legacy", 4)

	if rows, _ := f.runQuery(context.Background(), nil, "*"); rows != 4 {
		t.Errorf("all-legacy deployment, tenant 0:0: want 4 rows, got %d", rows)
	}
	if rows, _ := f.runQuery(context.Background(), []logstorage.TenantID{{AccountID: 9}}, "*"); rows != 0 {
		t.Errorf("all-legacy deployment, tenant 9:0: want 0 rows, got %d", rows)
	}
}

// ---------------------------------------------------------------------------
// Property test over randomly generated tenant sets, both layouts.
// ---------------------------------------------------------------------------

func TestTenantScope_Property_RandomTenantSets(t *testing.T) {
	rnd := rand.New(rand.NewSource(20260913))

	for iteration := 0; iteration < 12; iteration++ {
		layout := tsLayouts()[iteration%2]
		f := newEmptyTenantScopeFixture(t, layout, "{AccountID}/{ProjectID}/")

		n := 2 + rnd.Intn(6)
		seen := map[logstorage.TenantID]bool{}
		for i := 0; i < n; i++ {
			tid := logstorage.TenantID{
				AccountID: uint32(rnd.Intn(5)),
				ProjectID: uint32(rnd.Intn(3)),
			}
			if seen[tid] {
				continue
			}
			seen[tid] = true
			tn := tsTenant{
				tenant:  tid,
				key:     fmt.Sprintf("%d/%d/logs/%s/f%d.parquet", tid.AccountID, tid.ProjectID, tsPartition, i),
				service: fmt.Sprintf("svc-%d-%d", tid.AccountID, tid.ProjectID),
				rows:    1 + rnd.Intn(4),
			}
			tn.bucket = tsBucketOf(layout, tn.key)
			f.put(tsNow(), tn.bucket, tn.key, tn.service, tn.rows)
			f.tenants = append(f.tenants, tn)
		}

		// Query as every present tenant, plus one that is not present at all.
		probes := append([]tsTenant{}, f.tenants...)
		probes = append(probes, tsTenant{tenant: logstorage.TenantID{AccountID: 99, ProjectID: 99}})
		for _, probe := range probes {
			ids := []logstorage.TenantID{probe.tenant}
			f.mock.reset()
			gotRows, svcs := f.runQuery(context.Background(), ids, "*")
			assertNoForeignValues(t, "property-scan", svcs, f.expectedServices(ids, false))
			assertBucketsWithin(t, "property-scan", f.mock.bucketsTouched(), f.expectedBuckets(ids, false))
			if want := f.expectedRows(ids, false); gotRows != want {
				t.Fatalf("iteration %d (%s), tenant %d:%d: rows = %d, want %d",
					iteration, layout, probe.tenant.AccountID, probe.tenant.ProjectID, gotRows, want)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Concurrency: tenants querying at once must not see each other (-race).
// ---------------------------------------------------------------------------

func TestTenantScope_Concurrent_Tenants(t *testing.T) {
	for _, layout := range tsLayouts() {
		t.Run(string(layout), func(t *testing.T) {
			f := newTenantScopeFixtureLayout(t, layout)

			var wg sync.WaitGroup
			errs := make(chan string, 256)
			for i := 0; i < 8; i++ {
				for _, tn := range f.tenants {
					wg.Add(1)
					go func(tn tsTenant) {
						defer wg.Done()
						rows, svcs := f.runQuery(context.Background(), []logstorage.TenantID{tn.tenant}, "*")
						if rows != tn.rows {
							errs <- fmt.Sprintf("tenant %s: rows=%d want %d", tn.service, rows, tn.rows)
						}
						for v := range svcs {
							if strings.HasPrefix(v, "svc-") && v != tn.service {
								errs <- fmt.Sprintf("tenant %s saw %q", tn.service, v)
							}
						}
					}(tn)
				}
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				t.Error(e)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fault injection: a tenant whose manifest aggregate is missing must get an
// EMPTY answer, never everyone's data.
// ---------------------------------------------------------------------------

func TestTenantScope_Fault_MissingTenantAggregate(t *testing.T) {
	for _, layout := range tsLayouts() {
		t.Run(string(layout), func(t *testing.T) {
			f := newTenantScopeFixtureLayout(t, layout)

			// Template drift between the writer that saved the manifest snapshot
			// and the reader that loads it: the aggregates are rebuilt under a
			// single-segment template, so every {account, project} lookup the
			// read path makes misses. The objects are all still there. The read
			// path must degrade to "nothing", not to "everything".
			f.reloadManifestWithTemplate("{OrgID}/")

			for _, tn := range f.tenants {
				account := strconv.FormatUint(uint64(tn.tenant.AccountID), 10)
				project := strconv.FormatUint(uint64(tn.tenant.ProjectID), 10)
				if got := f.s.manifest.GetFilesForRangeTenant(f.startNs, f.endNs, account, project); len(got) != 0 {
					t.Fatalf("fault not injected: tenant %s:%s still resolves %d objects", account, project, len(got))
				}
				f.mock.reset()
				rows, svcs := f.runQuery(context.Background(), []logstorage.TenantID{tn.tenant}, "*")
				for v := range svcs {
					if strings.HasPrefix(v, "svc-") && v != tn.service {
						t.Errorf("with broken tenant aggregates, tenant %s saw foreign value %q", tn.service, v)
					}
				}
				if rows > tn.rows {
					t.Errorf("with broken tenant aggregates, tenant %s got %d rows (own data is %d) — it must degrade to empty, never to all tenants",
						tn.service, rows, tn.rows)
				}
				assertBucketsWithin(t, "fault", f.mock.bucketsTouched(), map[string]bool{tn.bucket: true})
			}
		})
	}
}

// TestTenantScope_Guard_DropsForeignObject proves the last line of defence:
// even if something hands the read path an object that is not the tenant's, the
// guard removes it instead of reading it.
func TestTenantScope_Guard_DropsForeignObject(t *testing.T) {
	f := newTenantScopeFixture(t)
	files := []manifest.FileInfo{
		{Key: "1001/0/logs/" + tsPartition + "/mine.parquet"},
		{Key: "2002/7/logs/" + tsPartition + "/theirs.parquet"},
		{Key: "logs/" + tsPartition + "/legacy.parquet"},
	}
	got := f.s.guardTenantFiles("test", tenantScope{account: "1001", project: "0"}, files)
	if len(got) != 1 || got[0].Key != files[0].Key {
		t.Errorf("guard kept %+v, want only %q", got, files[0].Key)
	}

	// The default tenant keeps the legacy object.
	got = f.s.guardTenantFiles("test", tenantScope{account: "0", project: "0"}, files)
	if len(got) != 1 || got[0].Key != files[2].Key {
		t.Errorf("guard for 0:0 kept %+v, want only the legacy object %q", got, files[2].Key)
	}

	// A cross-tenant (global-read) scope keeps everything.
	if got := f.s.guardTenantFiles("test", tenantScope{all: true}, files); len(got) != len(files) {
		t.Errorf("global-read guard kept %d objects, want %d", len(got), len(files))
	}
}

// ---------------------------------------------------------------------------
// Bucket-recording S3 double.
// ---------------------------------------------------------------------------

// bucketRecordingMock serves objects per (bucket, key) and records every bucket
// it was asked for, so the bucket layout can prove which buckets a request
// reached.
type bucketRecordingMock struct {
	mu      sync.Mutex
	files   map[string][]byte // "bucket/key" -> data
	buckets map[string]int    // bucket -> request count
	srv     *httptest.Server
}

func newBucketRecordingMock() *bucketRecordingMock {
	m := &bucketRecordingMock{
		files:   map[string][]byte{},
		buckets: map[string]int{},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *bucketRecordingMock) put(bucket, key string, data []byte) {
	m.mu.Lock()
	m.files[bucket+"/"+key] = data
	m.mu.Unlock()
}

func (m *bucketRecordingMock) reset() {
	m.mu.Lock()
	m.buckets = map[string]int{}
	m.mu.Unlock()
}

func (m *bucketRecordingMock) bucketsTouched() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.buckets))
	for k, v := range m.buckets {
		out[k] = v
	}
	return out
}

func (m *bucketRecordingMock) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	bucket, key := parts[0], parts[1]

	m.mu.Lock()
	m.buckets[bucket]++
	data, ok := m.files[bucket+"/"+key]
	m.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	if rangeHdr := r.Header.Get("Range"); strings.HasPrefix(rangeHdr, "bytes=") {
		bounds := strings.SplitN(strings.TrimPrefix(rangeHdr, "bytes="), "-", 2)
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		if start >= int64(len(data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

// ---------------------------------------------------------------------------
// Buffer bridge fan-out with a fake peer (layout-independent: the bridge
// carries rows, not objects).
// ---------------------------------------------------------------------------

func TestTenantScope_BufferBridge_FanOutIsScoped(t *testing.T) {
	rows := []schema.LogRow{
		{TimestampUnixNano: 100, AccountID: 0, ProjectID: 0, Body: "b", ServiceName: "svc-tenant-0-0"},
		{TimestampUnixNano: 101, AccountID: 1001, ProjectID: 0, Body: "b", ServiceName: "svc-tenant-1001-0"},
	}

	t.Run("peer filters and the caller trusts only a scoped answer", func(t *testing.T) {
		var mu sync.Mutex
		var gotAccount, gotProject, gotVersion string
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			gotAccount = r.URL.Query().Get("account_id")
			gotProject = r.URL.Query().Get("project_id")
			gotVersion = r.URL.Query().Get("tenant_scope")
			account, project := gotAccount, gotProject
			mu.Unlock()
			w.Header().Set(buffer.TenantScopeHeader, account+":"+project)
			enc := json.NewEncoder(w)
			for _, row := range rows {
				if strconv.FormatUint(uint64(row.AccountID), 10) != account {
					continue
				}
				_ = enc.Encode(row)
			}
		}))
		defer peer.Close()

		bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeLogs)
		bridge.SetEndpoints([]string{peer.URL})

		got, err := bridge.QueryLogs(context.Background(), 0, 1000, "1001", "0")
		if err != nil {
			t.Fatalf("QueryLogs: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if gotVersion != buffer.TenantScopeVersion {
			t.Errorf("bridge sent tenant_scope=%q, want %q", gotVersion, buffer.TenantScopeVersion)
		}
		if gotAccount != "1001" || gotProject != "0" {
			t.Errorf("bridge asked the peer for tenant %s:%s, want 1001:0", gotAccount, gotProject)
		}
		if len(got) != 1 || got[0].AccountID != 1001 {
			t.Errorf("bridge returned %+v, want only tenant 1001's row", got)
		}
	})

	t.Run("an unscoped peer answer is refused", func(t *testing.T) {
		// A pre-fix peer: ignores the parameters, echoes no header, returns
		// every tenant's rows.
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			enc := json.NewEncoder(w)
			for _, row := range rows {
				_ = enc.Encode(row)
			}
		}))
		defer peer.Close()

		bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeLogs)
		bridge.SetEndpoints([]string{peer.URL})

		got, err := bridge.QueryLogs(context.Background(), 0, 1000, "1001", "0")
		if err != nil {
			t.Fatalf("QueryLogs: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("rows from a peer that did not prove tenant scoping must be dropped, got %+v", got)
		}
	})

	t.Run("a peer echoing a different tenant is refused", func(t *testing.T) {
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(buffer.TenantScopeHeader, "0:0")
			enc := json.NewEncoder(w)
			for _, row := range rows {
				_ = enc.Encode(row)
			}
		}))
		defer peer.Close()

		bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
		bridge.SetEndpoints([]string{peer.URL})
		got, err := bridge.QueryTraces(context.Background(), 0, 1000, "1001", "0")
		if err != nil {
			t.Fatalf("QueryTraces: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("rows declared for another tenant must be dropped, got %+v", got)
		}
	})

	t.Run("row conversion drops rows the peer should not have sent", func(t *testing.T) {
		s := testStorage()
		db := s.logRowsToDataBlock(tenantScope{account: "1001", project: "0"}, "test", rows)
		if db == nil || db.RowsCount() != 1 {
			t.Fatalf("expected exactly tenant 1001's row, got %v", db)
		}
		if db := s.logRowsToDataBlock(tenantScope{account: "77", project: "0"}, "test", rows); db != nil && db.RowsCount() != 0 {
			t.Errorf("a tenant with no buffered rows got %d rows", db.RowsCount())
		}

		traceRows := []schema.TraceRow{
			{TimestampUnixNano: 100, AccountID: 0, ProjectID: 0, TraceID: "t0", SpanID: "s0"},
			{TimestampUnixNano: 101, AccountID: 1001, ProjectID: 0, TraceID: "t1001", SpanID: "s1"},
		}
		if db := s.traceRowsToDataBlock(tenantScope{account: "0", project: "0"}, "test", traceRows); db == nil || db.RowsCount() != 1 {
			t.Errorf("trace conversion for 0:0: want 1 row, got %v", db)
		}
		if db := s.traceRowsToDataBlock(tenantScope{all: true}, "test", traceRows); db == nil || db.RowsCount() != 2 {
			t.Errorf("trace conversion for a cross-tenant read: want 2 rows, got %v", db)
		}
	})
}

// ---------------------------------------------------------------------------
// /select/tenant_ids reports the real tenant list.
// ---------------------------------------------------------------------------

func TestTenantScope_TenantIDsForRange(t *testing.T) {
	f := newTenantScopeFixture(t)
	f.addLegacyObject()

	got := f.s.TenantIDsForRange(f.startNs, f.endNs)
	want := []logstorage.TenantID{
		{AccountID: 0, ProjectID: 0},
		{AccountID: 1001, ProjectID: 0},
		{AccountID: 2002, ProjectID: 7},
	}
	if len(got) != len(want) {
		t.Fatalf("TenantIDsForRange = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("TenantIDsForRange[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	if got := f.s.TenantIDsForRange(f.endNs+int64(48*time.Hour), f.endNs+int64(49*time.Hour)); len(got) != 0 {
		t.Errorf("TenantIDsForRange outside the data window = %+v, want none", got)
	}
}

// ---------------------------------------------------------------------------
// Scope resolution unit tests.
// ---------------------------------------------------------------------------

func TestTenantScope_Resolution(t *testing.T) {
	if got := resolveTenantScope(nil); got.all || got.account != "0" || got.project != "0" {
		t.Errorf("no tenant list must resolve to the default tenant 0:0, got %+v", got)
	}
	if got := resolveTenantScope([]logstorage.TenantID{{AccountID: 4, ProjectID: 9}}); got.all || got.account != "4" || got.project != "9" {
		t.Errorf("single tenant must resolve to itself, got %+v", got)
	}
	if got := resolveTenantScope([]logstorage.TenantID{{}, {AccountID: 1}}); !got.all {
		t.Errorf("more than one tenant is the cross-tenant scope, got %+v", got)
	}
	if got := scopeFor(storage.WithGlobalRead(context.Background()), []logstorage.TenantID{{AccountID: 1001}}); !got.all {
		t.Errorf("a validated global-read context must widen the scope, got %+v", got)
	}
	if got := scopeFor(context.Background(), []logstorage.TenantID{{AccountID: 1001}}); got.all {
		t.Error("a plain context must never widen the scope")
	}
	if got := (tenantScope{all: true}).String(); got != "*" {
		t.Errorf("cross-tenant scope String() = %q, want *", got)
	}
	if got := (tenantScope{account: "3", project: "4"}).String(); got != "3:4" {
		t.Errorf("scope String() = %q, want 3:4", got)
	}
}

func TestTenantScope_OrgIDShapedTemplate(t *testing.T) {
	// A single-segment template identifies the tenant by its first key segment
	// alone; the ownership check must accept exactly that segment.
	parse := func(key string) (string, string, bool) {
		first, _, ok := strings.Cut(key, "/")
		if !ok || first == "logs" {
			return "", "", false
		}
		return first, "", true
	}
	scope := tenantScope{account: "acme", project: "0"}
	if !tenantOwnsKey(parse, scope, "acme/logs/"+tsPartition+"/a.parquet") {
		t.Error("single-segment key of the tenant must be owned by it")
	}
	if tenantOwnsKey(parse, scope, "globex/logs/"+tsPartition+"/a.parquet") {
		t.Error("single-segment key of another tenant must not be owned")
	}
}
