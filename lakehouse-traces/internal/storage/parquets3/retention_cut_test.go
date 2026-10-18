package parquets3

import (
	"net/http"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/retention"
)

// Twin of lakehouse-traces/internal/storage/parquets3/retention_cut_test.go.
//
// A query never plans a file past its retention, although the retention loop may
// not have deleted it yet. The cut is the same predicate as the eviction
// (internal/retention.Policy: age of the file's newest row > TTL) judged by the
// object store's clock. The fixture's rows were written at tsNow(); each test
// moves the store clock, so the result does not depend on the host's date.

func mustRetention(t *testing.T, cfg retention.Config) *retention.Policy {
	t.Helper()
	p, err := retention.NewPolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// cutFx moves the object store's clock for the queries of one fixture. The mock
// S3 answers carry the real Date, which would move the clock back to today on the
// first object read, so every query re-applies the chosen time first.
type cutFx struct {
	f  *tsFixture
	at time.Time
}

func (c *cutFx) observe() {
	c.f.s.Pool().StoreClock().Observe(c.at.UTC().Format(http.TimeFormat))
}

// now makes the store clock read at (to the second) for the queries that follow.
func (c *cutFx) now(at time.Time) {
	c.at = at
	c.observe()
}

// rows is the number of rows a query of the tenants (or a global read) returns.
func (c *cutFx) rows(tenants []logstorage.TenantID, global bool) int {
	if !c.at.IsZero() {
		c.observe()
	}
	n, _ := c.f.runQuery(c.f.ctx(global), tenants, "*")
	return n
}

// putLabelled adds an object whose manifest entry carries the account_id and
// project_id labels the tenant retention rules match on.
func putLabelled(f *tsFixture, account, project, key, service string, n int) {
	f.t.Helper()
	now := tsNow()
	rows := make([]logRow, n)
	for i := range rows {
		rows[i] = logRow{TimestampUnixNano: now.Add(time.Duration(i)).UnixNano(), ServiceName: service, Body: service, SeverityText: "INFO"}
	}
	data := writeParquetToBytes(f.t, rows)
	f.mock.put(tsDefaultBucket, key, data)
	f.s.manifest.AddFile(tsPartition, manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: int64(n),
		MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[n-1].TimestampUnixNano,
		LabelAggregates: map[string]map[string]int64{"service.name": {service: int64(n)}},
		Labels:          map[string][]string{"account_id": {account}, "project_id": {project}},
	})
}

// Failing first: before the cut a file past its retention was read until the
// eviction loop got to it; a file inside it is read before and after.
func TestRetentionCut_PlansExcludeExpiredFiles(t *testing.T) {
	f := newTenantScopeFixture(t)
	c := &cutFx{f: f}
	all := f.expectedRows(nil, true)
	if all == 0 {
		t.Fatal("fixture has no rows")
	}
	ttl := 30 * 24 * time.Hour
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "30d"}))

	c.now(tsNow().Add(ttl - time.Hour))
	if got := c.rows(nil, true); got != all {
		t.Fatalf("a file inside its retention: %d rows, want %d", got, all)
	}
	c.now(tsNow().Add(ttl + time.Hour))
	if got := c.rows(nil, true); got != 0 {
		t.Fatalf("a file past its retention was read: %d rows, want 0", got)
	}

	// No policy, and a disabled policy, cut nothing, whatever the age.
	for name, p := range map[string]*retention.Policy{
		"no policy":       nil,
		"retention off":   mustRetention(t, retention.Config{}),
		"a 100-year keep": mustRetention(t, retention.Config{Enabled: true, Default: "36500d"}),
	} {
		f.s.SetRetentionPolicy(p)
		if got := c.rows(nil, true); got != all {
			t.Fatalf("%s: %d rows, want %d", name, got, all)
		}
	}
}

// The boundary is the eviction's: kept while the newest row is at most TTL old,
// cut once it is older. The store's Date header has a one-second resolution, so
// the probes sit a few seconds either side.
func TestRetentionCut_BoundaryAtTheTTL(t *testing.T) {
	f := newTenantScopeFixture(t)
	c := &cutFx{f: f}
	all := f.expectedRows(nil, true)
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "48h"}))
	c.now(tsNow().Add(48*time.Hour - 5*time.Second))
	if got := c.rows(nil, true); got != all {
		t.Fatalf("5s before the TTL: %d rows, want %d", got, all)
	}
	c.now(tsNow().Add(48*time.Hour + 5*time.Second))
	if got := c.rows(nil, true); got != 0 {
		t.Fatalf("5s after the TTL: %d rows, want 0", got)
	}
}

// Per-tenant retention: a tenant override materialises as a rule on the file's
// account_id/project_id labels (retention.SynthesizeRules), so one tenant's files
// are cut while another's, and the default's, are not.
func TestRetentionCut_PerTenantRetention(t *testing.T) {
	f := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
	c := &cutFx{f: f}
	now := tsNow()
	putLabelled(f, "0", "0", "0/0/logs/"+tsPartition+"/a.parquet", "svc-default", 3)
	putLabelled(f, "1001", "0", "1001/0/logs/"+tsPartition+"/b.parquet", "svc-short", 5)
	putLabelled(f, "2002", "7", "2002/7/logs/"+tsPartition+"/c.parquet", "svc-long", 2)

	rules := retention.SynthesizeRules([]retention.TenantRetentionEntry{
		{AccountID: 1001, ProjectID: 0, Keep: (7 * 24 * time.Hour).String()},
		{AccountID: 2002, ProjectID: 7, Keep: (90 * 24 * time.Hour).String()},
	})
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "30d", Rules: rules}))

	t1001 := []logstorage.TenantID{{AccountID: 1001}}
	t2002 := []logstorage.TenantID{{AccountID: 2002, ProjectID: 7}}
	t00 := []logstorage.TenantID{{}}

	c.now(now.Add(3 * 24 * time.Hour)) // inside every retention
	if a, b, c := c.rows(t1001, false), c.rows(t2002, false), c.rows(t00, false); a != 5 || b != 2 || c != 3 {
		t.Fatalf("at 3 days every tenant should still be readable: 1001=%d (want 5), 2002:7=%d (want 2), 0:0=%d (want 3)", a, b, c)
	}
	c.now(now.Add(10 * 24 * time.Hour)) // past 1001's 7 days only
	if got := c.rows(t1001, false); got != 0 {
		t.Fatalf("tenant 1001 (7d) at 10 days: %d rows, want 0", got)
	}
	if c.rows(t2002, false) != 2 || c.rows(t00, false) != 3 {
		t.Fatal("tenant 1001's retention cut another tenant's files")
	}
	if got := c.rows(nil, true); got != 5 {
		t.Fatalf("global read at 10 days: %d rows, want 5 (default + 2002)", got)
	}
	c.now(now.Add(40 * 24 * time.Hour)) // past the 30d default as well
	if c.rows(t00, false) != 0 || c.rows(t2002, false) != 2 {
		t.Fatal("at 40 days the default tenant should be cut and the 90d tenant kept")
	}
	c.now(now.Add(100 * 24 * time.Hour))
	if got := c.rows(nil, true); got != 0 {
		t.Fatalf("at 100 days: %d rows, want 0", got)
	}
}

// A keep of zero expires everything it matches at once (what eviction does), and
// only that tenant's files.
func TestRetentionCut_ZeroKeepCutsOnlyItsRule(t *testing.T) {
	f := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
	c := &cutFx{f: f}
	putLabelled(f, "1001", "0", "1001/0/logs/"+tsPartition+"/z.parquet", "svc-zero", 4)
	putLabelled(f, "0", "0", "0/0/logs/"+tsPartition+"/o.parquet", "svc-other", 3)

	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "30d", Rules: []retention.Rule{
		{Match: map[string]string{"account_id": "1001", "project_id": "0"}, Keep: "0s"},
	}}))
	c.now(tsNow().Add(time.Minute))
	if got := c.rows([]logstorage.TenantID{{AccountID: 1001}}, false); got != 0 {
		t.Fatalf("a zero keep left %d rows readable", got)
	}
	if got := c.rows([]logstorage.TenantID{{}}, false); got != 3 {
		t.Fatalf("another tenant lost rows to a zero keep: %d, want 3", got)
	}
}

// The cut judges file ages by the object store's clock, the one the eviction
// uses, not by the pod's own: a pod whose clock is off must not plan a file that
// the pods whose clocks are right have stopped serving.
func TestRetentionCut_UsesTheStoreClock(t *testing.T) {
	f := newTenantScopeFixture(t)
	c := &cutFx{f: f}
	all := f.expectedRows(nil, true)
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "200d"}))
	c.now(tsNow().Add(100 * 24 * time.Hour))
	if got := c.rows(nil, true); got != all {
		t.Fatalf("with a 200 day retention and 100 day old data: %d rows, want %d", got, all)
	}
	// Whatever the host's clock says, the store's clock decides.
	c.now(tsNow().Add(300 * 24 * time.Hour))
	if got := c.rows(nil, true); got != 0 {
		t.Fatalf("with the store's clock 300 days ahead the cut returned %d rows, want 0: it judged by the pod's own clock", got)
	}
}

// The cut on a large plan: nothing expired costs one pass and no allocation;
// when some files are expired the survivors are copied once.
func BenchmarkCutRetention(b *testing.B) {
	s := testStorage()
	s.SetRetentionPolicy(func() *retention.Policy {
		p, err := retention.NewPolicy(retention.Config{Enabled: true, Default: "30d", Rules: []retention.Rule{
			{Match: map[string]string{"account_id": "1"}, Keep: "7d"},
		}})
		if err != nil {
			b.Fatal(err)
		}
		return p
	}())
	now := time.Now()
	mk := func(n int, age time.Duration) []manifest.FileInfo {
		files := make([]manifest.FileInfo, n)
		for i := range files {
			files[i] = manifest.FileInfo{Key: "k", MaxTimeNs: now.Add(-age).UnixNano(), Labels: map[string][]string{"account_id": {"7"}}}
		}
		return files
	}
	b.Run("33600_files_none_expired", func(b *testing.B) {
		files := mk(33600, time.Hour)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = s.cutRetention(files)
		}
	})
	b.Run("33600_files_all_expired", func(b *testing.B) {
		files := mk(33600, 90*24*time.Hour)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = s.cutRetention(files)
		}
	})
}
