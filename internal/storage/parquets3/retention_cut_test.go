package parquets3

import (
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
// (internal/retention.Policy: upstream's day floor for a TTL of a day or more,
// an exact age for a shorter one). The fixture's rows were written at tsNow();
// each test sets the clock the cut reads, so the result does not depend on the
// host's date.

func mustRetention(t *testing.T, cfg retention.Config) *retention.Policy {
	t.Helper()
	p, err := retention.NewPolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// cutNow sets the clock the cut reads (the same injectable clock the startup
// pass uses) for the rest of the test.
func cutNow(t *testing.T, at time.Time) {
	t.Helper()
	prev := nowFn
	nowFn = func() time.Time { return at }
	t.Cleanup(func() { nowFn = prev })
}

// rowsOf is the number of rows a query of the tenants (or a global read) returns.
func rowsOf(f *tsFixture, tenants []logstorage.TenantID, global bool) int {
	n, _ := f.runQuery(f.ctx(global), tenants, "*")
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
// eviction loop got to it; a file inside it is read before and after. The
// fixture's rows were written on 2026-05-10 14:30 UTC.
func TestRetentionCut_PlansExcludeExpiredFiles(t *testing.T) {
	f := newTenantScopeFixture(t)
	all := f.expectedRows(nil, true)
	if all == 0 {
		t.Fatal("fixture has no rows")
	}
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "30d"}))

	cutNow(t, tsNow().Add(29*24*time.Hour))
	if got := rowsOf(f, nil, true); got != all {
		t.Fatalf("a file inside its retention: %d rows, want %d", got, all)
	}
	cutNow(t, tsNow().Add(31*24*time.Hour+12*time.Hour))
	if got := rowsOf(f, nil, true); got != 0 {
		t.Fatalf("a file past its retention was read: %d rows, want 0", got)
	}

	// No policy, a disabled policy and a 100-year keep cut nothing, whatever the age.
	for name, p := range map[string]*retention.Policy{
		"no policy":       nil,
		"retention off":   mustRetention(t, retention.Config{}),
		"a 100-year keep": mustRetention(t, retention.Config{Enabled: true, Default: "36500d"}),
	} {
		f.s.SetRetentionPolicy(p)
		if got := rowsOf(f, nil, true); got != all {
			t.Fatalf("%s: %d rows, want %d", name, got, all)
		}
	}
}

// The boundary is upstream's day floor: a file stays until the day of its newest
// row is before the day of now-TTL, so it is not hidden up to 23 hours early. With
// a 48h TTL and rows from 2026-05-10 14:30, the floor day is May 10 for every
// "now" through May 12 23:59:59, and May 11 from May 13 00:00:00.
func TestRetentionCut_DayFloorBoundary(t *testing.T) {
	f := newTenantScopeFixture(t)
	all := f.expectedRows(nil, true)
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "48h"}))

	cutNow(t, tsNow().Add(48*time.Hour+5*time.Hour)) // 5h past the TTL by the clock, same floor day
	if got := rowsOf(f, nil, true); got != all {
		t.Fatalf("5h past the TTL but inside the day floor: %d rows, want %d (the exact rule would have cut it)", got, all)
	}
	cutNow(t, time.Date(2026, 5, 12, 23, 59, 59, 0, time.UTC))
	if got := rowsOf(f, nil, true); got != all {
		t.Fatalf("one second before the floor moves: %d rows, want %d", got, all)
	}
	cutNow(t, time.Date(2026, 5, 13, 0, 0, 1, 0, time.UTC))
	if got := rowsOf(f, nil, true); got != 0 {
		t.Fatalf("one second after the floor moved past the file's day: %d rows, want 0", got)
	}
}

// Per-tenant retention: a tenant override materialises as a rule on the file's
// account_id/project_id labels (retention.SynthesizeRules), so one tenant's files
// are cut while another's, and the default's, are not.
func TestRetentionCut_PerTenantRetention(t *testing.T) {
	f := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
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
	now := tsNow()

	cutNow(t, now.Add(3*24*time.Hour)) // inside every retention
	if a, b, c := rowsOf(f, t1001, false), rowsOf(f, t2002, false), rowsOf(f, t00, false); a != 5 || b != 2 || c != 3 {
		t.Fatalf("at 3 days every tenant should still be readable: 1001=%d (want 5), 2002:7=%d (want 2), 0:0=%d (want 3)", a, b, c)
	}
	cutNow(t, now.Add(10*24*time.Hour)) // past 1001's 7 days only
	if got := rowsOf(f, t1001, false); got != 0 {
		t.Fatalf("tenant 1001 (7d) at 10 days: %d rows, want 0", got)
	}
	if rowsOf(f, t2002, false) != 2 || rowsOf(f, t00, false) != 3 {
		t.Fatal("tenant 1001's retention cut another tenant's files")
	}
	if got := rowsOf(f, nil, true); got != 5 {
		t.Fatalf("global read at 10 days: %d rows, want 5 (default + 2002)", got)
	}
	cutNow(t, now.Add(40*24*time.Hour)) // past the 30d default as well
	if rowsOf(f, t00, false) != 0 || rowsOf(f, t2002, false) != 2 {
		t.Fatal("at 40 days the default tenant should be cut and the 90d tenant kept")
	}
	cutNow(t, now.Add(100*24*time.Hour))
	if got := rowsOf(f, nil, true); got != 0 {
		t.Fatalf("at 100 days: %d rows, want 0", got)
	}
}

// A keep of zero (below a day: an exact rule, a Lakehouse extension) expires
// everything it matches at once, and only that tenant's files.
func TestRetentionCut_ZeroKeepCutsOnlyItsRule(t *testing.T) {
	f := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
	putLabelled(f, "1001", "0", "1001/0/logs/"+tsPartition+"/z.parquet", "svc-zero", 4)
	putLabelled(f, "0", "0", "0/0/logs/"+tsPartition+"/o.parquet", "svc-other", 3)

	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "30d", Rules: []retention.Rule{
		{Match: map[string]string{"account_id": "1001", "project_id": "0"}, Keep: "0s"},
	}}))
	cutNow(t, tsNow().Add(time.Minute))
	if got := rowsOf(f, []logstorage.TenantID{{AccountID: 1001}}, false); got != 0 {
		t.Fatalf("a zero keep left %d rows readable", got)
	}
	if got := rowsOf(f, []logstorage.TenantID{{}}, false); got != 3 {
		t.Fatalf("another tenant lost rows to a zero keep: %d, want 3", got)
	}
}

// The cut reads the injected clock, not the host's: the same plan is cut or not
// by the time the test says it is.
func TestRetentionCut_UsesTheInjectedClock(t *testing.T) {
	f := newTenantScopeFixture(t)
	all := f.expectedRows(nil, true)
	f.s.SetRetentionPolicy(mustRetention(t, retention.Config{Enabled: true, Default: "200d"}))
	cutNow(t, tsNow().Add(100*24*time.Hour))
	if got := rowsOf(f, nil, true); got != all {
		t.Fatalf("with a 200 day retention and 100 day old data: %d rows, want %d", got, all)
	}
	cutNow(t, tsNow().Add(300*24*time.Hour))
	if got := rowsOf(f, nil, true); got != 0 {
		t.Fatalf("with the clock 300 days ahead the cut returned %d rows, want 0", got)
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
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = s.cutRetention(files)
		}
	})
	b.Run("33600_files_all_expired", func(b *testing.B) {
		files := mk(33600, 90*24*time.Hour)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = s.cutRetention(files)
		}
	})
}
