package membuffer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// countQuery runs qStr against st for the given tenants and returns the matched
// row count. Shared helper for the hardening tests.
func countQuery(t *testing.T, st *Store, tenants []logstorage.TenantID, qStr string, atTS int64) int64 {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp(qStr, atTS)
	if err != nil {
		t.Fatalf("parse %q: %v", qStr, err)
	}
	var rows atomic.Int64
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, tenants, q, false, nil)
	if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		rows.Add(int64(db.RowsCount()))
	}); err != nil {
		t.Fatalf("runquery %q: %v", qStr, err)
	}
	return rows.Load()
}

func addRow(lr *logstorage.LogRows, tid logstorage.TenantID, ts int64, svc, traceID string) {
	lr.MustAdd(tid, ts, []logstorage.Field{
		{Name: "service.name", Value: svc},
		{Name: "trace_id", Value: traceID},
	}, 1)
}

// TestStore_PersistsAndRestoresAcrossReopen proves the durability story: the
// buffer reuses logstorage's OWN persistence — Close flushes the in-memory
// parts to the data dir, and reopening the same dir restores them. This is why
// no LH WAL is needed (it would duplicate this); the crash-loss window matches
// VT/VL hot.
func TestStore_PersistsAndRestoresAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UnixNano()

	st, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	const n = 5
	for i := 0; i < n; i++ {
		addRow(lr, logstorage.TenantID{}, now, "api-gateway", fmt.Sprintf("t%d", i))
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()
	st.Close() // flushes in-memory parts to the persistent dir

	// Reopen the SAME dir — logstorage restores its parts (no LH WAL involved).
	st2, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if got := countQuery(t, st2, []logstorage.TenantID{{}}, `_stream:{service.name="api-gateway"}`, now); got != n {
		t.Fatalf("after Close+reopen: want %d restored rows, got %d", n, got)
	}
}

func TestStore_EmptyBatchAndLifecycle(t *testing.T) {
	st, err := Open(Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	now := time.Now().UnixNano()

	// Empty LogRows must not panic and must yield nothing.
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()
	st.DebugFlush() // idempotent
	if got := countQuery(t, st, []logstorage.TenantID{{}}, "*", now); got != 0 {
		t.Fatalf("empty store: want 0, got %d", got)
	}
	st.Close() // must not panic
}

func TestStore_OpenErrors(t *testing.T) {
	if _, err := Open(Config{Path: ""}); err == nil {
		t.Fatal("Open with empty Path must error")
	}
}

// TestStore_PathAndGetTenantIDs covers the thin accessors: Path returns the
// configured dir, and GetTenantIDs enumerates the tenants with data in a window
// (reused by the shadow exporter to know which tenants to export).
func TestStore_PathAndGetTenantIDs(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	if st.Path() != dir {
		t.Fatalf("Path() = %q, want %q", st.Path(), dir)
	}

	now := time.Now().UnixNano()
	tA := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	tB := logstorage.TenantID{AccountID: 3, ProjectID: 4}
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	addRow(lr, tA, now, "a", "ta")
	addRow(lr, tB, now, "b", "tb")
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()

	ids, err := st.GetTenantIDs(context.Background(), now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatalf("GetTenantIDs: %v", err)
	}
	seen := map[logstorage.TenantID]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if !seen[tA] || !seen[tB] {
		t.Fatalf("GetTenantIDs missing tenants: got %v, want %v and %v", ids, tA, tB)
	}

	// Empty window → no tenants.
	if got, _ := st.GetTenantIDs(context.Background(), now+int64(time.Hour), now+int64(2*time.Hour)); len(got) != 0 {
		t.Fatalf("GetTenantIDs over empty window: want 0, got %d", len(got))
	}
}

// TestStore_MultiTenantIsolation proves the buffer enforces tenant boundaries
// exactly like the file path — a query for one tenant never sees another's rows.
func TestStore_MultiTenantIsolation(t *testing.T) {
	st, err := Open(Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	now := time.Now().UnixNano()
	tA := logstorage.TenantID{AccountID: 1, ProjectID: 1}
	tB := logstorage.TenantID{AccountID: 2, ProjectID: 2}

	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 4; i++ {
		addRow(lr, tA, now, "svc-a", fmt.Sprintf("a%d", i))
	}
	for i := 0; i < 7; i++ {
		addRow(lr, tB, now, "svc-b", fmt.Sprintf("b%d", i))
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()

	if got := countQuery(t, st, []logstorage.TenantID{tA}, "*", now); got != 4 {
		t.Fatalf("tenant A: want 4, got %d", got)
	}
	if got := countQuery(t, st, []logstorage.TenantID{tB}, "*", now); got != 7 {
		t.Fatalf("tenant B: want 7, got %d", got)
	}
	// Cross-tenant query must not leak.
	if got := countQuery(t, st, []logstorage.TenantID{tA}, `_stream:{service.name="svc-b"}`, now); got != 0 {
		t.Fatalf("tenant A querying svc-b: want 0, got %d", got)
	}
}

// TestStore_ConcurrentAddAndQuery exercises concurrent ingest + query; run with
// -race to catch data races in the dual-write path (logstorage.MustAddRows is
// called from many insert goroutines in production).
func TestStore_ConcurrentAddAndQuery(t *testing.T) {
	st, err := Open(Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	now := time.Now().UnixNano()
	tid := logstorage.TenantID{}

	const goroutines, perG = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
			for i := 0; i < perG; i++ {
				addRow(lr, tid, now, "api-gateway", fmt.Sprintf("g%d-%d", g, i))
			}
			st.MustAddRows(lr)
			logstorage.PutLogRows(lr)
		}(g)
	}
	// Concurrent reader while writers run.
	go func() { _ = countQuery(t, st, []logstorage.TenantID{tid}, "*", now) }()
	wg.Wait()
	st.DebugFlush()

	if got := countQuery(t, st, []logstorage.TenantID{tid}, `_stream:{service.name="api-gateway"}`, now); got != goroutines*perG {
		t.Fatalf("concurrent: want %d, got %d", goroutines*perG, got)
	}
}
