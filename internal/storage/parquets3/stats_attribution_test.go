package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestAttributeLogStats_SingleTenant(t *testing.T) {
	rows := []schema.LogRow{
		{AccountID: 0, ProjectID: 0},
		{AccountID: 0, ProjectID: 0},
		{AccountID: 0, ProjectID: 0},
	}
	var calls int
	var seen tenantKey
	var c, r, n int64
	cb := func(a, p uint32, comp, raw, rows int64, _ string) {
		calls++
		seen = tenantKey{a, p}
		c, r, n = comp, raw, rows
	}
	attributeLogStats(cb, rows, 300, 600)
	if calls != 1 {
		t.Fatalf("expected 1 callback for single tenant, got %d", calls)
	}
	if seen != (tenantKey{0, 0}) {
		t.Errorf("seen tenant %+v, want {0,0}", seen)
	}
	if c != 300 || r != 600 || n != 3 {
		t.Errorf("got compressed=%d raw=%d rows=%d, want 300/600/3", c, r, n)
	}
}

func TestAttributeLogStats_MultiTenant_BytesConserved(t *testing.T) {
	rows := []schema.LogRow{
		{AccountID: 1, ProjectID: 1},
		{AccountID: 1, ProjectID: 1},
		{AccountID: 1, ProjectID: 1},
		{AccountID: 1, ProjectID: 1},
		{AccountID: 1001, ProjectID: 0},
		{AccountID: 1001, ProjectID: 0},
		{AccountID: 1001, ProjectID: 0},
		{AccountID: 1001, ProjectID: 0},
		{AccountID: 1001, ProjectID: 0},
		{AccountID: 1001, ProjectID: 0},
	}
	var totalC, totalR, totalRows int64
	tenantsSeen := map[tenantKey]bool{}
	cb := func(a, p uint32, comp, raw, rs int64, _ string) {
		totalC += comp
		totalR += raw
		totalRows += rs
		tenantsSeen[tenantKey{a, p}] = true
	}
	const wantC, wantR int64 = 1000, 2000
	attributeLogStats(cb, rows, wantC, wantR)

	if totalC != wantC {
		t.Errorf("compressed bytes %d, want %d (must be conserved across tenants)", totalC, wantC)
	}
	if totalR != wantR {
		t.Errorf("raw bytes %d, want %d (must be conserved across tenants)", totalR, wantR)
	}
	if totalRows != int64(len(rows)) {
		t.Errorf("rows %d, want %d", totalRows, len(rows))
	}
	if len(tenantsSeen) != 2 {
		t.Errorf("expected 2 distinct tenants seen, got %d", len(tenantsSeen))
	}
}

func TestAttributeLogStats_EmptyRows(t *testing.T) {
	var called bool
	cb := func(_, _ uint32, _, _, _ int64, _ string) { called = true }
	attributeLogStats(cb, nil, 100, 200)
	if called {
		t.Error("callback should not fire for empty rows")
	}
}

func TestAttributeTraceStats_SingleTenant(t *testing.T) {
	rows := []schema.TraceRow{
		{AccountID: 42, ProjectID: 7},
		{AccountID: 42, ProjectID: 7},
	}
	var calls int
	cb := func(a, p uint32, comp, raw, rs int64, _ string) {
		calls++
		if a != 42 || p != 7 || comp != 100 || raw != 200 || rs != 2 {
			t.Errorf("unexpected callback args: a=%d p=%d c=%d r=%d rs=%d", a, p, comp, raw, rs)
		}
	}
	attributeTraceStats(cb, rows, 100, 200)
	if calls != 1 {
		t.Fatalf("expected 1 callback, got %d", calls)
	}
}
