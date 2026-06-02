package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestGroupLogRowsByTenant_SingleTenant_FastPath(t *testing.T) {
	rows := []schema.LogRow{
		{AccountID: 7, ProjectID: 3},
		{AccountID: 7, ProjectID: 3},
		{AccountID: 7, ProjectID: 3},
	}
	groups := groupLogRowsByTenant(rows)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].AccountID != 7 || groups[0].ProjectID != 3 {
		t.Errorf("group tenant = %d:%d, want 7:3", groups[0].AccountID, groups[0].ProjectID)
	}
	if &groups[0].Rows[0] != &rows[0] {
		t.Error("single-tenant fast path should share the input slice (no copy)")
	}
}

func TestGroupLogRowsByTenant_MultiTenant_PreservesRowsAndOrders(t *testing.T) {
	rows := []schema.LogRow{
		{AccountID: 1001, ProjectID: 0, TimestampUnixNano: 1},
		{AccountID: 1, ProjectID: 1, TimestampUnixNano: 2},
		{AccountID: 1001, ProjectID: 0, TimestampUnixNano: 3},
		{AccountID: 1, ProjectID: 1, TimestampUnixNano: 4},
		{AccountID: 1, ProjectID: 1, TimestampUnixNano: 5},
	}
	groups := groupLogRowsByTenant(rows)
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	// Deterministic order: smaller account first.
	if groups[0].AccountID != 1 || groups[1].AccountID != 1001 {
		t.Errorf("group order broken: got %d then %d, want 1 then 1001",
			groups[0].AccountID, groups[1].AccountID)
	}
	var total int
	for _, g := range groups {
		total += len(g.Rows)
		a, p := g.AccountID, g.ProjectID
		for _, r := range g.Rows {
			if r.AccountID != a || r.ProjectID != p {
				t.Errorf("row %d:%d ended up in group %d:%d", r.AccountID, r.ProjectID, a, p)
			}
		}
	}
	if total != len(rows) {
		t.Errorf("total grouped rows = %d, want %d (no rows must be dropped)", total, len(rows))
	}
}

func TestGroupLogRowsByTenant_Empty(t *testing.T) {
	if g := groupLogRowsByTenant(nil); g != nil {
		t.Errorf("nil input should return nil, got %+v", g)
	}
	if g := groupLogRowsByTenant([]schema.LogRow{}); g != nil {
		t.Errorf("empty slice should return nil, got %+v", g)
	}
}

func TestGroupTraceRowsByTenant_MultiTenant_DeterministicOrder(t *testing.T) {
	rows := []schema.TraceRow{
		{AccountID: 9, ProjectID: 9},
		{AccountID: 1, ProjectID: 1},
		{AccountID: 9, ProjectID: 9},
		{AccountID: 1, ProjectID: 1},
	}
	g1 := groupTraceRowsByTenant(rows)
	g2 := groupTraceRowsByTenant(rows)
	if len(g1) != len(g2) {
		t.Fatalf("group counts differ: %d vs %d", len(g1), len(g2))
	}
	for i := range g1 {
		if g1[i].AccountID != g2[i].AccountID || g1[i].ProjectID != g2[i].ProjectID {
			t.Errorf("group %d not deterministic: %v vs %v", i, g1[i], g2[i])
		}
	}
}
