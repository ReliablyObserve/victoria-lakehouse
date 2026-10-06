package membuffer

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// RowsInWindow counts every tenant's rows of every segment in the window: the
// number a `* | stats count()` over the buffer (all tenants) reports.
func TestSnapshot_RowsInWindow_AllTenantsAllSegmentsWindowed(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()

	addRows(s, logstorage.TenantID{}, "a", 5)
	addRows(s, logstorage.TenantID{AccountID: 1}, "b", 3)
	if _, ok := s.Seal(); !ok {
		t.Fatal("seal")
	}
	addRows(s, logstorage.TenantID{AccountID: 7, ProjectID: 2}, "c", 4)
	s.DebugFlush()

	snap := s.Snapshot()
	defer snap.Release()
	now := time.Now().UnixNano()
	got, err := snap.RowsInWindow(context.Background(), now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got != 12 {
		t.Fatalf("rows=%d, want 12 (5+3 sealed, 4 active, three tenants)", got)
	}

	// A window that ends before the rows were written counts none of them.
	got, err = snap.RowsInWindow(context.Background(), now-3*int64(time.Hour), now-2*int64(time.Hour))
	if err != nil || got != 0 {
		t.Fatalf("old window: rows=%d err=%v, want 0", got, err)
	}
}

func TestSnapshot_RowsInWindow_EmptyBuffer(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	snap := s.Snapshot()
	defer snap.Release()
	got, err := snap.RowsInWindow(context.Background(), 0, time.Now().UnixNano())
	if err != nil || got != 0 {
		t.Fatalf("rows=%d err=%v, want 0", got, err)
	}
}

// SegmentRowsInWindow splits the count per segment and carries each segment's
// committed state: the parity check treats a committed segment's rows as
// exactly its objects' rows, an uncommitted one's as possibly more.
func TestSnapshot_SegmentRowsInWindow_PerSegmentWithCommittedState(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()

	addRows(s, logstorage.TenantID{}, "a", 5)
	addRows(s, logstorage.TenantID{AccountID: 1}, "b", 3)
	sealed, ok := s.Seal()
	if !ok {
		t.Fatal("seal")
	}
	s.Commit(sealed, time.Now())
	addRows(s, logstorage.TenantID{AccountID: 7}, "c", 4)
	s.DebugFlush()

	snap := s.Snapshot()
	defer snap.Release()
	now := time.Now().UnixNano()
	got, err := snap.SegmentRowsInWindow(context.Background(), now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("segments=%v, want 2", got)
	}
	for _, g := range got {
		switch g.Nonce {
		case sealed.Nonce():
			if g.Rows != 8 || !g.Committed {
				t.Errorf("sealed = %+v, want 8 rows committed", g)
			}
		default:
			if g.Rows != 4 || g.Committed {
				t.Errorf("active = %+v, want 4 rows uncommitted", g)
			}
		}
	}
	// A window with no rows counts none, per segment.
	got, err = snap.SegmentRowsInWindow(context.Background(), now-3*int64(time.Hour), now-2*int64(time.Hour))
	if err != nil || len(got) != 2 || got[0].Rows+got[1].Rows != 0 {
		t.Fatalf("old window: %v err=%v", got, err)
	}
}
