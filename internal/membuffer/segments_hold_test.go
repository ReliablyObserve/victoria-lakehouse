package membuffer

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// A held committed segment is skipped by Reap however old its commit is; the
// ones committed after the hold are not held (#379).
func TestSegments_HoldCommittedOnlyHoldsWhatIsCommittedAtThatPoint(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	tid := logstorage.TenantID{}
	base := time.Now().Add(-time.Hour)
	var segs []*Segment
	for i := 0; i < 3; i++ {
		addRows(s, tid, "x", 1)
		g, ok := s.Seal()
		if !ok {
			t.Fatal("seal failed")
		}
		segs = append(segs, g)
	}
	s.CommitThrough(segs[0].Seq(), base) // restored: committed long ago
	heldAt := time.Now().Add(-10 * time.Minute)
	if n := s.HoldCommitted(heldAt); n != 1 {
		t.Fatalf("HoldCommitted held %d, want 1", n)
	}
	if n := s.HoldCommitted(heldAt); n != 0 {
		t.Fatalf("a second HoldCommitted held %d more, want 0", n)
	}
	s.Commit(segs[1], base) // committed after the hold: not held

	now := time.Now()
	st := s.Stats(now)
	if st.Held != 1 || st.OldestHeldAge < 10*time.Minute || st.OldestHeldAge > 11*time.Minute {
		t.Fatalf("stats held=%d oldest=%v, want 1 and about 10m", st.Held, st.OldestHeldAge)
	}
	if n := s.Reap(now, time.Minute); n != 1 {
		t.Fatalf("Reap removed %d, want 1 (the unheld committed segment only)", n)
	}
	if st := s.Stats(now); st.Committed != 1 || st.Held != 1 {
		t.Fatalf("after Reap committed=%d held=%d, want 1 and 1", st.Committed, st.Held)
	}
	if n := s.ReleaseHeld(); n != 1 {
		t.Fatalf("ReleaseHeld released %d, want 1", n)
	}
	if n := s.ReleaseHeld(); n != 0 {
		t.Fatalf("a second ReleaseHeld released %d, want 0", n)
	}
	if n := s.Reap(now, time.Minute); n != 1 {
		t.Fatalf("Reap after the release removed %d, want 1", n)
	}
	if st := s.Stats(now); st.Held != 0 || st.OldestHeldAge != 0 {
		t.Fatalf("after release held=%d oldest=%v, want 0", st.Held, st.OldestHeldAge)
	}
}
