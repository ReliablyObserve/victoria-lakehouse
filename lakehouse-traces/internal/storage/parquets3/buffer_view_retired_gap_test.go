package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestBufferView_RetiredBeforeTheSelectPodListed_KnownGap391 is the traces
// binary's twin of the logs test of the same name. It pins a known gap, issue
// #391 (registry lh.durability.select_pod_lists_after_segment_retired_*,
// expect: differ). It asserts today's behaviour, and the PR that fixes #391
// inverts it: the spans must then stay visible throughout.
//
// An insert pod keeps a committed segment readable for its grace, computed from
// its own manifest.refresh_interval; a select pod drops the segment's objects
// only while the insert pod still serves the segment through the buffer bridge.
// If the select pod has not listed the objects when the segment is retired, the
// segment's acknowledged spans are missing from its answers (and so from the
// Jaeger and Tempo reads built on them) until its next complete listing.
//
// Two pods over one bucket: the insert pod and a select pod that reads it
// through the real /internal/buffer/query route.
func TestBufferView_RetiredBeforeTheSelectPodListed_KnownGap391(t *testing.T) {
	e := newViewEnv(t) // the insert pod
	peer := authInsertPod(t, e, "", false)
	sel := authSelectPod(t, e, "", peer.URL)
	sel.manifest = manifest.New("test-bucket", "traces/") // the select pod lists the prefix the insert pod writes
	listing := func() {
		t.Helper()
		if err := sel.manifest.RefreshFromS3(context.Background(), sel.pool.S3Client()); err != nil {
			t.Fatal(err)
		}
	}
	tenants := []logstorage.TenantID{{}}
	exactOn := func(when string) {
		t.Helper()
		got := msgCounts(t, sel, tenants, false)
		var problems []string
		for i := 1; i <= e.n; i++ {
			if k := fmt.Sprintf("row-%d", i); got[k] != 1 {
				problems = append(problems, fmt.Sprintf("%s×%d", k, got[k]))
			}
		}
		if len(got) != e.n || len(problems) > 0 {
			t.Errorf("%s: %d distinct spans, want %d; wrong counts: %v", when, len(got), e.n, head(problems, 6))
		}
	}

	base := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	listing()                              // the select pod's last listing, before the segment's objects exist
	e.ingest(base.Add(10*time.Minute), 20) // row-1 .. row-20: the segment that is retired
	g, _ := e.segs.Seal()
	if err := e.f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	e.ingest(base.Add(20*time.Minute), 5) // row-21 .. row-25: still in the active segment
	exactOn("committed, the insert pod still serves the segment")

	// The insert pod's grace passes before the select pod lists again.
	if n := e.segs.Reap(time.Now().Add(time.Hour), time.Minute); n != 1 {
		t.Fatalf("reaped %d segments, want 1", n)
	}
	got := msgCounts(t, sel, tenants, false)
	var missing, other []string
	for i := 1; i <= e.n; i++ {
		k := fmt.Sprintf("row-%d", i)
		switch {
		case i <= 20 && got[k] == 0:
			missing = append(missing, k)
		case got[k] != 1:
			other = append(other, fmt.Sprintf("%s×%d", k, got[k]))
		}
	}
	if len(missing) != 20 || len(other) != 0 || len(got) != 5 {
		t.Errorf("between the reap and the select pod's next listing: %d of the retired segment's 20 spans missing (want all 20: the known gap of #391), other wrong counts %v, %d distinct spans answered (want the 5 active ones)", len(missing), head(other, 6), len(got))
	}

	listing()
	exactOn("after the select pod's next listing")
}
