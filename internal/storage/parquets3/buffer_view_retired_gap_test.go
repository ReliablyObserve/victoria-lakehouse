package parquets3

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestBufferView_RetiredBeforeTheSelectPodListed_KnownGap391 pins a known gap,
// issue #391 (registry lh.durability.select_pod_lists_after_segment_retired_*,
// expect: differ). It asserts today's behaviour, and the PR that fixes #391
// inverts it: the rows must then stay visible throughout.
//
// An insert pod keeps a committed segment readable for its grace, which it
// computes from its own manifest.refresh_interval, and a select pod drops the
// segment's objects only while the insert pod still serves the segment through
// the buffer bridge. If the select pod has not listed the segment's objects
// when the insert pod retires the segment (its refresh interval is longer, or
// its listing is slow or failing), the segment's acknowledged rows are missing
// from the select pod's answers until its next complete listing: exactly that
// segment's rows, with HTTP 200, and nothing counts it.
//
// Two pods over one bucket: the insert pod (segments, flusher, writer) and a
// select pod that reads it through the real /internal/buffer/query handler.
func TestBufferView_RetiredBeforeTheSelectPodListed_KnownGap391(t *testing.T) {
	e := newViewEnv(t) // the insert pod
	peer := httptest.NewServer(buffer.NewHandler(BridgeSource{Segments: e.segs}, ""))
	t.Cleanup(peer.Close)

	sel := testStorageWithS3(t, e.srv.url()) // the select pod
	sel.cfg.Mode = config.ModeLogs
	sel.bufferBridge = NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 5 * time.Second}, config.ModeLogs)
	sel.bufferBridge.SetEndpoints([]string{peer.URL})
	listing := func() {
		t.Helper()
		if err := sel.manifest.RefreshFromS3(context.Background(), sel.pool.S3Client()); err != nil {
			t.Fatal(err)
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
	e.exactOn(sel, "committed, the insert pod still serves the segment")

	// The insert pod's grace passes before the select pod lists again.
	if n := e.segs.Reap(time.Now().Add(time.Hour), time.Minute); n != 1 {
		t.Fatalf("reaped %d segments, want 1", n)
	}
	got := e.answerOn(sel)
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
		t.Errorf("between the reap and the select pod's next listing: %d of the retired segment's 20 rows missing (want all 20: the known gap of #391), other wrong counts %v, %d distinct rows answered (want the 5 active ones)", len(missing), head(other, 6), len(got))
	}

	listing()
	e.exactOn(sel, "after the select pod's next listing")
}
