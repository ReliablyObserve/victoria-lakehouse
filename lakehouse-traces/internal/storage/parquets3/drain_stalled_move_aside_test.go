package parquets3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// The runbook for a stalled segment (docs/durability.md) ends with a last resort:
// stop the pod, move the segment directory out of insert.buffer_dir, start the
// pod. This pins what that does, so the runbook says only what is true: the
// segments after the stalled one are drained, and the moved segment's rows are
// never written, not even when the directory is moved back later, because by
// then the segments after it have committed and the commit watermark is past it:
// the moved-back directory is restored as committed, held, and deleted once the
// hold is released and its grace has passed.
func TestDrain_MovingAStalledSegmentAside(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, year1965(), 3)
	e.ingest(segTenantA, hourAgo, 2)
	stalled := e.seal()
	e.ingest(segTenantB, hourAgo, 4)
	e.seal()
	f := e.flusher(1000)

	if err := f.drain(context.Background(), stalled); !errors.Is(err, errDrainInvariant) {
		t.Fatalf("the first segment was not stalled: %v", err)
	}
	e.drainAll(f)
	if n := len(e.storedMsgs()); n != 0 {
		t.Fatalf("a segment behind the stalled one was written (%d rows): the drain is not in order", n)
	}
	if v := metrics.BufferStalledSegments.Get(); v != 1 {
		t.Fatalf("lakehouse_buffer_stalled_segments = %d, want 1", v)
	}

	// The operator stops the pod and moves the stalled segment's directory away.
	dir := filepath.Join(e.dir, "buffer", fmt.Sprintf("seg-%016x-%s", stalled.Seq(), stalled.Nonce()))
	aside := filepath.Join(e.dir, "moved-aside")
	e.segs.Close()
	if err := os.Rename(dir, aside); err != nil {
		t.Fatal(err)
	}
	e.newWriter()
	e.open()
	f = e.flusher(1000)
	e.drainAll(f)
	if n := len(e.storedMsgs()); n != 4 {
		t.Fatalf("after moving the stalled segment aside, %d rows are stored, want the 4 of the next segment", n)
	}
	if v := metrics.BufferStalledSegments.Get(); v != 0 {
		t.Errorf("lakehouse_buffer_stalled_segments = %d after the next segment committed, want 0", v)
	}

	// Later the operator moves it back, as the runbook once said it could be.
	e.segs.Close()
	if err := os.Rename(aside, dir); err != nil {
		t.Fatal(err)
	}
	e.newWriter()
	e.open()
	f = e.flusher(1000)
	e.drainAll(f)
	got := e.storedMsgs()
	t.Logf("after moving it back: %d rows stored, committed through %d, stalled segment is %d, pending %d", len(got), f.state.CommittedThroughSeq, stalled.Seq(), len(e.segs.Pending()))
	if len(got) != 4 {
		t.Errorf("%d rows are stored after the segment was moved back; the runbook says nothing re-imports it, so want only the 4 rows of the next segment", len(got))
	}

	// It is restored as a committed segment, so it is held until a complete
	// manifest refresh, served from the buffer meanwhile, and then deleted once
	// its grace has passed: moving the only copy back loses it.
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the moved-back directory is gone before any refresh: %v", err)
	}
	if st := e.segs.Stats(time.Now()); st.Held == 0 {
		t.Errorf("the moved-back segment is not held (held=%d): the runbook says it is restored as committed and held", st.Held)
	}
	f.clock = func() time.Time { return time.Now().Add(time.Hour) }
	f.ManifestRefreshed(time.Now().Add(time.Second))
	if _, err := os.Stat(dir); err == nil {
		t.Errorf("the moved-back segment directory still exists after the hold was released and its grace passed; the runbook says it is deleted")
	}
	if n := len(e.storedMsgs()); n != 4 {
		t.Errorf("%d rows are stored after the moved-back segment was deleted, want 4: nothing re-imports it", n)
	}
}
