package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Inferred-bounds handling on the read path (see restart_watermark_test.go for
// the restart scenarios these support).
//
// Twin of internal/storage/parquets3/restart_watermark_bounds_test.go (the logs module).

// After the fix's second snapshot the restart loads exact bounds; without it the
// listing yields inferred ones until the footer resolves them.
func TestRestartWatermark_SnapshotAfterFlushLoadsExactBounds(t *testing.T) {
	last := at(rwHour, 10*time.Minute+2*time.Second)
	for _, tc := range []struct {
		name          string
		snapshotAfter bool
	}{{"snapshot persisted after the final flush", true}, {"snapshot persisted before it only", false}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRWRig(t)
			r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), last)
			r.restart(true, tc.snapshotAfter)
			files := r.s.manifest.GetFilesForRange(rwHour.UnixNano(), rwHour.Add(time.Hour).UnixNano())
			if len(files) != 1 {
				t.Fatalf("manifest holds %d objects, want 1", len(files))
			}
			if tc.snapshotAfter {
				if files[0].BoundsInferred || files[0].MaxTimeNs != last.UnixNano() {
					t.Errorf("restart from the post-flush snapshot: %+v, want exact MaxTimeNs %d", files[0], last.UnixNano())
				}
				return
			}
			if !files[0].BoundsInferred {
				t.Fatalf("restart from the pre-flush snapshot: %+v, want the listing's inferred bounds", files[0])
			}
			// The watermark resolves the exact bound from the footer first.
			wm := r.s.bufferWatermarksFor(context.Background(), 0, files)[logstorage.TenantID{}]
			if wm != last.UnixNano() {
				t.Errorf("watermark = %s, want the newest cold row %s", time.Unix(0, wm).UTC(), last.UTC())
			}
			if cur, _ := r.s.manifest.GetFileByKey(files[0].Key); cur.BoundsInferred || cur.MaxTimeNs != last.UnixNano() {
				t.Errorf("manifest after resolving: %+v, want exact bounds", cur)
			}
		})
	}
}

// Unit: the watermark raised by exact bounds, an inferred object that cannot
// change it is ignored without a read, and one that can but cannot be resolved
// contributes its inferred end (never nothing: that would double count).
func TestBufferWatermarksFor_InferredObjects(t *testing.T) {
	rwFreezeClock(t)
	f := newTenantScopeFixture(t)
	hourEnd := rwHour.Add(time.Hour).UnixNano() - 1
	files := []manifest.FileInfo{
		{Key: "0/0/logs/" + tsPartition + "/exact.parquet", MaxTimeNs: hourEnd - 1000},
		{Key: "0/0/logs/" + tsPartition + "/inferred-below.parquet", MinTimeNs: 1, MaxTimeNs: hourEnd - 2000, BoundsInferred: true},
		{Key: "1001/0/logs/" + tsPartition + "/only-inferred.parquet", MinTimeNs: 1, MaxTimeNs: hourEnd, BoundsInferred: true},
	}
	unresolved0 := metrics.WatermarkInferredUnresolved.Get()
	wm := f.s.bufferWatermarksFor(context.Background(), 0, files)
	if got := wm[logstorage.TenantID{}]; got != hourEnd-1000 {
		t.Errorf("0:0 watermark = %d, want %d: the inferred object below the exact one must not raise it", got, hourEnd-1000)
	}
	if got := wm[logstorage.TenantID{AccountID: 1001}]; got != hourEnd {
		t.Errorf("1001:0 watermark = %d, want its unresolved object's inferred end %d (hidden, never double counted)", got, hourEnd)
	}
	if d := metrics.WatermarkInferredUnresolved.Get() - unresolved0; d != 1 {
		t.Errorf("unresolved counter moved by %d, want 1", d)
	}
}
