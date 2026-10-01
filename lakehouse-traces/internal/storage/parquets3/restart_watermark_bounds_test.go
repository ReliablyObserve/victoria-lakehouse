package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
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
			wm := r.s.bufferWatermarksFor(context.Background(), files)[logstorage.TenantID{}]
			if wm != last.UnixNano() {
				t.Errorf("watermark = %s, want the newest cold row %s", time.Unix(0, wm).UTC(), last.UTC())
			}
			if cur, _ := r.s.manifest.GetFileByKey(files[0].Key); cur.BoundsInferred || cur.MaxTimeNs != last.UnixNano() {
				t.Errorf("manifest after resolving: %+v, want exact bounds", cur)
			}
		})
	}
}

// The startup warmup resolves recent inferred bounds before the first query.
func TestRestartWatermark_WarmMetadataResolvesRecentInferredBounds(t *testing.T) {
	now := time.Now().UTC()
	hour := now.Truncate(time.Hour)
	r := newRWRig(t)
	last := hour.Add(time.Second)
	r.ingest("COLD", hour, last)
	r.restart(true, false)
	if f := r.s.manifest.GetFilesForRange(hour.UnixNano(), hour.Add(time.Hour).UnixNano()); len(f) != 1 || !f[0].BoundsInferred {
		t.Fatalf("precondition: want one object with inferred bounds, got %+v", f)
	}
	if n := r.s.enrichRecentInferredBounds(context.Background()); n != 1 {
		t.Fatalf("enrichRecentInferredBounds resolved %d objects, want 1", n)
	}
	f := r.s.manifest.GetFilesForRange(hour.UnixNano(), hour.Add(time.Hour).UnixNano())
	if f[0].BoundsInferred || f[0].MaxTimeNs != last.UnixNano() {
		t.Errorf("after warmup: %+v, want exact MaxTimeNs %d", f[0], last.UnixNano())
	}
}

// An object whose bounds cannot be resolved (here: it is gone from the bucket)
// stays out of the watermark instead of contributing the hour's end.
func TestRestartWatermark_UnresolvableObjectIsLeftOutOfTheWatermark(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	files := r.s.manifest.GetFilesForRange(rwHour.UnixNano(), rwHour.Add(time.Hour).UnixNano())
	if len(files) != 1 || !files[0].BoundsInferred {
		t.Fatalf("precondition: %+v", files)
	}
	r.mock.mu.Lock()
	delete(r.mock.files, files[0].Key)
	r.mock.mu.Unlock()
	if wm := r.s.bufferWatermarksFor(context.Background(), files); wm[logstorage.TenantID{}] != 0 {
		t.Errorf("watermark = %d for an object with unresolvable bounds, want none (never the hour's end)", wm[logstorage.TenantID{}])
	}
}

// Unit: the watermark ignores inferred bounds and uses exact ones.
func TestBufferWatermarksFor_IgnoresInferredBounds(t *testing.T) {
	f := newTenantScopeFixture(t)
	hourEnd := rwHour.Add(time.Hour).UnixNano() - 1
	files := []manifest.FileInfo{
		{Key: "0/0/logs/" + tsPartition + "/exact.parquet", MaxTimeNs: 500},
		{Key: "0/0/logs/" + tsPartition + "/inferred.parquet", MinTimeNs: 1, MaxTimeNs: hourEnd, BoundsInferred: true},
		{Key: "1001/0/logs/" + tsPartition + "/only-inferred.parquet", MinTimeNs: 1, MaxTimeNs: hourEnd, BoundsInferred: true},
	}
	wm := f.s.bufferWatermarksFor(context.Background(), files)
	if got := wm[logstorage.TenantID{}]; got != 500 {
		t.Errorf("0:0 watermark = %d, want 500 (the inferred object must not raise it)", got)
	}
	if got, ok := wm[logstorage.TenantID{AccountID: 1001}]; ok && got != 0 {
		t.Errorf("1001:0 watermark = %d, want none: its only object has inferred bounds", got)
	}
}
