package compaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fakeMarkers answers the marker listing of an insert pod's committed buffer
// segments: marker key -> the object store's LastModified.
type fakeMarkers struct {
	mu      sync.Mutex
	markers map[string]time.Time
	err     error
	listed  []string // the prefixes asked
}

func (f *fakeMarkers) ListModTimes(_ context.Context, prefix string) (map[string]time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed = append(f.listed, prefix)
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]time.Time{}
	for k, v := range f.markers {
		out[k] = v
	}
	return out, nil
}

// segmentFixture is a partition of 12 small level-0 objects, each drained from
// its own buffer segment (the batch id carries the segment's nonce).
type segmentFixture struct {
	pool    *mockPool
	m       *manifest.Manifest
	sched   *Scheduler
	nonces  []string
	markers *fakeMarkers
}

const segPartition = "dt=2026-01-01/hour=00"

func newSegmentFixture(t *testing.T, withGuard bool) *segmentFixture {
	t.Helper()
	f := &segmentFixture{pool: newMockPool(), m: manifest.New("test-bucket", "logs/"), markers: &fakeMarkers{markers: map[string]time.Time{}}}
	created := time.Now().Add(-time.Hour)
	for i := 0; i < 12; i++ {
		nonce := fmt.Sprintf("%08x%08x", uint32(created.Unix()), uint32(i+1))
		f.nonces = append(f.nonces, nonce)
		data := makeTestParquet(t, []schema.LogRow{{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"}})
		key := fmt.Sprintf("logs/%s/%s-0.parquet", segPartition, nonce)
		if err := f.pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		f.m.AddFile(segPartition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1,
			MinTimeNs: int64(i*1000 + 1), MaxTimeNs: int64(i*1000 + 1), SchemaFingerprint: "fp"})
	}
	f.sched = NewScheduler(SchedulerConfig{
		Manifest: f.m, Pool: f.pool, Ownership: soleOwnerResolver(), Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute, MaxConcurrent: 2,
		RowGroupSize: 1000, CompressionLevel: 7,
	})
	if withGuard {
		f.sched.SetSegmentGuard(f.markers, 5*time.Minute)
	}
	return f
}

func (f *segmentFixture) commit(age time.Duration) {
	for _, n := range f.nonces {
		f.markers.markers["logs/_segments/"+n] = time.Now().Add(-age)
	}
}

// Objects of buffer segments that may still be served from their insert pod are
// not compacted: merged into an object without the segment's nonce, their rows
// would be answered twice. They are compacted once the segment is committed and
// its protection has passed.
func TestScheduler_LeavesTheObjectsOfLiveBufferSegmentsAlone(t *testing.T) {
	for _, tc := range []struct {
		name     string
		guard    bool
		setup    func(f *segmentFixture)
		wantComp int
	}{
		{"no guard configured", false, func(*segmentFixture) {}, 0},
		{"no marker yet", true, func(*segmentFixture) {}, 0},
		{"committed a moment ago", true, func(f *segmentFixture) { f.commit(time.Minute) }, 0},
		{"marker listing fails", true, func(f *segmentFixture) { f.commit(time.Hour); f.markers.err = errors.New("list: boom") }, 0},
		{"committed and protected long enough", true, func(f *segmentFixture) { f.commit(time.Hour) }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSegmentFixture(t, tc.guard)
			tc.setup(f)
			errs0 := metrics.CompactionSegmentGuardErrors.Get()
			n, err := f.sched.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if n != tc.wantComp {
				t.Fatalf("%d compactions, want %d", n, tc.wantComp)
			}
			files := len(f.m.FilesForPartition(segPartition))
			if tc.wantComp == 0 && files != 12 {
				t.Errorf("%d objects left, want all 12 untouched", files)
			}
			if tc.wantComp == 1 && files != 1 {
				t.Errorf("%d objects left, want the compacted one", files)
			}
			if tc.guard && len(f.markers.listed) == 0 {
				t.Error("the markers were not listed")
			} else if len(f.markers.listed) > 0 && f.markers.listed[0] != "logs/_segments/" {
				t.Errorf("listed %q, want logs/_segments/", f.markers.listed[0])
			}
			if f.markers.err != nil && metrics.CompactionSegmentGuardErrors.Get() != errs0+1 {
				t.Error("a failed marker listing was not counted")
			}
		})
	}
}

// Markers are no longer needed once their segment is older than the release age
// (its objects are free whatever the marker says); the scan deletes them,
// a bounded number at a time, and keeps the recent ones.
func TestScheduler_DeletesMarkersOfSegmentsPastTheReleaseAge(t *testing.T) {
	f := newSegmentFixture(t, true)
	oldKey, freshKey := "logs/_segments/old", "logs/_segments/fresh"
	f.markers.markers[oldKey] = time.Now().Add(-(manifest.SegmentReleaseAfter + 25*time.Hour))
	f.markers.markers[freshKey] = time.Now().Add(-time.Hour)
	_ = f.pool.Upload(context.Background(), oldKey, []byte("{}"))
	_ = f.pool.Upload(context.Background(), freshKey, []byte("{}"))

	if _, err := f.sched.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d, _ := f.pool.Download(context.Background(), oldKey); d != nil {
		t.Error("the marker of a segment past the release age was not deleted")
	}
	if d, _ := f.pool.Download(context.Background(), freshKey); d == nil {
		t.Error("a recent marker was deleted")
	}
}
