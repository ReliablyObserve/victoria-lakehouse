package delete

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// extractPartition is the manifest partition (the dt=/hour= segments) of an
// object key.
func extractPartition(key string) string {
	var parts []string
	for _, p := range strings.Split(key, "/") {
		if strings.Contains(p, "=") {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, "/")
}

// newTestManifest returns a manifest that already registers every key given,
// with the declared row counts.
func newTestManifest(t *testing.T, rowsByKey map[string]int64) *manifest.Manifest {
	t.Helper()
	m := manifest.New("test-bucket", "")
	keys := make([]string, 0, len(rowsByKey))
	for k := range rowsByKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	objects := make([]manifest.ListedObject, 0, len(keys))
	for _, k := range keys {
		m.AddFile(extractPartition(k), manifest.FileInfo{
			Key:       k,
			Size:      1024,
			RowCount:  rowsByKey[k],
			MinTimeNs: 1,
			MaxTimeNs: 1 << 40,
		})
		objects = append(objects, manifest.ListedObject{Key: k, Size: 1024})
	}
	if !m.ApplyListing(objects, time.Now()) {
		t.Fatal("fixture: the listing was rejected")
	}
	return m
}

// scopedManifest is the manifest as the binaries hand it to the handler: file
// ranges and the manifest's own key -> tenant parser.
type scopedManifest struct{ m *manifest.Manifest }

func (s *scopedManifest) GetFilesForRange(startNs, endNs int64) []FileInfo {
	var out []FileInfo
	for _, fi := range s.m.GetFilesForRange(startNs, endNs) {
		out = append(out, FileInfo{Key: fi.Key, Size: fi.Size, MinTimeNs: fi.MinTimeNs, MaxTimeNs: fi.MaxTimeNs})
	}
	return out
}

func (s *scopedManifest) TenantKeyParser() func(string) (string, string, bool) {
	return s.m.TenantKeyParser()
}

// testPrefix is the signal prefix every test store lives under.
const testPrefix = "logs/"

// newPod returns a store attached to pod name of the bucket, with a short poll
// interval and no cache. The first sync has not run.
func newPod(t *testing.T, b *deletetest.Bucket, name string, opts ...func(*Options)) (*TombstoneStore, *deletetest.Pod) {
	t.Helper()
	pod := b.Pod(name)
	o := Options{Backend: pod, Prefix: testPrefix, Interval: time.Minute, Now: pod.Now}
	for _, f := range opts {
		f(&o)
	}
	s := NewTombstoneStore()
	if err := s.Attach(o); err != nil {
		t.Fatal(err)
	}
	return s, pod
}

// newSyncedPod is newPod plus the first (full) sync.
func newSyncedPod(t *testing.T, b *deletetest.Bucket, name string, opts ...func(*Options)) (*TombstoneStore, *deletetest.Pod) {
	t.Helper()
	s, pod := newPod(t, b, name, opts...)
	if err := s.SyncNow(context.Background(), true); err != nil {
		t.Fatalf("first sync of %s: %v", name, err)
	}
	return s, pod
}

// newTestStore is one synced pod over a fresh bucket, for tests of a single
// pod's behaviour.
func newTestStore(t *testing.T) *TombstoneStore {
	t.Helper()
	s, _ := newSyncedPod(t, deletetest.NewBucket(), "pod")
	return s
}

func tenant1001() []TenantRef { return []TenantRef{{AccountID: 1001}} }

// hideTombstone is a minimal valid tombstone of tenant 1001:0.
func hideTombstone(id string) Tombstone {
	return Tombstone{ID: id, Query: "error", StartNs: 0, EndNs: 1 << 60, Mode: "hide", Tenants: tenant1001()}
}

// sampleTombstone is a valid tombstone of the default tenant.
func sampleTombstone(id string) Tombstone {
	return Tombstone{ID: id, Query: "*", StartNs: 0, EndNs: 100, CreatedAt: time.Unix(0, 1_000_000).UTC(), Mode: "hide", Tenants: []TenantRef{{}}}
}
