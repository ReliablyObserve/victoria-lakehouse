package parquets3

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRefreshManifest_DoesNotReadoptASupersededObject runs the production
// refresh (a real ListObjectsV2 against the mock bucket) after a publish that
// replaced a flushed file whose delete has not landed. The superseded object is
// still in the bucket; adopting it again serves its rows twice and hides it
// from the orphan sweep, which only reclaims unmanifested objects.
func TestRefreshManifest_DoesNotReadoptASupersededObject(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)

	now := time.Now().UnixNano()
	bw.AddLogRows([]schema.LogRow{{TimestampUnixNano: now, Body: "a", ServiceName: "svc"}})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now-1, now+1)
	if len(files) != 1 {
		t.Fatalf("fixture: want 1 file, got %d", len(files))
	}
	src := files[0]
	ctx := context.Background()
	data, err := s.pool.Download(ctx, src.Key)
	if err != nil {
		t.Fatalf("download: %v", err)
	}

	// A rewrite (or compaction) publishes a replacement; the superseded
	// object's delete has not run yet.
	replacement := src
	replacement.Key = strings.TrimSuffix(src.Key, ".parquet") + "-replacement.parquet"
	if err := s.pool.Upload(ctx, replacement.Key, data); err != nil {
		t.Fatalf("upload: %v", err)
	}
	partition, _ := s.manifest.PartitionForKey(src.Key)
	if !s.manifest.ReplaceFile(partition, src.Key, replacement) {
		t.Fatal("fixture: publish refused")
	}

	if err := s.RefreshManifest(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if s.manifest.HasKey(src.Key) {
		t.Fatalf("the refresh re-adopted %s, which the publish replaced", src.Key)
	}
	if fi, ok := s.manifest.GetFileByKey(replacement.Key); !ok || fi.RowCount != 1 {
		t.Fatalf("the replacement must stay registered with its row count, got %+v ok=%v", fi, ok)
	}
	if got := s.manifest.TotalRows(); got != 1 {
		t.Fatalf("manifest rows = %d, want 1: the row must not be counted twice", got)
	}
}
