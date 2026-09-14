package manifest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The periodic refresh rebuilds the manifest from a bucket listing. An object
// the manifest deliberately let go of — the source a rewrite or compaction
// publish replaced, whose delete failed or has not run yet — is still listed,
// and the refresh used to adopt it straight back: its rows served twice next to
// the replacement, its deleted rows visible again once the tombstone retired,
// and the orphan sweep (which only reclaims unmanifested objects) blind to it
// for good.

// listingServer answers every ListObjectsV2 with the given keys.
func listingServer(t *testing.T, keys ...string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b strings.Builder
		b.WriteString(`<ListBucketResult>`)
		for _, k := range keys {
			b.WriteString(`<Contents><Key>` + k + `</Key><Size>100</Size></Contents>`)
		}
		b.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(b.String()))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

const refreshPartition = "dt=2026-06-04/hour=00"

func refreshKey(name string) string { return "logs/" + refreshPartition + "/" + name + ".parquet" }

func enriched(key string, rows int64) FileInfo {
	return FileInfo{Key: key, Size: 100, RowCount: rows, MinTimeNs: 1, MaxTimeNs: 2,
		Labels: map[string][]string{"service.name": {"web"}}}
}

func TestRefresh_DoesNotReadoptTheSourceARewriteReplaced(t *testing.T) {
	src, repl := refreshKey("src"), refreshKey("replacement")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(src, 5))
	if !m.ReplaceFile(refreshPartition, src, enriched(repl, 3)) {
		t.Fatal("fixture: publish refused")
	}

	// The superseded object's delete has not landed: both are listed.
	if err := m.RefreshFromS3(context.Background(), coverageS3Client(t, listingServer(t, src, repl))); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if m.HasKey(src) {
		t.Fatalf("refresh re-adopted %s, which the publish replaced: its rows are now served twice", src)
	}
	if fi, ok := m.GetFileByKey(repl); !ok || fi.RowCount != 3 {
		t.Fatalf("the replacement must stay with its enrichment, got %+v ok=%v", fi, ok)
	}
}

func TestRefresh_DoesNotReadoptTheSourcesACompactionReplaced(t *testing.T) {
	a, b, out := refreshKey("a"), refreshKey("b"), refreshKey("compacted-L1-0001")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(a, 2))
	m.AddFile(refreshPartition, enriched(b, 2))
	if !m.ReplaceFiles(refreshPartition, []string{a, b}, enriched(out, 4)) {
		t.Fatal("fixture: publish refused")
	}

	if err := m.RefreshFromS3(context.Background(), coverageS3Client(t, listingServer(t, a, b, out))); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	for _, k := range []string{a, b} {
		if m.HasKey(k) {
			t.Fatalf("refresh re-adopted the merged source %s next to %s", k, out)
		}
	}
	if got := m.TotalRows(); got != 4 {
		t.Fatalf("manifest rows = %d, want the 4 the output holds", got)
	}
}

// TestRefresh_KeepsAFilePublishedWhileTheListingRan covers the race on the
// other side: a listing that started before a publish does not contain the new
// object, and swapping it in dropped the published file until the next refresh
// — for a compaction, while its sources were already gone.
func TestRefresh_KeepsAFilePublishedWhileTheListingRan(t *testing.T) {
	old, fresh := refreshKey("old"), refreshKey("fresh")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(old, 5))

	listStart := time.Now()
	listing := []ListedObject{{Key: old, Size: 100}} // taken before the publish
	time.Sleep(time.Millisecond)
	m.AddFile(refreshPartition, enriched(fresh, 7)) // published while the listing ran

	if !m.ApplyListing(listing, listStart) {
		t.Fatal("the listing was rejected")
	}
	if !m.HasKey(fresh) {
		t.Fatalf("refresh dropped %s, published after the listing started", fresh)
	}
	if !m.HasKey(old) {
		t.Fatalf("refresh dropped %s, which the listing contains", old)
	}
}

// TestRefresh_StillAdoptsObjectsItNeverKnew pins what must NOT change: a fresh
// node, a node restarting from an old snapshot, and every peer learn of files
// flushed elsewhere only through the listing.
func TestRefresh_StillAdoptsObjectsItNeverKnew(t *testing.T) {
	known, other := refreshKey("known"), refreshKey("flushed-by-a-peer")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(known, 5))
	if err := m.RefreshFromS3(context.Background(), coverageS3Client(t, listingServer(t, known, other))); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if !m.HasKey(other) {
		t.Fatalf("refresh must adopt %s, an object this manifest never let go of", other)
	}
}

func BenchmarkApplyListing_WithRetiredKeys(b *testing.B) {
	m := New("b", "")
	var listing []ListedObject
	for i := 0; i < 5000; i++ {
		k := refreshKey("f" + strconv.Itoa(i))
		m.AddFile(refreshPartition, enriched(k, 1))
		listing = append(listing, ListedObject{Key: k, Size: 100})
	}
	for i := 0; i < 1000; i++ {
		k := refreshKey("f" + strconv.Itoa(i))
		m.RemoveFile(refreshPartition, k)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ApplyListing(listing, time.Now())
	}
}
