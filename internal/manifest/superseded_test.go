package manifest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Compaction writes the merged output, removes the sources from the manifest
// and deletes their objects. An S3 LIST that started before those deletes still
// returns the sources, so the refresh that applies it would put their rows back
// next to the merged output that already holds them — every value counted
// twice, every row read twice, until the next refresh.
//
// The manifest, not the read path, is where that is settled: the compactor
// marks what it merged away, and the refresh ignores those keys. Guessing it in
// the enumeration paths instead (a higher compaction level covering the same
// seconds) dropped the newest flush of a live partition, whose rows fall inside
// the compacted object's backfilled range.

// listingOf serves a LIST returning exactly these keys.
func listingOf(t *testing.T, keys ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		body := `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>`
		for _, k := range keys {
			body += fmt.Sprintf("<Contents><Key>%s</Key><Size>1000</Size></Contents>", k)
		}
		body += `</ListBucketResult>`
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

const (
	supersededPartition = "dt=2026-05-01/hour=10"
	supersededSourceA   = "logs/dt=2026-05-01/hour=10/src-a.parquet"
	supersededSourceB   = "logs/dt=2026-05-01/hour=10/src-b.parquet"
	supersededMerged    = "logs/dt=2026-05-01/hour=10/compacted-L1-0001.parquet"
)

// TestRefresh_CompactedAwaySourcesAreNotReadmitted: a listing taken before the
// compactor's deletes must not resurrect the merged sources.
func TestRefresh_CompactedAwaySourcesAreNotReadmitted(t *testing.T) {
	srv := listingOf(t, supersededSourceA, supersededSourceB, supersededMerged)
	client := testS3Client(t, srv.URL)
	m := New("test-bucket", "logs/")

	m.AddFile(supersededPartition, FileInfo{Key: supersededMerged, Size: 1000, RowCount: 20, CompactionLevel: 1})
	m.MarkSuperseded([]string{supersededSourceA, supersededSourceB})
	m.RemoveFile(supersededPartition, supersededSourceA)
	m.RemoveFile(supersededPartition, supersededSourceB)

	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	keys := map[string]bool{}
	for _, fi := range m.GetFilesForRange(0, 1<<62) {
		keys[fi.Key] = true
	}
	for _, src := range []string{supersededSourceA, supersededSourceB} {
		if keys[src] {
			t.Errorf("compacted-away source %s came back from the listing; its rows are in %s", src, supersededMerged)
		}
	}
	if !keys[supersededMerged] {
		t.Errorf("the merged output is missing from the manifest: %v", keys)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles=%d, want 1 — the refresh totals must not count the ignored objects", got)
	}
	// The listing still returns them, so the marks are still needed.
	if got := m.SupersededCount(); got != 2 {
		t.Errorf("SupersededCount=%d, want 2 while the listing still returns the sources", got)
	}
}

// TestRefresh_SupersededMarksAreForgottenOnceTheListingDropsTheKey: the marks
// are not a growing set — a delete S3 has caught up with needs no mark.
func TestRefresh_SupersededMarksAreForgottenOnceTheListingDropsTheKey(t *testing.T) {
	m := New("test-bucket", "logs/")
	m.MarkSuperseded([]string{supersededSourceA, supersededSourceB})
	if got := m.SupersededCount(); got != 2 {
		t.Fatalf("SupersededCount=%d after marking 2 keys", got)
	}

	client := testS3Client(t, listingOf(t, supersededMerged).URL)
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}
	if got := m.SupersededCount(); got != 0 {
		t.Errorf("SupersededCount=%d, want 0 once the listing no longer returns the keys", got)
	}
	if m.IsSuperseded(supersededSourceA) {
		t.Error("a key S3 no longer lists is still marked")
	}
}

// TestMarkSuperseded_MarksExpire: a delete that failed after the merged output
// was written must not hide its object forever.
func TestMarkSuperseded_MarksExpire(t *testing.T) {
	m := New("test-bucket", "logs/")
	m.MarkSuperseded([]string{supersededSourceA})
	if !m.IsSuperseded(supersededSourceA) {
		t.Fatal("key is not marked right after MarkSuperseded")
	}

	m.mu.Lock()
	m.superseded[supersededSourceA] = time.Now().Add(-supersededTTL - time.Minute)
	m.mu.Unlock()

	if m.IsSuperseded(supersededSourceA) {
		t.Error("an expired mark still hides the object")
	}
	if got := m.SupersededCount(); got != 0 {
		t.Errorf("SupersededCount=%d, want 0 for an expired mark", got)
	}

	// The refresh takes the object back, because S3 still has it.
	client := testS3Client(t, listingOf(t, supersededSourceA).URL)
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles=%d, want 1 — an expired mark must not keep the object out", got)
	}
}

// TestMarkSuperseded_EmptyAndBlankKeys: defensive, no mark for nothing.
func TestMarkSuperseded_EmptyAndBlankKeys(t *testing.T) {
	m := New("test-bucket", "logs/")
	m.MarkSuperseded(nil)
	m.MarkSuperseded([]string{""})
	if got := m.SupersededCount(); got != 0 {
		t.Errorf("SupersededCount=%d, want 0", got)
	}
}
