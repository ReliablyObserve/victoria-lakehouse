package manifest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// REVIEW #404: a sparse first listing after a snapshot load is accepted, so it
// "confirms gone" every retired key it happens to miss — including a compaction
// source whose delete is still owed (Reclaim). The retirement is forgotten and
// the next full listing re-adopts the source next to its replacement: its rows
// are served twice, and nothing owes its delete any more.
func TestReview404Regression_SparseFirstListingForgetsOwedRetirement(t *testing.T) {
	src := New("b", "")
	var objs []ListedObject
	for i := 0; i < 10; i++ {
		k := refreshKey("f" + strconv.Itoa(i))
		src.AddFile(refreshPartition, enriched(k, 1))
		objs = append(objs, ListedObject{Key: k, Size: 1})
	}
	if !src.ApplyListing(objs, time.Now()) {
		t.Fatal("fixture")
	}
	owed := refreshKey("compacted-source")
	src.Retire(owed, refreshKey("f0"), true) // delete failed: reclaim owed
	path := filepath.Join(t.TempDir(), "m.snap")
	if err := src.SaveTo(path); err != nil {
		t.Fatal(err)
	}

	m := New("b", "")
	if err := m.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	accepted := m.ApplyListing(objs[:1], time.Now()) // truncated first LIST
	t.Logf("sparse first listing accepted=%v files=%d", accepted, m.TotalFiles())

	full := append(append([]ListedObject(nil), objs...), ListedObject{Key: owed, Size: 1})
	if !m.ApplyListing(full, time.Now()) {
		t.Fatal("full listing rejected")
	}
	if m.HasKey(owed) {
		t.Fatalf("the full listing re-adopted %s, a retired compaction source whose delete is owed: its rows are served twice", owed)
	}
}

// REVIEW #404: ErrRefreshPartial tells callers "do not infer the object is
// gone", but the manifest itself already did: the partial listing was applied,
// so every retired key of the skipped account is "confirmed gone" and
// forgotten, and Listed() turns true for the delete scheduler and compactor.
func TestReview404Regression_PartialListingForgetsRetiredKeysOfSkippedAccount(t *testing.T) {
	keys := map[string]int64{}
	for _, tenant := range []string{"1/0", "2/0"} {
		keys[fmt.Sprintf("%s/logs/%s/f.parquet", tenant, refreshPartition)] = 100
	}
	owed := fmt.Sprintf("2/0/logs/%s/compacted-source.parquet", refreshPartition)
	keys[owed] = 100
	b := newMockBucket(keys)
	var failing bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing && r.URL.Query().Get("delimiter") != "" && r.URL.Query().Get("prefix") == "2/" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code></Error>`))
			return
		}
		b.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	client := coverageS3Client(t, srv.URL)

	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	m.Retire(owed, "x", true) // compaction replaced it; its delete failed
	time.Sleep(time.Millisecond)
	failing = true
	err := m.RefreshFromS3(context.Background(), client)
	t.Logf("partial refresh: %v listed=%v", err, m.Listed())
	failing = false
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if m.HasKey(owed) {
		t.Fatalf("a partial listing forgot the owed retirement of %s; the next full listing re-adopted it (rows served twice)", owed)
	}
}
