package manifest

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// classBucket is a minimal ListObjectsV2 server that reports a storage class
// per key, which the shared mock does not.
type classBucket struct {
	mu      sync.Mutex
	classes map[string]string // key -> class ("" = omitted from the listing)
}

type classObject struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass,omitempty"`
}

type classList struct {
	XMLName     xml.Name      `xml:"ListBucketResult"`
	Contents    []classObject `xml:"Contents"`
	IsTruncated bool          `xml:"IsTruncated"`
}

func (b *classBucket) set(key, class string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.classes[key] = class
}

func (b *classBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	prefix := r.URL.Query().Get("prefix")
	var res classList
	for k, c := range b.classes {
		if strings.HasPrefix(k, prefix) {
			res.Contents = append(res.Contents, classObject{Key: k, Size: 100, StorageClass: c})
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(res)
}

func classFile(t *testing.T, m *Manifest, partition, key string) FileInfo {
	t.Helper()
	for _, f := range m.FilesForPartition(partition) {
		if f.Key == key {
			return f
		}
	}
	t.Fatalf("%s not in manifest", key)
	return FileInfo{}
}

// TestRefresh_RecordsStorageClassFromList guards the LIST path: a new key gets
// the class S3 reports, with ClassSource "list" and a check time, at no extra
// request. Without it compaction never learns an object was moved to IA or
// Glacier and rewrites it.
func TestRefresh_RecordsStorageClassFromList(t *testing.T) {
	const part = "dt=2026-06-10/hour=10"
	key := "logs/" + part + "/a.parquet"
	b := &classBucket{classes: map[string]string{key: "STANDARD_IA"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	client := coverageS3Client(t, srv.URL)

	m := New("test-bucket", "logs/")
	before := time.Now()
	if err := m.RefreshFromS3(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	f := classFile(t, m, part, key)
	if f.StorageClass != "STANDARD_IA" || f.ClassSource != ClassSourceList || f.ClassCheckedAt.Before(before) {
		t.Fatalf("class not recorded from the listing: %+v", f)
	}
}

// TestRefresh_UpdatesStorageClassOfTrackedKey guards mergeRefreshedFilesLocked:
// the class is the one thing about an immutable object that changes (S3
// lifecycle moves it), so a later listing must replace it on a tracked key
// while every other enrichment field is kept. Without the update an object
// moved to Glacier stays "STANDARD" in the manifest and is rewritten.
func TestRefresh_UpdatesStorageClassOfTrackedKey(t *testing.T) {
	const part = "dt=2026-06-10/hour=10"
	key := "logs/" + part + "/a.parquet"
	b := &classBucket{classes: map[string]string{key: "STANDARD"}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	client := coverageS3Client(t, srv.URL)

	m := New("test-bucket", "logs/")
	m.AddFile(part, FileInfo{Key: key, Size: 100, RowCount: 42, CompactionLevel: 2, SchemaFingerprint: "sf",
		Labels: map[string][]string{"service.name": {"a"}}, StorageClass: "STANDARD"})
	if err := m.RefreshFromS3(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	b.set(key, "GLACIER")
	if err := m.RefreshFromS3(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	f := classFile(t, m, part, key)
	if f.StorageClass != "GLACIER" || f.ClassSource != ClassSourceList {
		t.Fatalf("class not updated by the refresh: %+v", f)
	}
	if f.RowCount != 42 || f.CompactionLevel != 2 || f.SchemaFingerprint != "sf" || len(f.Labels["service.name"]) != 1 {
		t.Fatalf("refresh dropped enrichment while updating the class: %+v", f)
	}
}

// TestRefresh_ListWithoutClassKeepsRecordedClass guards the `listed.StorageClass
// != ""` check: a listing that omits the class (an S3-compatible store) must
// not erase a class recorded earlier.
func TestRefresh_ListWithoutClassKeepsRecordedClass(t *testing.T) {
	const part = "dt=2026-06-10/hour=10"
	key := "logs/" + part + "/a.parquet"
	b := &classBucket{classes: map[string]string{key: ""}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	client := coverageS3Client(t, srv.URL)

	m := New("test-bucket", "logs/")
	m.AddFile(part, FileInfo{Key: key, Size: 100, StorageClass: "GLACIER_IR", ClassSource: "writer"})
	if err := m.RefreshFromS3(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	if f := classFile(t, m, part, key); f.StorageClass != "GLACIER_IR" || f.ClassSource != "writer" {
		t.Fatalf("recorded class erased by a listing without one: %+v", f)
	}
}

// TestRangePartitions_FullWalkAndEarlyStop guards the iteration contract: every
// partition once, the files untouched, and fn returning false ends the walk.
func TestRangePartitions_FullWalkAndEarlyStop(t *testing.T) {
	m := New("b", "logs/")
	for h := 0; h < 5; h++ {
		part := "dt=2026-06-10/hour=0" + string(rune('0'+h))
		for i := 0; i < 3; i++ {
			m.AddFile(part, FileInfo{Key: "logs/" + part + "/f" + string(rune('a'+i)) + ".parquet"})
		}
	}
	seen := map[string]int{}
	m.RangePartitions(func(p string, files []FileInfo) bool {
		seen[p] = len(files)
		return true
	})
	if len(seen) != 5 {
		t.Fatalf("visited %d partitions, want 5", len(seen))
	}
	for p, n := range seen {
		if n != 3 {
			t.Fatalf("%s: %d files", p, n)
		}
	}
	calls := 0
	m.RangePartitions(func(string, []FileInfo) bool { calls++; return false })
	if calls != 1 {
		t.Fatalf("fn returning false must stop the walk after 1 call, got %d", calls)
	}
	empty := New("b", "logs/")
	empty.RangePartitions(func(string, []FileInfo) bool { t.Fatal("called on an empty manifest"); return true })
}

// TestRangePartitions_WriterNotBlockedForWholeWalk guards the one-partition-at
// -a-time lock: a writer that starts while the walk is inside the first
// partition gets in at a partition boundary, before the walk ends. With the
// read lock held for the whole walk the writer could only finish after the
// last partition. The walk has many partitions and the check passes as soon as
// any later callback sees the write done, so a slow scheduler (a loaded CI
// runner) only delays the write by a few partitions instead of failing the test.
func TestRangePartitions_WriterNotBlockedForWholeWalk(t *testing.T) {
	const parts = 64
	m := New("b", "logs/")
	for h := 0; h < parts; h++ {
		part := fmt.Sprintf("dt=2026-06-10/hour=%02d/p=%02d", h%24, h)
		m.AddFile(part, FileInfo{Key: "logs/" + part + "/a.parquet"})
	}
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	visited := 0
	doneBeforeEnd := false
	m.RangePartitions(func(string, []FileInfo) bool {
		visited++
		if visited == 1 {
			go func() {
				close(writerStarted)
				m.AddFile("dt=2026-06-11/hour=00", FileInfo{Key: "logs/dt=2026-06-11/hour=00/w.parquet"})
				close(writerDone)
			}()
			<-writerStarted
			time.Sleep(20 * time.Millisecond) // let the writer reach the lock
			return true
		}
		select {
		case <-writerDone:
			if visited < parts {
				doneBeforeEnd = true
			}
		default:
		}
		return true
	})
	<-writerDone
	if !doneBeforeEnd {
		t.Fatal("a concurrent writer waited for the whole walk")
	}
}
