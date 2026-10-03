package compaction

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// classLister is a ListObjectsV2 server reporting a storage class per key.
type classLister struct {
	mu    sync.Mutex
	class map[string]string
}

func (l *classLister) setAll(c string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k := range l.class {
		l.class[k] = c
	}
}

func (l *classLister) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	type obj struct {
		Key          string
		Size         int64
		StorageClass string
	}
	type res struct {
		XMLName  xml.Name `xml:"ListBucketResult"`
		Contents []obj
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	prefix := r.URL.Query().Get("prefix")
	var out res
	for k, c := range l.class {
		if strings.HasPrefix(k, prefix) {
			out.Contents = append(out.Contents, obj{k, 1000, c})
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(out)
}

// TestScan_ObjectMovedToGlacierByRefreshIsNeverSelected: the class reaches the
// planner only through the manifest's LIST refresh. Twelve L0 files are
// tracked as STANDARD, then a refresh reports them GLACIER (S3 lifecycle moved
// them): the scan must not download or rewrite them, and the frozen gauge
// counts them. The control world is refreshed with STANDARD and merges. Without
// the class update in the refresh merge, or without classFrozen in the
// planner, the Glacier objects would be rewritten.
func TestScan_ObjectMovedToGlacierByRefreshIsNeverSelected(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for _, tc := range []struct {
			class     string
			wantMerge int
		}{{"GLACIER", 0}, {"DEEP_ARCHIVE", 0}, {"STANDARD_IA", 0}, {"STANDARD", 1}, {"INTELLIGENT_TIERING", 1}} {
			t.Run(tc.class, func(t *testing.T) {
				w := newPlanWorld(t, mode)
				l := newLedger(w)
				p := partitionAt(time.Now().Add(-3 * time.Hour))
				lister := &classLister{class: map[string]string{}}
				for i := 0; i < 12; i++ {
					key := fmt.Sprintf("%s/%s/batch-L0-%05d.parquet", mode, p, i)
					l.put(key, "legacy@", "", p, 0, 2, 0)
					lister.class[key] = "STANDARD"
				}
				srv := httptest.NewServer(lister)
				defer srv.Close()
				cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"),
					awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")))
				if err != nil {
					t.Fatal(err)
				}
				endpoint := srv.URL
				client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = &endpoint; o.UsePathStyle = true })

				if err := w.m.RefreshFromS3(context.Background(), client); err != nil {
					t.Fatal(err)
				}
				lister.setAll(tc.class) // lifecycle moved the objects
				if err := w.m.RefreshFromS3(context.Background(), client); err != nil {
					t.Fatal(err)
				}
				n, err := w.schedulerOn(w.pool).Scan(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if n != tc.wantMerge {
					t.Fatalf("class %s: merges=%d, want %d", tc.class, n, tc.wantMerge)
				}
				if tc.wantMerge == 0 {
					if got := len(w.m.FilesForPartition(p)); got != 12 {
						t.Fatalf("tiered objects were rewritten: %d files left", got)
					}
					if g := metrics.CompactionFrozenFiles.Get(frozenStorageClass); g != 12 {
						t.Fatalf("frozen_files{storage_class} = %d, want 12", g)
					}
				}
				l.check("after scan", true)
			})
		}
	})
}
