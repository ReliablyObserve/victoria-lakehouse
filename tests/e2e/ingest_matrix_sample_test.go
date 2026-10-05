//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	im "github.com/ReliablyObserve/victoria-lakehouse/tests/ingestmatrix"
)

// A subprocess observes testing.T.Fatal without weakening the runner's error
// handling. Its normal TestMain reads only this parent-owned mock server; no
// external stack is involved in the child fixture.
func TestIngestMatrix_EstablishedSampleRejectsDip(t *testing.T) {
	if os.Getenv("LH_INGEST_SAMPLE_CHILD") != "1" {
		setup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/manifest/range" {
				_, _ = fmt.Fprintln(w, `{"totalFiles":1,"minTime":1,"maxTime":2}`)
			}
		}))
		defer setup.Close()
		for _, sig := range []im.Signal{im.Logs, im.Traces} {
			for _, dip := range []string{"0", "1"} {
				t.Run(string(sig)+"/dip="+dip, func(t *testing.T) {
					cmd := exec.Command(os.Args[0], "-test.run=^TestIngestMatrix_EstablishedSampleRejectsDip$")
					cmd.Env = append(os.Environ(), "LH_INGEST_SAMPLE_CHILD=1", "LH_INGEST_SAMPLE_SIGNAL="+string(sig), "LH_INGEST_SAMPLE_DIP="+dip,
						"LOGS_BASE_URL="+setup.URL, "TRACES_BASE_URL="+setup.URL)
					out, err := cmd.CombinedOutput()
					if dip == "0" {
						if err != nil {
							t.Fatalf("positive established sample: %v\n%s", err, out)
						}
					} else if err == nil || !strings.Contains(string(out), "handoff sample") || !strings.Contains(string(out), "sample count") {
						t.Fatalf("established dip did not fail immediately: err=%v\n%s", err, out)
					}
				})
			}
		}
		return
	}
	var reads atomic.Int32
	hot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"_msg":"marker"}`)
	}))
	defer hot.Close()
	lh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		read := reads.Add(1)
		// A second read would recover. The first missing established sample
		// must fail, rather than being hidden by an eventual equality wait.
		if os.Getenv("LH_INGEST_SAMPLE_DIP") == "0" || read > 1 {
			_, _ = fmt.Fprintln(w, `{"_msg":"marker"}`)
		}
	}))
	defer lh.Close()
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>obs-archive</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	defer store.Close()
	s3URL = store.URL // Child-local fixture; the parent and other tests keep their endpoint.
	s := &caseState{c: im.Case{Rows: 1}, p: im.Params{Tenant: im.NumericTenant, Marker: "marker"}, gapHits: map[string]int{}, visible: true}
	r := &matrixRun{sig: im.Signal(os.Getenv("LH_INGEST_SAMPLE_SIGNAL")), hot: ingestEnd{name: "hot", base: hot.URL}, lh: ingestEnd{name: "lh", base: lh.URL}, base: time.Now(), ingestAt: time.Now(), pq: map[string]*pqObject{}, s3c: newS3Client(t), states: []*caseState{s}}
	r.sampleEstablished(t)
	if reads.Load() != 1 || s.samples != 1 {
		t.Fatalf("strict sample reads=%d samples=%d, want one", reads.Load(), s.samples)
	}
}
