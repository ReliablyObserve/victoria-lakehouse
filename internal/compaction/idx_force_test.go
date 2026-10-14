package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// addIdx seeds n trace_id_idx objects of rows rows each next to a tenant's span objects.
func (w *planWorld) addIdx(tenant, partition string, level, n, rows int, fp string) []string {
	w.t.Helper()
	pt, err := manifest.ParsePartitionTime(partition)
	if err != nil {
		w.t.Fatal(err)
	}
	var keys []string
	for i := 0; i < n; i++ {
		w.seq++
		base := pt.UnixNano() + int64(w.seq)*1000
		rs := make([]tracesidx.Row, rows)
		for r := range rs {
			id := fmt.Sprintf("%032x", w.seq*1000+r)
			rs[r] = tracesidx.Row{Time: base + int64(r), Stream: tracesidx.StreamOf(id), TraceID: id, Start: base + int64(r), End: base + int64(r) + 10, Duration: 10}
		}
		data, info, err := tracesidx.Marshal(rs, 3)
		if err != nil {
			w.t.Fatal(err)
		}
		key := fmt.Sprintf("%s/%s/%s/batch-L%d-%05d.parquet", tenant, tracesidx.Signal, partition, level, w.seq)
		w.pool.put(key, data)
		w.m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: int64(rows), MinTimeNs: info.MinTime, MaxTimeNs: info.MaxTime,
			SchemaFingerprint: fp, CompactionLevel: level})
		keys = append(keys, key)
	}
	return keys
}

// The manual trigger (POST /lakehouse/compaction/recompact) merges the span objects and the
// trace_id_idx objects of a tenant, each signal on its own, whatever fingerprint the idx
// objects carry in the manifest (the flush records the span schema's, an object adopted from a
// listing has none).
func TestForceCompactPartition_SpanAndIdxObjectsMergeEachOnTheirOwn(t *testing.T) {
	for _, idxFP := range []string{planFP, ""} {
		t.Run("idxFP="+idxFP, func(t *testing.T) {
			w := newPlanWorld(t, config.ModeTraces)
			p := partitionAt(time.Now().Add(-5 * time.Hour))
			w.add("1001/0", p, 0, 2, 3, nil)
			w.addIdx("1001/0", p, 0, 2, 3, idxFP)
			w.add("1002/0", p, 0, 2, 3, nil)
			w.addIdx("1002/0", p, 0, 2, 3, idxFP)
			sched := w.shippedScheduler()
			rows := w.rows()
			res, err := sched.ForceCompactPartition(context.Background(), p, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.InputFiles) != 8 || len(res.OutputFiles) != 4 {
				t.Fatalf("inputs=%d outputs=%d, want 8 and 4: %+v", len(res.InputFiles), len(res.OutputFiles), res.OutputFiles)
			}
			var spans, idx int
			for _, k := range w.keys(p) {
				if strings.Contains(k, "/"+tracesidx.Signal+"/") {
					idx++
				} else {
					spans++
				}
				if !strings.Contains(k, "compacted-L1-") {
					t.Errorf("object %s was not compacted", k)
				}
			}
			if spans != 2 || idx != 2 {
				t.Fatalf("live objects: %d span and %d idx, want 2 and 2", spans, idx)
			}
			if w.rows() != rows {
				t.Fatalf("rows %d -> %d", rows, w.rows())
			}
		})
	}
}

// The recompact answer tells how many of the merged objects were trace_id_idx objects, so a
// caller that waits for the span objects to merge does not stop on the idx merges.
func TestRecompactHandler_ReportsIdxObjectsSeparately(t *testing.T) {
	w := newPlanWorld(t, config.ModeTraces)
	p := partitionAt(time.Now().Add(-5 * time.Hour))
	w.add("1001/0", p, 0, 2, 3, nil)
	w.addIdx("1001/0", p, 0, 3, 3, planFP)
	rr := httptest.NewRecorder()
	body := fmt.Sprintf(`{"partition":%q}`, p)
	RecompactHandler(w.shippedScheduler())(rr, httptest.NewRequest(http.MethodPost, "/lakehouse/compaction/recompact", strings.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	var out struct {
		InputFiles     int `json:"input_files"`
		OutputFiles    int `json:"output_files"`
		IdxInputFiles  int `json:"idx_input_files"`
		IdxOutputFiles int `json:"idx_output_files"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.InputFiles != 5 || out.IdxInputFiles != 3 || out.OutputFiles != 2 || out.IdxOutputFiles != 1 {
		t.Fatalf("answer %+v, want 5 inputs (3 idx) and 2 outputs (1 idx)", out)
	}
}

// With the trace_id_idx objects next to the span objects, a scan that has done all the work
// there is leaves nothing to do: every group (the spans and the idx objects of every tenant) is
// planned on its own, and a compacted idx object is never planned again alone (the storage-health
// cell of the readers fixture: "scans after convergence must be no-ops").
func TestScan_SecondScanDoesNothing_WithIdxObjects(t *testing.T) {
	w := newPlanWorld(t, config.ModeTraces)
	now := time.Now()
	for i, tenant := range []string{"1001/0", "1002/0", "1003/7"} {
		for _, fp := range []string{planFP, ""} { // flushed (the span schema's fingerprint) and listing-adopted (none)
			w.add(tenant, partitionAt(now.Add(-72*time.Hour)), 1, 2, 3, nil)
			w.addIdx(tenant, partitionAt(now.Add(-72*time.Hour)), 1, 2, 3, fp)
			w.add(tenant, partitionAt(now.Add(-50*time.Hour)), 0, 3+i, 3, nil)
			w.addIdx(tenant, partitionAt(now.Add(-50*time.Hour)), 0, 3+i, 3, fp)
			w.add(tenant, partitionAt(now.Add(-3*time.Hour)), 0, 10, 3, nil)
			w.addIdx(tenant, partitionAt(now.Add(-3*time.Hour)), 0, 10, 3, fp)
			w.add(tenant, partitionAt(now.Add(-2*time.Hour)), 1, 1, 3, nil)
			w.addIdx(tenant, partitionAt(now.Add(-2*time.Hour)), 1, 1, 3, fp)
		}
	}
	sched := w.shippedScheduler()
	rows := w.rows()
	for scan := 0; scan < 50; scan++ {
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if got := w.rows(); got != rows {
		t.Fatalf("rows not conserved: %d -> %d", rows, got)
	}
	for scan := 1; scan <= 3; scan++ {
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			var keys []string
			for _, files := range w.m.AllFiles() {
				for _, f := range files {
					keys = append(keys, f.Key)
				}
			}
			sort.Strings(keys)
			t.Fatalf("scan %d after convergence: compactions=%d, want 0; objects: %v", scan, n, keys)
		}
	}
}

// The idx objects of a group are merged together whatever fingerprint the manifest records for
// them: an object the flush wrote (it records the writer's fingerprint) and one adopted from a
// listing (none) are the same kind of object, so one scan merges them into one output instead of
// one output per fingerprint that a later scan merges again (the readers fixture's "scans after
// convergence must be no-ops").
func TestScan_IdxObjectsOfDifferentFingerprintsMergeAtOnce(t *testing.T) {
	w := newPlanWorld(t, config.ModeTraces)
	p := partitionAt(time.Now().Add(-50 * time.Hour))
	w.addIdx("1001/0", p, 0, 2, 3, planFP)
	w.addIdx("1001/0", p, 0, 2, 3, "")
	sched := w.shippedScheduler()
	n, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	live := w.keys(p)
	if n != 1 || len(live) != 1 {
		t.Fatalf("first scan: %d compactions, live objects %v; want 1 compaction and one object", n, live)
	}
	if n, err := sched.Scan(context.Background()); err != nil || n != 0 {
		t.Fatalf("second scan: %d compactions (%v), want 0", n, err)
	}
}
