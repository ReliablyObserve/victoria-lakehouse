package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
