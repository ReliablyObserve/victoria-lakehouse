package main

// Twin of cmd/lakehouse-logs/tombstone_multiinstance_test.go (spans instead of
// log rows) — keep the two aligned.
//
// Two writers and two selects over one bucket, each with its own Storage, its own
// view of the deletes (read from the bucket through the real S3 client) and its
// own request log. Deletes are taken through the real delete API handler.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/jaeger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// tombPod is one pod of the fleet.
// fastPoll shortens the poll interval, a constant of one minute in production, so
// a test sees a delete reach another pod within a second.
func fastPoll(o *delete.Options) { o.Interval = time.Second }

type tombPod struct {
	name  string
	cfg   *config.Config
	store *parquets3.Storage
	ts    *delete.TombstoneStore
	s3    *fakeS3
	api   http.Handler
	stop  chan struct{}
}

func (p *tombPod) request(method, path string, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	p.api.ServeHTTP(rec, req)
	return rec
}

// newTombPod builds a pod over its own endpoint of the shared bucket and starts
// the tombstone sync the way run() does.
func newTombPod(t *testing.T, name string, role config.Role, ep *fakeS3, tweak func(*config.Config)) *tombPod {
	t.Helper()
	cfg := podConfig(t, role, ep.URL())
	cfg.Retention.Enabled = false
	cfg.Compaction.Enabled = false
	if tweak != nil {
		tweak(cfg)
	}
	store := newPod(t, cfg)
	ts := delete.NewTombstoneStore()
	if err := attachTombstones(cfg, store.Pool(), ts, &tombstoneRetention{}, fastPoll); err != nil {
		t.Fatal(err)
	}
	store.SetTombstoneStore(ts)
	p := &tombPod{name: name, cfg: cfg, store: store, ts: ts, s3: ep, stop: make(chan struct{})}
	startTombstoneSync(ts, p.stop)
	t.Cleanup(func() { close(p.stop) })

	mq := &manifestQuerierAdapter{m: store.Manifest()}
	mux := http.NewServeMux()
	delete.NewHandler(ts, mq, newStorageClassDetector(cfg), &cfg.Delete, "traces").Register(mux)
	p.api = mux
	return p
}

type tombFleet struct {
	bucket  *fakeS3
	writers []*tombPod
	selects []*tombPod
}

func (f *tombFleet) all() []*tombPod {
	return append(append([]*tombPod(nil), f.writers...), f.selects...)
}

// newTombFleet seeds the bucket through a writer and starts 2 writers and 2
// selects.
func newTombFleet(t *testing.T, files, rowsEach int) *tombFleet {
	t.Helper()
	bucket := newFakeS3()
	t.Cleanup(bucket.Close)
	f := &tombFleet{bucket: bucket}
	eps := []*fakeS3{bucket, newFakeS3On(bucket), newFakeS3On(bucket), newFakeS3On(bucket)}
	for _, ep := range eps[1:] {
		t.Cleanup(ep.Close)
	}
	w1 := newTombPod(t, "writer-1", config.RoleInsert, eps[0], nil)
	seedFlushes(t, w1.store, w1.cfg, rowsEach, seedGroup{files: files, age: 48 * time.Hour})
	bucket.Age(time.Hour)
	bucket.AgePrefix("_segments/", time.Hour)
	f.writers = []*tombPod{w1, newTombPod(t, "writer-2", config.RoleInsert, eps[1], nil)}
	f.selects = []*tombPod{newTombPod(t, "select-1", config.RoleSelect, eps[2], nil), newTombPod(t, "select-2", config.RoleSelect, eps[3], nil)}
	for _, p := range f.all() {
		if err := p.store.RefreshManifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := p.store.Manifest().TotalFiles(); got != files {
			t.Fatalf("%s sees %d objects, want %d", p.name, got, files)
		}
	}
	return f
}

func (f *tombFleet) eventually(t *testing.T, within time.Duration, what string, ok func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < within {
		if ok() {
			return time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: not true after %v", what, within)
	return 0
}

func rowCount(t *testing.T, p *tombPod) int { return len(queryKeys(t, p.store, "trace_id")) }

func deleteFormMode(query, mode string) url.Values {
	return url.Values{"query": {query}, "start": {"0"}, "end": {fmt.Sprint(time.Now().Add(time.Hour).UnixNano())}, "mode": {mode}}
}

// deleteForm is a hide-mode delete, the one mode that can be undone.
func deleteForm(query string) url.Values { return deleteFormMode(query, "hide") }

// The delete, the un-delete, and the S3 request rules, through the real client. A delete
// taken on one select pod hides its rows on the other select pod and on both
// writers within one poll interval; an un-delete brings them back the same way;
// every PUT under _tombstones/ carries If-None-Match and nothing is ever deleted
// there; a select pod writes nothing but those keys.
func TestTombstones_MultiInstance_DeleteAndUndeleteReachEveryPod(t *testing.T) {
	const files, rowsEach = 4, 5
	f := newTombFleet(t, files, rowsEach)
	total := files * rowsEach
	for _, p := range f.all() {
		if got := rowCount(t, p); got != total {
			t.Fatalf("%s sees %d rows before any delete, want %d", p.name, got, total)
		}
		p.s3.Reset()
	}

	// Delete the five rows of "file 1" through the API of select-1.
	rec := f.selects[0].request(http.MethodPost, "/delete/tracessql/delete", deleteForm(`trace_id:~"^t1-"`))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	id := body["tombstone_id"].(string)
	if got := rowCount(t, f.selects[0]); got != total-rowsEach {
		t.Fatalf("the pod that took the delete sees %d rows, want %d at once", got, total-rowsEach)
	}
	var worst time.Duration
	for _, p := range []*tombPod{f.selects[1], f.writers[0], f.writers[1]} {
		d := f.eventually(t, 6*time.Second, p.name+" enforces the delete", func() bool { return rowCount(t, p) == total-rowsEach })
		if d > worst {
			worst = d
		}
	}
	t.Logf("delete visible on every pod after %v (poll interval 1s)", worst)

	// Un-delete through the other select pod.
	rec = f.selects[1].request(http.MethodDelete, "/delete/tracessql/tombstone/"+id, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("un-delete: %d %s", rec.Code, rec.Body.String())
	}
	for _, p := range f.all() {
		f.eventually(t, 6*time.Second, p.name+" shows the rows again", func() bool { return rowCount(t, p) == total })
	}

	// What the pods asked the bucket.
	for _, p := range f.all() {
		var tombPuts, tombDeletes int
		for _, r := range p.s3.Requests() {
			if !strings.Contains(r.Key, "_tombstones/") {
				if p.cfg.Role == config.RoleSelect && bucketWriteOps[r.Op] {
					t.Errorf("%s (select) wrote %s %s outside the tombstone keys", p.name, r.Op, r.Key)
				}
				continue
			}
			switch r.Op {
			case "PUT":
				tombPuts++
				if !r.Cond {
					t.Errorf("%s: PUT %s without If-None-Match", p.name, r.Key)
				}
			case "DELETE", "DELETE_OBJECTS":
				tombDeletes++
			}
		}
		if tombDeletes != 0 {
			t.Errorf("%s deleted tombstone keys without a retention expiry", p.name)
		}
		t.Logf("%s: %d conditional PUTs under _tombstones/, %d DELETEs", p.name, tombPuts, tombDeletes)
	}
	// The whole life of a hide delete is two keys: the definition and the undo.
	n := 0
	f.bucket.mu.Lock()
	for k := range f.bucket.objs {
		if strings.Contains(k, "_tombstones/") {
			n++
		}
	}
	f.bucket.mu.Unlock()
	if n != 2 {
		t.Errorf("%d tombstone keys after a delete and an un-delete, want 2 (definition, undo)", n)
	}
	// A permanent (or auto) delete cannot be undone: the answer is 409 and nothing
	// is written.
	rec = f.selects[0].request(http.MethodPost, "/delete/tracessql/delete", deleteFormMode(`"file-2-row"`, "permanent"))
	if rec.Code != http.StatusOK {
		t.Fatalf("permanent delete: %d %s", rec.Code, rec.Body.String())
	}
	permanentID := decodeBody(t, rec)["tombstone_id"].(string)
	if rec = f.selects[0].request(http.MethodDelete, "/delete/tracessql/tombstone/"+permanentID, nil); rec.Code != http.StatusConflict {
		t.Fatalf("un-delete of a permanent delete: %d %s, want 409", rec.Code, rec.Body.String())
	}
}

// A store that returns a short listing without an error must never make a pod
// drop a delete, nor delete anything: the keys stay and the next full listing
// completes the view. Also across the restart of a pod.
func TestTombstones_MultiInstance_ShortListingNeverDropsOrDeletes(t *testing.T) {
	const files, rowsEach = 3, 4
	f := newTombFleet(t, files, rowsEach)
	reader := f.selects[0]
	for i := 0; i < 3; i++ {
		rec := f.writers[0].request(http.MethodPost, "/delete/tracessql/delete", deleteForm(fmt.Sprintf(`trace_id:~"^t%d-"`, i)))
		if rec.Code != http.StatusOK {
			t.Fatalf("delete %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
	f.eventually(t, 6*time.Second, "the reader enforces all three", func() bool { return rowCount(t, reader) == 0 })
	reader.s3.Reset()

	// Two short listings in a row, one of them a full one forced through the API
	// of the view; the reader keeps enforcing.
	reader.s3.ShortListOnce(1)
	if err := reader.ts.SyncNow(context.Background(), true); err != nil {
		t.Fatalf("sync: %v", err)
	}
	reader.s3.ShortListOnce(1)
	time.Sleep(1300 * time.Millisecond)
	if got := rowCount(t, reader); got != 0 {
		t.Fatalf("a short listing un-hid %d rows", got)
	}
	for _, r := range reader.s3.Requests() {
		if strings.Contains(r.Key, "_tombstones/") && (r.Op == "DELETE" || r.Op == "DELETE_OBJECTS") {
			t.Fatalf("the reader deleted %s after a short listing", r.Key)
		}
	}

	// A restarted pod with a short first listing enforces what its cache holds.
	restarted := newTombPod(t, "select-1b", config.RoleSelect, newFakeS3On(f.bucket), func(c *config.Config) { c.Delete.CacheDir = reader.cfg.Delete.CacheDir })
	if err := restarted.store.RefreshManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.eventually(t, 6*time.Second, "the restarted pod enforces all three", func() bool { return rowCount(t, restarted) == 0 })
}

// A pod whose view cannot be refreshed fails its reads closed after three poll
// intervals (503), while the write path is untouched.
func TestTombstones_MultiInstance_StaleViewFailsReadsClosedAndSparesIngest(t *testing.T) {
	f := newTombFleet(t, 2, 3)
	writer := f.writers[1]
	if rowCount(t, writer) == 0 {
		t.Fatal("fixture")
	}
	writer.s3.FailListings(true)
	deadline := time.Now().Add(10 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		err = queryErr(writer)
		if err != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err == nil {
		t.Fatal("reads never failed closed although the view could not be refreshed")
	}
	var esc *httpserver.ErrorWithStatusCode
	if !asStatus(err, &esc) || esc.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stale-view read error = %v, want a 503", err)
	}
	// The write path does not wait for the view: the pod still flushes.
	before := writer.store.Manifest().TotalFiles()
	seedFlushesAfter(t, writer.store, writer.cfg, 2, before, seedGroup{files: 1, age: 49 * time.Hour})
	if writer.store.Manifest().TotalFiles() != before+1 {
		t.Fatal("ingest and flush were blocked by a stale tombstone view")
	}
	// Other pods are unaffected.
	if got := rowCount(t, f.selects[0]); got == 0 {
		t.Fatal("a healthy pod stopped serving")
	}
	// The store recovers: the gate reopens.
	writer.s3.FailListings(false)
	f.eventually(t, 8*time.Second, "reads reopen after recovery", func() bool { return queryErr(writer) == nil })
}

// A store without conditional writes: creating the same delete task from two
// pods still leaves one object (check, then write; identical bytes).
func TestTombstones_MultiInstance_StoreWithoutConditionalWrites(t *testing.T) {
	f := newTombFleet(t, 1, 2)
	f.bucket.RejectConditionalWrites(true)
	taskTime := time.Now().UnixNano()
	for _, p := range f.all() {
		if err := delete.RunTask(context.Background(), p.ts, false, strconv.FormatInt(taskTime, 10), taskTime, []delete.TenantRef{{AccountID: 0}}, `trace_id:~"^t0-"`, "hide"); err != nil {
			t.Fatalf("%s: %v", p.name, err)
		}
	}
	n := 0
	f.bucket.mu.Lock()
	for k := range f.bucket.objs {
		if strings.Contains(k, "_tombstones/") {
			n++
		}
	}
	f.bucket.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d tombstone keys after four creates of one task, want 1", n)
	}
	for _, p := range f.all() {
		f.eventually(t, 4*time.Second, p.name+" hides the task's rows", func() bool { return rowCount(t, p) == 0 })
	}
}

// Compaction does not copy a deleted row forward into a visible state: after the
// writer compacts the files, every pod still hides the deleted rows (the
// tombstone stays in force; physical removal is not enabled, so the rows are
// still stored and hidden).
func TestTombstones_MultiInstance_CompactionKeepsDeletedRowsHidden(t *testing.T) {
	const files, rowsEach = 4, 5
	f := newTombFleet(t, files, rowsEach)
	rec := f.selects[0].request(http.MethodPost, "/delete/tracessql/delete", deleteForm(`trace_id:~"^t2-"`))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body.String())
	}
	for _, p := range f.all() {
		f.eventually(t, 6*time.Second, p.name+" hides the rows", func() bool { return rowCount(t, p) == files*rowsEach-rowsEach })
	}

	// The writer compacts the four files into one.
	w := f.writers[0]
	w.cfg.Compaction.Enabled = true
	w.cfg.Compaction.Interval = 50 * time.Millisecond
	w.cfg.Compaction.MinFilesL0 = 2
	w.cfg.Compaction.MinAge = 0
	w.store.SetWriterPeers([]string{"writer-1:9428"})
	var holder *tenant.PolicyRegistry
	_, sweep, stop := setupCompaction(w.cfg, w.store, nil, "writer-1:9428", &holder, newStorageClassDetector(w.cfg), selfaddr.Env{})
	if sweep != nil {
		sweep.Stop()
	}
	defer stop()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, n := bucketRows(t, f.bucket); n == 1 {
			break
		}
		f.bucket.AgePrefix("_segments/", time.Hour)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(800 * time.Millisecond) // a late merge or delete would land here
	rows, objects := bucketRows(t, f.bucket)
	if objects != 1 {
		t.Fatalf("compaction left %d objects, want 1", objects)
	}
	if rows != int64(files*rowsEach) {
		t.Fatalf("the compacted object holds %d rows, want all %d (hide-only: nothing is physically removed)", rows, files*rowsEach)
	}
	for _, p := range f.all() {
		if err := p.store.RefreshManifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, k := range queryKeys(t, p.store, "trace_id") {
			if strings.HasPrefix(k, "t2-") {
				t.Fatalf("%s shows the deleted row %s after compaction: a deleted row was copied forward visibly", p.name, k)
			}
			seen[k] = true
		}
		// (The writer that took the seed also holds the rows in its insert buffer,
		// so its count can include a row twice; the kept set is what must be whole.)
		for _, k := range seededKeys(files, rowsEach) {
			if !strings.HasPrefix(k, "t2-") && !seen[k] {
				t.Fatalf("%s lost the kept row %s", p.name, k)
			}
		}
	}
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return out
}

// queryErr runs a match-all query over the seeded window and returns its error.
func queryErr(p *tombPod) error {
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		return err
	}
	q.AddTimeFilter(time.Now().Add(-96*time.Hour).UnixNano(), time.Now().UnixNano())
	return p.store.RunQuery(context.Background(), nil, q, func(uint, *logstorage.DataBlock) {})
}

func asStatus(err error, target **httpserver.ErrorWithStatusCode) bool { return errors.As(err, target) }

// Trace-by-ID goes through the same gate: both the index lookup (VT's stats
// query against the trace_id_idx stream) and the span fetch answer 503 on a pod
// whose view of the deletes is stale, and work on a healthy one. The index
// lookup is answered from footers without reading spans, so it is gated on its
// own (LookupTraceIndex), not only through RunQuery.
func TestTombstones_MultiInstance_StaleViewFailsTraceByIDClosed(t *testing.T) {
	f := newTombFleet(t, 2, 3)
	writer := f.writers[1]
	vtstorageadapter.Init(writer.store)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })

	byID := func(qs string) error {
		q, err := logstorage.ParseQuery(qs)
		if err != nil {
			t.Fatalf("parse %q: %v", qs, err)
		}
		q.AddTimeFilter(time.Now().Add(-96*time.Hour).UnixNano(), time.Now().UnixNano())
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		return vtstorage.RunQuery(qctx, func(uint, *logstorage.DataBlock) {})
	}
	indexQuery := `{trace_id_idx_stream="1"} trace_id_idx:="t0-0" | stats min(_time) as _time, min(start_time) as start_time, max(end_time) as end_time | limit 10`
	spanQuery := `trace_id:="t0-0"`

	for _, qs := range []string{indexQuery, spanQuery} {
		if err := byID(qs); err != nil {
			t.Fatalf("healthy trace-by-ID %q failed: %v", qs, err)
		}
	}
	writer.s3.FailListings(true)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && queryErr(writer) == nil {
		time.Sleep(100 * time.Millisecond)
	}
	for _, qs := range []string{indexQuery, spanQuery} {
		err := byID(qs)
		var esc *httpserver.ErrorWithStatusCode
		if err == nil || !asStatus(err, &esc) || esc.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("trace-by-ID %q on a stale pod = %v, want a 503", qs, err)
		}
	}
	// The index lookup itself is gated, not only the span fetch behind it.
	if _, _, _, err := writer.store.LookupTraceIndex(context.Background(), nil, "t0-0"); err == nil {
		t.Fatal("LookupTraceIndex answered on a stale pod")
	}
	writer.s3.FailListings(false)
	f.eventually(t, 8*time.Second, "trace-by-ID reopens after recovery", func() bool { return byID(spanQuery) == nil })
}

// Jaeger's trace-by-ID of a trace whose spans a delete hid answers 404 (VT's
// "no spans" answer) on every pod, a trace the delete did not touch still answers
// 200, and the un-delete brings the trace back. The 404 is VT's own handler
// reading no spans, so the cell holds for any hide-mode delete.
func TestTombstones_MultiInstance_DeletedTraceByIDAnswers404(t *testing.T) {
	f := newTombFleet(t, 3, 4)
	vtstorageadapter.Init(f.selects[1].store)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })

	byID := func(id string) int {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/select/jaeger/api/traces/"+id, nil)
		rec := httptest.NewRecorder()
		if !jaeger.RequestHandler(context.Background(), rec, req) {
			t.Fatalf("the jaeger handler did not serve %s", req.URL.Path)
		}
		return rec.Code
	}
	if got := byID("t1-0"); got != http.StatusOK {
		t.Fatalf("trace t1-0 before the delete: %d, want 200", got)
	}

	rec := f.selects[0].request(http.MethodPost, "/delete/tracessql/delete", deleteForm(`trace_id:~"^t1-"`))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body.String())
	}
	id := decodeBody(t, rec)["tombstone_id"].(string)
	f.eventually(t, 6*time.Second, "select-2 answers 404 for the deleted trace", func() bool { return byID("t1-0") == http.StatusNotFound })
	if got := byID("t0-0"); got != http.StatusOK {
		t.Fatalf("a trace the delete did not touch: %d, want 200", got)
	}

	if rec = f.selects[0].request(http.MethodDelete, "/delete/tracessql/tombstone/"+id, nil); rec.Code != http.StatusOK {
		t.Fatalf("un-delete: %d %s", rec.Code, rec.Body.String())
	}
	f.eventually(t, 6*time.Second, "select-2 serves the trace again", func() bool { return byID("t1-0") == http.StatusOK })
}

// Another tenant cannot un-delete a tenant's delete through a pod that has not
// polled it yet: the pod syncs to learn the id, applies the tenant check and
// answers 404 without writing an undo key; the delete stays in force on every pod.
func TestTombstones_MultiInstance_UndeleteByAnotherTenantThroughAnUnpolledPodIsRefused(t *testing.T) {
	const files, rowsEach = 2, 3
	f := newTombFleet(t, files, rowsEach)
	rec := f.selects[0].request(http.MethodPost, "/delete/tracessql/delete", deleteForm(`trace_id:~"^t1-"`))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body.String())
	}
	id := decodeBody(t, rec)["tombstone_id"].(string)

	// Immediately, through the other select pod, as another tenant.
	req := httptest.NewRequest(http.MethodDelete, "/delete/tracessql/tombstone/"+id, nil)
	req.Header.Set("AccountID", "7")
	out := httptest.NewRecorder()
	f.selects[1].api.ServeHTTP(out, req)
	if out.Code != http.StatusNotFound {
		t.Fatalf("another tenant's un-delete through the other select pod: %d %s, want 404", out.Code, out.Body.String())
	}
	f.bucket.mu.Lock()
	for k := range f.bucket.objs {
		if strings.HasSuffix(k, ".undo") {
			f.bucket.mu.Unlock()
			t.Fatalf("an undo key %s was written by a refused un-delete", k)
		}
	}
	f.bucket.mu.Unlock()
	for _, p := range f.all() {
		f.eventually(t, 6*time.Second, p.name+" still enforces the delete", func() bool { return rowCount(t, p) == files*rowsEach-rowsEach })
	}
}
