package parquets3

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Buffer-flush idempotency (#45): a window whose flush failed or was
// interrupted is retried exactly, to the same objects, so no row is written
// twice.

var (
	idemTenantA = logstorage.TenantID{AccountID: 1, ProjectID: 2}
	idemTenantB = logstorage.TenantID{AccountID: 2, ProjectID: 3}
)

const idemRowsPerGroup = 10

// idemEnv is a buffer, a writer whose uploads go to a faultyUploader, and a
// watermark directory: everything a flusher needs, and what survives a restart.
type idemEnv struct {
	t       *testing.T
	bufDir  string
	wmDir   string
	st      *membuffer.Store
	u       *faultyUploader
	bw      *BatchWriter
	m       *manifest.Manifest
	hour    time.Time // start of the first partition hour
	failFor atomic.Value
	hook    atomic.Value // func(key string): runs inside every upload, before it is stored
	statMu  sync.Mutex
	statRow int64
}

func newIdemEnv(t *testing.T) *idemEnv {
	t.Helper()
	e := &idemEnv{t: t, bufDir: t.TempDir(), wmDir: t.TempDir()}
	e.failFor.Store("")
	e.hook.Store(func(string) {})
	e.hour = time.Now().Add(-4 * time.Hour).Truncate(time.Hour)
	e.u = &faultyUploader{fail: func(key string) error {
		e.hook.Load().(func(string))(key)
		if id, _ := e.failFor.Load().(string); id != "" && strings.Contains(key, id) {
			return errPutFailed
		}
		return nil
	}}
	e.bw, e.m = durabilityWriter(t, e.u)
	e.bw.SetStatsCallback(func(_, _ uint32, _, _, rows int64, _ string) {
		e.statMu.Lock()
		e.statRow += rows
		e.statMu.Unlock()
	})
	e.open()
	t.Cleanup(func() {
		if e.st != nil {
			e.st.Close()
		}
	})
	return e
}

func (e *idemEnv) open() {
	e.t.Helper()
	st, err := membuffer.Open(membuffer.Config{Path: e.bufDir, Retention: 24 * time.Hour})
	if err != nil {
		e.t.Fatalf("open buffer: %v", err)
	}
	e.st = st
}

// restart is a crash: the buffer is closed and reopened from disk.
func (e *idemEnv) restart() {
	e.t.Helper()
	e.st.DebugFlush()
	e.st.Close()
	e.st = nil
	e.open()
}

// flusher returns a new flusher over the env's buffer, writer and watermark dir.
func (e *idemEnv) flusher(targetBytes int64, maxLinger time.Duration) *BufferFlusher {
	f := NewBufferFlusher(e.bw, e.st, e.wmDir, nil, targetBytes, maxLinger)
	f.latencyOffset = 0
	return f
}

// window is (last, end]: it holds both partition hours.
func (e *idemEnv) window() (last, end int64) {
	return e.hour.UnixNano(), e.hour.Add(3 * time.Hour).UnixNano()
}

// p1/p2 are the two partition hours' partition names.
func (e *idemEnv) p1() string { return partitionFromNano(e.hour.Add(10 * time.Minute).UnixNano()) }
func (e *idemEnv) p2() string { return partitionFromNano(e.hour.Add(70 * time.Minute).UnixNano()) }

// seed ingests idemRowsPerGroup rows for each of the two tenants in each of the
// two partitions: four objects per window.
func (e *idemEnv) seed() {
	for _, tenant := range []logstorage.TenantID{idemTenantA, idemTenantB} {
		ingestLogAt(e.t, e.st, tenant, e.hour.Add(10*time.Minute).UnixNano(), e.hour.Add(20*time.Minute).UnixNano(), idemRowsPerGroup)
		ingestLogAt(e.t, e.st, tenant, e.hour.Add(70*time.Minute).UnixNano(), e.hour.Add(80*time.Minute).UnixNano(), idemRowsPerGroup)
	}
	e.st.DebugFlush()
}

func (e *idemEnv) id(last, end int64, tenant logstorage.TenantID, partition string) string {
	return windowBatchID(last, end, tenant.AccountID, tenant.ProjectID, partition)
}

func (e *idemEnv) uploads() (keys, twice, once int) {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	for _, n := range e.u.uploaded {
		keys++
		switch {
		case n == 1:
			once++
		case n >= 2:
			twice++
		}
	}
	return keys, twice, once
}

func (e *idemEnv) statRows() int64 {
	e.statMu.Lock()
	defer e.statMu.Unlock()
	return e.statRow
}

func readWatermarkFile(t *testing.T, f *BufferFlusher) flushWatermark {
	t.Helper()
	b, err := os.ReadFile(f.watermarkPath)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	var wm flushWatermark
	if err := json.Unmarshal(b, &wm); err != nil {
		t.Fatalf("watermark json: %v", err)
	}
	return wm
}

func mustCommitted(t *testing.T, m *manifest.Manifest, wantRows int64, wantFiles int, when string) {
	t.Helper()
	if rows, files := committedRows(m); rows != wantRows || files != wantFiles {
		t.Fatalf("%s: committed %d rows in %d files, want %d in %d", when, rows, files, wantRows, wantFiles)
	}
}

// One of four uploads fails. The next tick retries exactly the same window: the
// three objects already written are overwritten under the same key (never
// written again under a new name), the failed one is written, and the row count
// is exact.
func TestBufferFlusher_PartialFailureRetriesTheExactWindow(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failFor.Store(e.id(last, end, idemTenantB, e.p2()))

	got := f.tick(context.Background(), last, end)
	if got != last {
		t.Fatalf("watermark advanced to %d despite a failed upload", got)
	}
	mustCommitted(t, e.m, 3*idemRowsPerGroup, 3, "after the partial failure")
	wm := readWatermarkFile(t, f)
	if wm.LastFlushWindowEndNs != last || wm.PendingWindowEndNs != end {
		t.Fatalf("watermark file = %+v, want last=%d pending=%d recorded before the uploads", wm, last, end)
	}

	e.failFor.Store("")
	// A later clock must not widen the window.
	got = f.tick(context.Background(), last, end+int64(time.Hour))
	if got != end {
		t.Fatalf("after the retry the watermark = %d, want the pending end %d (not a larger window)", got, end)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the retry")
	keys, twice, once := e.uploads()
	if keys != 4 || twice != 3 || once != 1 {
		t.Fatalf("uploaded %d distinct keys (%d written twice, %d once), want 4 keys: the 3 from attempt 1 overwritten, the failed one written once", keys, twice, once)
	}
	for _, tenant := range []logstorage.TenantID{idemTenantA, idemTenantB} {
		for _, p := range []string{e.p1(), e.p2()} {
			id := e.id(last, end, tenant, p)
			found := false
			for k := range e.u.uploaded {
				if strings.Contains(k, id) && strings.Contains(k, p) {
					found = true
				}
			}
			if !found {
				t.Errorf("no object named for tenant %+v partition %s (id %s)", tenant, p, id)
			}
		}
	}
	wm = readWatermarkFile(t, f)
	if wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file after success = %+v, want last=%d and no pending", wm, end)
	}
	if f.pending != 0 {
		t.Fatalf("in-memory pending = %d after success", f.pending)
	}
	if got := e.statRows(); got != 4*idemRowsPerGroup {
		t.Fatalf("stats counted %d rows, want %d (rewritten objects count only their difference)", got, 4*idemRowsPerGroup)
	}
}

// The process dies after the uploads and before the watermark is saved. After a
// restart the pending window is retried to the same objects: nothing is written
// twice.
func TestBufferFlusher_CrashAfterUploadsBeforeWatermarkDoesNotDuplicate(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()

	// What tick does up to the crash point: record the window, upload it.
	if err := f.saveWatermark(last); err != nil {
		t.Fatal(err)
	}
	if err := f.savePending(last, end); err != nil {
		t.Fatal(err)
	}
	f.buffer.DebugFlush()
	collected, n, err := f.collectWindow(context.Background(), last, end)
	if err != nil || n != 4*idemRowsPerGroup {
		t.Fatalf("collect: n=%d err=%v", n, err)
	}
	if err := f.flushCollected(context.Background(), collected, last, end); err != nil {
		t.Fatal(err)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "before the crash")
	// crash: no saveWatermark(end)

	e.restart()
	f2 := e.flusher(1, time.Nanosecond)
	restored := f2.loadWatermark(time.Now().UnixNano())
	if restored != last || f2.pending != end {
		t.Fatalf("restored watermark=%d pending=%d, want %d and %d", restored, f2.pending, last, end)
	}
	got := f2.tick(context.Background(), restored, end+int64(time.Hour))
	if got != end {
		t.Fatalf("watermark after the recovery tick = %d, want %d", got, end)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the recovery tick")
	if keys, twice, once := e.uploads(); keys != 4 || twice != 4 || once != 0 {
		t.Fatalf("uploaded %d keys (%d twice, %d once), want the same 4 keys each written twice", keys, twice, once)
	}
	wm := readWatermarkFile(t, f2)
	if wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file = %+v", wm)
	}
}

// Rows that arrive inside the pending window between a failed attempt and its
// retry are included; the rewritten object's manifest entry describes the
// superset and the total is exact.
func TestBufferFlusher_LateRowsInThePendingWindowAreIncludedOnce(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failFor.Store(e.id(last, end, idemTenantB, e.p2()))

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d despite a failed upload", got)
	}
	mustCommitted(t, e.m, 3*idemRowsPerGroup, 3, "attempt 1")

	// Late rows: into an object attempt 1 already wrote (tenant A, partition 1)
	// and into the one that failed (tenant B, partition 2).
	const late = 5
	ingestLogAt(t, e.st, idemTenantA, e.hour.Add(21*time.Minute).UnixNano(), e.hour.Add(25*time.Minute).UnixNano(), late)
	ingestLogAt(t, e.st, idemTenantB, e.hour.Add(75*time.Minute).UnixNano(), e.hour.Add(79*time.Minute).UnixNano(), late)

	e.failFor.Store("")
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	total := int64(4*idemRowsPerGroup + 2*late)
	mustCommitted(t, e.m, total, 4, "after the retry")
	if got := e.statRows(); got != total {
		t.Fatalf("stats counted %d rows, want %d", got, total)
	}
	idA := e.id(last, end, idemTenantA, e.p1())
	seen := false
	for _, part := range e.m.AllFiles() {
		for _, fi := range part {
			if strings.Contains(fi.Key, idA) {
				seen = true
				if fi.RowCount != idemRowsPerGroup+late {
					t.Errorf("rewritten object %s has RowCount %d, want the superset %d", fi.Key, fi.RowCount, idemRowsPerGroup+late)
				}
			}
		}
	}
	if !seen {
		t.Fatal("the rewritten object is not in the manifest")
	}
	if keys, _, _ := e.uploads(); keys != 4 {
		t.Fatalf("uploaded %d distinct keys, want 4", keys)
	}
}

// A pending window is flushed although the size gate would hold it: its objects
// may already exist, so it must reach a commit.
func TestBufferFlusher_PendingWindowIgnoresTheSizeGate(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	last, end := e.window()

	// Attempt 1 (small gate) fails on every upload and leaves the window pending.
	f1 := e.flusher(1, time.Nanosecond)
	e.failFor.Store("parquet")
	if got := f1.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.failFor.Store("")

	// After a restart the gate is huge: nothing would flush a fresh window.
	f2 := e.flusher(1<<40, 24*time.Hour)
	last2 := f2.loadWatermark(time.Now().UnixNano())
	if f2.pending != end {
		t.Fatalf("pending = %d, want %d", f2.pending, end)
	}
	if got := f2.tick(context.Background(), last2, end+int64(time.Hour)); got != end {
		t.Fatalf("the pending window was held by the size gate: watermark = %d, want %d", got, end)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the gate-ignoring retry")

	// A fresh window is still gated.
	ingestLogAt(t, e.st, idemTenantA, end+int64(time.Minute), end+int64(2*time.Minute), 3)
	if got := f2.tick(context.Background(), end, end+int64(time.Hour)); got != end {
		t.Fatalf("a fresh sub-target window flushed despite the gate: watermark = %d", got)
	}
	if f2.pending != 0 {
		t.Fatalf("a gated window must not be recorded as pending, got %d", f2.pending)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "fresh window held")
}

// windowBatchID is a pure function of (window, tenant, partition).
func TestWindowBatchID_DeterministicAndSensitiveToEveryInput(t *testing.T) {
	const p = "dt=2026-05-03/hour=14"
	base := windowBatchID(100, 200, 1, 2, p)
	if again := windowBatchID(100, 200, 1, 2, p); again != base {
		t.Fatalf("not deterministic: %q vs %q", base, again)
	}
	if base == "" {
		t.Fatal("empty batch id")
	}
	variants := map[string]string{
		"start":     windowBatchID(101, 200, 1, 2, p),
		"end":       windowBatchID(100, 201, 1, 2, p),
		"account":   windowBatchID(100, 200, 2, 2, p),
		"project":   windowBatchID(100, 200, 1, 3, p),
		"partition": windowBatchID(100, 200, 1, 2, "dt=2026-05-03/hour=15"),
		"swapped":   windowBatchID(200, 100, 1, 2, p),
		"acct/proj": windowBatchID(100, 200, 2, 1, p),
	}
	seen := map[string]string{base: "base"}
	for name, id := range variants {
		if prev, dup := seen[id]; dup {
			t.Errorf("%s collides with %s: %q", name, prev, id)
		}
		seen[id] = name
	}
}

// loadWatermark restores a pending window only when it is ahead of the
// committed watermark.
func TestBufferFlusher_LoadWatermarkPendingHandling(t *testing.T) {
	cases := []struct {
		name        string
		json        string
		wantLast    int64
		wantPending int64
	}{
		{"pending ahead is restored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":250,"version":2}`, 100, 250},
		{"pending equal to last is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":100,"version":2}`, 100, 0},
		{"pending behind last is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":40,"version":2}`, 100, 0},
		{"no pending (older file)", `{"last_flush_window_end_ns":100,"version":1}`, 100, 0},
		{"corrupt file", `{nope`, 7, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
			if err := os.WriteFile(f.watermarkPath, []byte(tc.json), 0o600); err != nil {
				t.Fatal(err)
			}
			want := tc.wantLast
			if got := f.loadWatermark(7); got != want {
				t.Fatalf("last = %d, want %d", got, want)
			}
			if f.pending != tc.wantPending {
				t.Fatalf("pending = %d, want %d", f.pending, tc.wantPending)
			}
		})
	}
	// savePending/saveWatermark round trip.
	f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
	if err := f.savePending(10, 20); err != nil {
		t.Fatal(err)
	}
	g := NewBufferFlusher(nil, nil, filepath.Dir(f.watermarkPath), nil, 0, 0)
	if last := g.loadWatermark(1); last != 10 || g.pending != 20 {
		t.Fatalf("round trip: last=%d pending=%d", last, g.pending)
	}
	if err := g.saveWatermark(20); err != nil {
		t.Fatal(err)
	}
	h := NewBufferFlusher(nil, nil, filepath.Dir(f.watermarkPath), nil, 0, 0)
	if last := h.loadWatermark(1); last != 20 || h.pending != 0 {
		t.Fatalf("after commit: last=%d pending=%d", last, h.pending)
	}
}

// The same partial failure, but the process restarts before the retry: the
// pending window comes from the watermark file, so the retry still overwrites
// the objects the first attempt wrote.
func TestBufferFlusher_PartialFailureThenRestartRetriesTheExactWindow(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failFor.Store(e.id(last, end, idemTenantA, e.p2()))
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d despite a failed upload", got)
	}
	mustCommitted(t, e.m, 3*idemRowsPerGroup, 3, "before the restart")

	e.failFor.Store("")
	e.restart()
	f2 := e.flusher(1, time.Nanosecond)
	restored := f2.loadWatermark(time.Now().UnixNano())
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the restart retry = %d, want %d", got, end)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the restart retry")
	if keys, twice, once := e.uploads(); keys != 4 || twice != 3 || once != 1 {
		t.Fatalf("uploaded %d keys (%d twice, %d once), want 4 (3 overwritten, 1 new)", keys, twice, once)
	}
}

// keyOf returns the manifest entry of the object holding tenant's rows of
// partition in the window (last, end].
func (e *idemEnv) keyOf(last, end int64, tenant logstorage.TenantID, partition string) manifest.FileInfo {
	e.t.Helper()
	id := e.id(last, end, tenant, partition)
	for _, part := range e.m.AllFiles() {
		for _, fi := range part {
			if strings.Contains(fi.Key, id) {
				return fi
			}
		}
	}
	e.t.Fatalf("no committed object for tenant %+v partition %s", tenant, partition)
	return manifest.FileInfo{}
}

// compact retires fi the way compaction does — ReplaceFiles, sources retired —
// and returns the output's key. The output carries fi's rows.
func (e *idemEnv) compact(fi manifest.FileInfo, partition string) string {
	e.t.Helper()
	out := partition + "/compacted-" + e.id(1, 2, idemTenantA, partition) + ".parquet"
	c := fi
	c.Key = out
	if !e.m.ReplaceFiles(partition, []string{fi.Key}, c) {
		e.t.Fatalf("ReplaceFiles refused %s", fi.Key)
	}
	return out
}

func (e *idemEnv) uploadCount(key string) int {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	return e.u.uploaded[key]
}

// Attempt 1 commits K and another group fails; compaction then merges K into C
// and retires it. The retry must not upload K again or add it back: C already
// carries its rows.
func TestBufferFlusher_RetryDoesNotReAddAKeyCompactedAway(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failFor.Store(e.id(last, end, idemTenantB, e.p2()))
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	k := e.keyOf(last, end, idemTenantA, e.p1())
	c := e.compact(k, e.p1())
	superseded0 := metrics.InsertRowsSuperseded.Get()

	e.failFor.Store("")
	// Bounded: a retry that requeued the superseded group would never finish.
	got := last
	for i := 0; i < 3 && got != end; i++ {
		got = f.tick(context.Background(), last, end+int64(time.Hour))
	}
	if got != end {
		t.Fatalf("the retry did not complete: watermark = %d, want %d", got, end)
	}
	if n := e.uploadCount(k.Key); n != 1 {
		t.Errorf("%s uploaded %d times, want only attempt 1's", k.Key, n)
	}
	if e.m.HasKey(k.Key) {
		t.Error("the compacted-away key is live again")
	}
	if !e.m.HasKey(c) {
		t.Error("the compaction output disappeared")
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the retry")
	// The skipped group's rows (its whole group, K's) are counted; they are
	// carried by C.
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; int64(d) != k.RowCount {
		t.Errorf("superseded counter rose by %d, want %d (the skipped group)", d, k.RowCount)
	}
}

// K is retired while the retry's own upload of it is in flight: the commit
// refuses it, K is not live, the object the retry just wrote is queued for
// deletion, and its rows are counted.
func TestBufferFlusher_KeyRetiredDuringTheRetryUploadIsReclaimed(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failFor.Store(e.id(last, end, idemTenantB, e.p2()))
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	k := e.keyOf(last, end, idemTenantA, e.p1())
	var c string
	var once sync.Once
	e.hook.Store(func(key string) {
		if key == k.Key {
			once.Do(func() {
				c = e.compact(k, e.p1())
				// The compactor's delete of K lands and is confirmed before
				// this upload does, so nothing owes the object a delete now.
				e.m.ConfirmDeleted(k.Key)
			})
		}
	})
	superseded0 := metrics.InsertRowsSuperseded.Get()
	statRows0 := e.statRows()

	e.failFor.Store("")
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if c == "" {
		t.Fatal("the hook never retired K: the commit path was not exercised")
	}
	if e.m.HasKey(k.Key) {
		t.Error("the commit re-added a key retired during the upload")
	}
	rk, ok := e.m.LookupRetired(k.Key)
	if !ok || !rk.Reclaim || rk.Deleted {
		t.Errorf("LookupRetired(%s) = %+v, %v: want retired with Reclaim owed (and not settled as Deleted) so the re-uploaded object is deleted", k.Key, rk, ok)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the retry")
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; int64(d) != k.RowCount {
		t.Errorf("superseded counter rose by %d, want %d", d, k.RowCount)
	}
	if d := e.statRows() - statRows0; d != idemRowsPerGroup {
		t.Errorf("stats rose by %d rows, want only the failed group's %d", d, idemRowsPerGroup)
	}
}

// The watermark cannot be saved after all uploads succeeded (its directory
// turns read-only): the window stays pending on disk, and a new flusher retries
// it to the same objects.
func TestBufferFlusher_WatermarkWriteFailsAfterUploads(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for root")
	}
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	if err := f.saveWatermark(last); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(e.wmDir, 0o700) })
	var once sync.Once
	e.hook.Store(func(string) {
		once.Do(func() {
			if err := os.Chmod(e.wmDir, 0o500); err != nil {
				t.Errorf("chmod: %v", err)
			}
		})
	})

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d although it could not be saved", got)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the uploads")
	wm := readWatermarkFile(t, f)
	if wm.LastFlushWindowEndNs != last || wm.PendingWindowEndNs != end {
		t.Fatalf("watermark file = %+v, want last=%d pending=%d", wm, last, end)
	}

	if err := os.Chmod(e.wmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	e.hook.Store(func(string) {})
	f2 := e.flusher(1, time.Nanosecond)
	restored := f2.loadWatermark(time.Now().UnixNano())
	if restored != last || f2.pending != end {
		t.Fatalf("restored last=%d pending=%d", restored, f2.pending)
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the retry = %d, want %d", got, end)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the retry")
	if keys, twice, once := e.uploads(); keys != 4 || twice != 4 || once != 0 {
		t.Fatalf("uploaded %d keys (%d twice, %d once), want the same 4 keys each written twice", keys, twice, once)
	}
}

// Tenants are flushed in a fixed order and a failure does not stop the rest of
// the window: with the first tenant's first group failing, every other group
// is still attempted — and committed — in the same attempt.
func TestBufferFlusher_AFailedGroupDoesNotStopTheRestOfTheWindow(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failFor.Store(e.id(last, end, idemTenantA, e.p1())) // the first group in flush order

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	mustCommitted(t, e.m, 3*idemRowsPerGroup, 3, "the groups after the failed one")
	if keys, _, _ := e.uploads(); keys != 3 {
		t.Fatalf("%d groups uploaded in the failing attempt, want the 3 after the failed one", keys)
	}
	e.failFor.Store("")
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	mustCommitted(t, e.m, 4*idemRowsPerGroup, 4, "after the retry")
}
