package parquets3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Buffer-flush idempotency (#45). The invariant under test: an object's bytes
// never change once uploaded, and no key is uploaded twice with different
// content. Every test here runs on a faultyUploader that records the sha256 of
// each upload, and durabilityWriter fails the test at cleanup if any key was
// stored (or attempted) with two different byte sequences.

var (
	idemTenantA = logstorage.TenantID{AccountID: 1, ProjectID: 2}
	idemTenantB = logstorage.TenantID{AccountID: 2, ProjectID: 3}
)

const idemRowsPerGroup = 10

// idemEnv is a buffer, a writer whose uploads go to a faultyUploader, and a
// watermark directory: everything a flusher needs, and what survives a restart.
type idemEnv struct {
	t        *testing.T
	bufDir   string
	wmDir    string
	st       *membuffer.Store
	u        *faultyUploader
	bw       *BatchWriter
	m        *manifest.Manifest
	hour     time.Time       // start of the first partition hour
	failMode atomic.Value    // func(key string) bool: the uploads that fail
	hook     atomic.Value    // func(key string): runs inside every upload, before it is stored
	statHook atomic.Value    // func(): runs after each group's manifest commit
	statMu   sync.Mutex      //
	statRow  int64           //
	deleted  map[string]bool // objects removed from the fake bucket
	compacts int             //
}

func newIdemEnv(t *testing.T) *idemEnv {
	t.Helper()
	e := &idemEnv{t: t, bufDir: t.TempDir(), wmDir: t.TempDir(), deleted: map[string]bool{}}
	e.failNone()
	e.hook.Store(func(string) {})
	e.statHook.Store(func() {})
	e.hour = time.Now().Add(-4 * time.Hour).Truncate(time.Hour)
	e.u = &faultyUploader{fail: func(key string) error {
		e.hook.Load().(func(string))(key)
		if e.failMode.Load().(func(string) bool)(key) {
			return errPutFailed
		}
		return nil
	}}
	e.bw, e.m = durabilityWriter(t, e.u)
	// Tenants get their own prefix so a key names its tenant.
	e.bw.SetTenantPrefix(func(account, project uint32) string { return fmt.Sprintf("t%d-%d/", account, project) })
	e.bw.SetStatsCallback(func(_, _ uint32, _, _, rows int64, _ string) {
		e.statMu.Lock()
		e.statRow += rows
		e.statMu.Unlock()
		e.statHook.Load().(func())()
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
//
// A restarted flusher collects the groups it never stored afresh, and Parquet
// encoding is not required to be byte-identical across encodings of the same
// rows, so from here on a failed (never stored) attempt may differ from the next
// attempt of its key. What must still hold, and is still checked, is that the
// bytes stored under a key never change and that a key stored once is not sent
// again (the tests count attempts per key).
func (e *idemEnv) restart() {
	e.t.Helper()
	e.u.mu.Lock()
	e.u.allowAttemptDrift = true
	e.u.mu.Unlock()
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

// resume is what a restarted process does: a new flusher loads the watermark.
func (e *idemEnv) resume() (*BufferFlusher, int64) {
	e.t.Helper()
	f := e.flusher(1, time.Nanosecond)
	return f, f.loadWatermark(time.Now().UnixNano())
}

// window is (last, end]: it holds both partition hours.
func (e *idemEnv) window() (last, end int64) {
	return e.hour.UnixNano(), e.hour.Add(3 * time.Hour).UnixNano()
}

// p1/p2 are the two partition hours' partition names.
func (e *idemEnv) p1() string { return partitionFromNano(e.hour.Add(10 * time.Minute).UnixNano()) }
func (e *idemEnv) p2() string { return partitionFromNano(e.hour.Add(70 * time.Minute).UnixNano()) }

// seed ingests idemRowsPerGroup rows for each of the two tenants in each of the
// two partitions: four objects per window, flushed in the order A/p1, A/p2,
// B/p1, B/p2.
func (e *idemEnv) seed() {
	for _, tenant := range []logstorage.TenantID{idemTenantA, idemTenantB} {
		ingestLogAt(e.t, e.st, tenant, e.hour.Add(10*time.Minute).UnixNano(), e.hour.Add(20*time.Minute).UnixNano(), idemRowsPerGroup)
		ingestLogAt(e.t, e.st, tenant, e.hour.Add(70*time.Minute).UnixNano(), e.hour.Add(80*time.Minute).UnixNano(), idemRowsPerGroup)
	}
	e.st.DebugFlush()
}

// late ingests n more rows for tenant into the partition p (1 or 2), inside the
// window.
func (e *idemEnv) late(tenant logstorage.TenantID, p, n int) {
	base := e.hour.Add(21 * time.Minute)
	if p == 2 {
		base = e.hour.Add(75 * time.Minute)
	}
	ingestLogAt(e.t, e.st, tenant, base.UnixNano(), base.Add(4*time.Minute).UnixNano(), n)
	e.st.DebugFlush()
}

func (e *idemEnv) tenantPrefix(tenant logstorage.TenantID) string {
	return fmt.Sprintf("t%d-%d/", tenant.AccountID, tenant.ProjectID)
}

// keyOf is the key the group (tenant, partition) of the window gets under nonce.
func (e *idemEnv) keyOf(nonce string, tenant logstorage.TenantID, partition string) string {
	last, end := e.window()
	return e.tenantPrefix(tenant) + partition + "/" + windowBatchID(nonce, last, end, tenant.AccountID, tenant.ProjectID, partition) + ".parquet"
}

func (e *idemEnv) allKeys(nonce string) []string {
	return []string{
		e.keyOf(nonce, idemTenantA, e.p1()), e.keyOf(nonce, idemTenantA, e.p2()),
		e.keyOf(nonce, idemTenantB, e.p1()), e.keyOf(nonce, idemTenantB, e.p2()),
	}
}

func (e *idemEnv) failNone() { e.failMode.Store(func(string) bool { return false }) }
func (e *idemEnv) failAll()  { e.failMode.Store(func(string) bool { return true }) }

// failGroup makes the uploads of (tenant, partition) fail.
func (e *idemEnv) failGroup(tenant logstorage.TenantID, partition string) {
	prefix := e.tenantPrefix(tenant)
	e.failMode.Store(func(key string) bool { return strings.HasPrefix(key, prefix) && strings.Contains(key, partition) })
}

func (e *idemEnv) statRows() int64 {
	e.statMu.Lock()
	defer e.statMu.Unlock()
	return e.statRow
}

func (e *idemEnv) attempts(key string) int { return e.u.attempts(key) }

func (e *idemEnv) storedCount(key string) int {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	return e.u.uploaded[key]
}

func (e *idemEnv) totalAttempts() int {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	n := 0
	for _, h := range e.u.attemptHashes {
		n += len(h)
	}
	return n
}

func (e *idemEnv) storedKeys() []string {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	var out []string
	for k := range e.u.uploaded {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (e *idemEnv) storedBytes(key string) []byte {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	return append([]byte(nil), e.u.data[key]...)
}

// parquetRows counts the rows in the object the fake bucket holds under key.
func (e *idemEnv) parquetRows(key string) int64 {
	e.t.Helper()
	b := e.storedBytes(key)
	if len(b) == 0 {
		e.t.Fatalf("no stored object %s", key)
	}
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		e.t.Fatalf("open %s: %v", key, err)
	}
	return f.NumRows()
}

// liveRows sums the rows the manifest serves. An entry adopted from a bare
// listing has no row count of its own; its object's footer supplies it.
func (e *idemEnv) liveRows() (rows int64, files int) {
	for _, part := range e.m.AllFiles() {
		for _, fi := range part {
			files++
			n := fi.RowCount
			if n == 0 {
				n = e.parquetRows(fi.Key)
			}
			rows += n
		}
	}
	return rows, files
}

func (e *idemEnv) mustLive(wantRows int64, wantFiles int, when string) {
	e.t.Helper()
	if rows, files := e.liveRows(); rows != wantRows || files != wantFiles {
		e.t.Fatalf("%s: live %d rows in %d files, want %d in %d", when, rows, files, wantRows, wantFiles)
	}
}

// list is a bucket listing applied to the manifest: every object the fake bucket
// holds and has not been deleted, plus what the manifest tracks that never went
// through it (compaction outputs). It begins after everything that happened so
// far, so it settles retirements exactly as a refresh does.
func (e *idemEnv) list() {
	e.t.Helper()
	time.Sleep(3 * time.Millisecond)
	start := time.Now()
	seen := map[string]bool{}
	var objs []manifest.ListedObject
	e.u.mu.Lock()
	for k, d := range e.u.data {
		if !e.deleted[k] {
			objs = append(objs, manifest.ListedObject{Key: k, Size: int64(len(d))})
			seen[k] = true
		}
	}
	e.u.mu.Unlock()
	for _, part := range e.m.AllFiles() {
		for _, fi := range part {
			if !seen[fi.Key] {
				objs = append(objs, manifest.ListedObject{Key: fi.Key, Size: fi.Size})
			}
		}
	}
	if !e.m.ApplyListing(objs, start) {
		e.t.Fatal("the listing was rejected")
	}
}

func (e *idemEnv) entry(key string) manifest.FileInfo {
	e.t.Helper()
	for _, part := range e.m.AllFiles() {
		for _, fi := range part {
			if fi.Key == key {
				return fi
			}
		}
	}
	e.t.Fatalf("no manifest entry for %s", key)
	return manifest.FileInfo{}
}

// compact retires fi the way compaction does — ReplaceFiles, source retired,
// source object deleted from the bucket — and returns the output's key. The
// output carries fi's rows.
func (e *idemEnv) compact(fi manifest.FileInfo, partition string) string {
	e.t.Helper()
	e.compacts++
	out := fmt.Sprintf("%s/compacted-%d.parquet", partition, e.compacts)
	c := fi
	c.Key = out
	if c.RowCount == 0 {
		c.RowCount = e.parquetRows(fi.Key)
	}
	if !e.m.ReplaceFiles(partition, []string{fi.Key}, c) {
		e.t.Fatalf("ReplaceFiles refused %s", fi.Key)
	}
	e.deleted[fi.Key] = true
	return out
}

// blockWatermarkAfter makes the watermark directory read-only once n groups of
// the window have been committed to the manifest: every upload and record
// before that succeeded, so what fails is exactly the watermark save.
func (e *idemEnv) blockWatermarkAfter(n int32) {
	e.t.Helper()
	if os.Geteuid() == 0 {
		e.t.Skip("permissions are not enforced for root")
	}
	e.t.Cleanup(func() { _ = os.Chmod(e.wmDir, 0o700) })
	var commits atomic.Int32
	e.statHook.Store(func() {
		if commits.Add(1) == n {
			if err := os.Chmod(e.wmDir, 0o500); err != nil {
				e.t.Errorf("chmod: %v", err)
			}
		}
	})
}

func (e *idemEnv) unblockWatermark() {
	e.t.Helper()
	if err := os.Chmod(e.wmDir, 0o700); err != nil {
		e.t.Fatal(err)
	}
	e.statHook.Store(func() {})
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

func hasKeyIn(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// 1. One of four uploads fails. The next tick retries exactly that group, with
// the very bytes of the first attempt; the three stored objects are not sent
// again; rows are exact and the watermark commits.
func TestBufferFlusher_PartialFailureRetriesOnlyTheFailedGroupWithIdenticalBytes(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d despite a failed upload", got)
	}
	e.mustLive(3*idemRowsPerGroup, 3, "after the partial failure")
	wm := readWatermarkFile(t, f)
	if wm.Version != 2 || wm.LastFlushWindowEndNs != last || wm.PendingWindowEndNs != end || wm.PendingNonce == "" {
		t.Fatalf("watermark file = %+v, want v2 with last=%d pending=%d and a nonce recorded before the uploads", wm, last, end)
	}
	keys := e.allKeys(wm.PendingNonce)
	failedKey := keys[3]
	if len(wm.PendingUploaded) != 3 || !sort.StringsAreSorted(wm.PendingUploaded) || hasKeyIn(wm.PendingUploaded, failedKey) {
		t.Fatalf("pending_uploaded = %v, want the 3 stored keys sorted and not %s", wm.PendingUploaded, failedKey)
	}
	if f.pending != end || f.nonce != wm.PendingNonce || len(f.failed) != 1 || f.failed[0].key != failedKey {
		t.Fatalf("in-memory state pending=%d nonce=%q failed=%d", f.pending, f.nonce, len(f.failed))
	}

	e.failNone()
	// A later clock must not widen the window.
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("after the retry the watermark = %d, want the pending end %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry")
	for _, k := range keys {
		if n := e.storedCount(k); n != 1 {
			t.Errorf("%s stored %d times, want once", k, n)
		}
		want := 1
		if k == failedKey {
			want = 2
		}
		if n := e.attempts(k); n != want {
			t.Errorf("%s attempted %d times, want %d", k, n, want)
		}
	}
	e.u.mu.Lock()
	h := e.u.attemptHashes[failedKey]
	e.u.mu.Unlock()
	if len(h) != 2 || h[0] != h[1] {
		t.Errorf("the retry of the failed group sent different bytes: %v", h)
	}
	wm = readWatermarkFile(t, f)
	if wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 || wm.PendingNonce != "" || len(wm.PendingUploaded) != 0 {
		t.Fatalf("watermark file after success = %+v, want last=%d and nothing pending", wm, end)
	}
	if f.pending != 0 || f.nonce != "" || f.uploaded != nil || f.failed != nil {
		t.Fatalf("in-memory pending state not cleared: %d %q %v %v", f.pending, f.nonce, f.uploaded, f.failed)
	}
	if got := e.statRows(); got != 4*idemRowsPerGroup {
		t.Fatalf("stats counted %d rows, want %d", got, 4*idemRowsPerGroup)
	}
}

// 2. A partial failure, then the process restarts. The stored keys were
// persisted, so they are not sent again; the group that never made it is
// collected again and uploaded; rows are exact.
func TestBufferFlusher_PartialFailureThenRestartSendsOnlyWhatWasNeverStored(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantA, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d despite a failed upload", got)
	}
	e.mustLive(3*idemRowsPerGroup, 3, "before the restart")
	nonce := readWatermarkFile(t, f).PendingNonce

	e.failNone()
	e.restart()
	f2, restored := e.resume()
	if restored != last || !f2.recovered || f2.pending != end || f2.nonce != nonce || len(f2.uploaded) != 3 {
		t.Fatalf("restored last=%d recovered=%v pending=%d nonce=%q uploaded=%d", restored, f2.recovered, f2.pending, f2.nonce, len(f2.uploaded))
	}
	e.list()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the restart retry = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the restart retry")
	keys := e.allKeys(nonce)
	for i, k := range keys {
		if n := e.storedCount(k); n != 1 {
			t.Errorf("%s stored %d times, want once", k, n)
		}
		want := 1
		if i == 1 { // A/p2, the group that failed
			want = 2
		}
		if n := e.attempts(k); n != want {
			t.Errorf("%s attempted %d times, want %d", k, n, want)
		}
	}
	if wm := readWatermarkFile(t, f2); wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file = %+v", wm)
	}
}

// 3. The process dies after every upload and before the watermark is saved (the
// watermark directory turns read-only, so the save fails and the process is then
// restarted). The restart sends nothing and commits the watermark.
func TestBufferFlusher_CrashAfterAllUploadsBeforeTheWatermarkSendsNothing(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.blockWatermarkAfter(4)

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d although it could not be saved", got)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "before the crash")
	wm := readWatermarkFile(t, f)
	if wm.PendingWindowEndNs != end || len(wm.PendingUploaded) != 4 {
		t.Fatalf("watermark file = %+v, want the window pending with all 4 keys recorded", wm)
	}
	e.unblockWatermark()

	e.restart()
	f2, restored := e.resume()
	if restored != last || !f2.recovered || len(f2.uploaded) != 4 {
		t.Fatalf("restored last=%d recovered=%v uploaded=%d", restored, f2.recovered, len(f2.uploaded))
	}
	e.list()
	before := e.totalAttempts()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the recovery tick = %d, want %d", got, end)
	}
	if after := e.totalAttempts(); after != before || before != 4 {
		t.Fatalf("uploads attempted: %d before the restart, %d after; want 4 and 4 (the restart sends nothing)", before, after)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the recovery tick")
	if wm := readWatermarkFile(t, f2); wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file = %+v", wm)
	}
}

// crashBetweenPutAndRecord runs a tick in which the first group (A/p1) is stored
// but its key cannot be recorded (the watermark directory is read-only exactly
// then), so the object is in the bucket, not in the persisted set and not in the
// manifest. It returns the flusher, the group's key and the window.
func crashBetweenPutAndRecord(t *testing.T, e *idemEnv) (f *BufferFlusher, x string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for root")
	}
	t.Cleanup(func() { _ = os.Chmod(e.wmDir, 0o700) })
	f = e.flusher(1, time.Nanosecond)
	last, end := e.window()
	prefix := e.tenantPrefix(idemTenantA)
	isX := func(key string) bool { return strings.HasPrefix(key, prefix) && strings.Contains(key, e.p1()) }
	var locked, unlocked atomic.Bool
	e.hook.Store(func(key string) {
		switch {
		case isX(key) && !locked.Load():
			locked.Store(true)
			if err := os.Chmod(e.wmDir, 0o500); err != nil {
				t.Errorf("chmod: %v", err)
			}
		case !isX(key) && locked.Load() && !unlocked.Load():
			unlocked.Store(true)
			if err := os.Chmod(e.wmDir, 0o700); err != nil {
				t.Errorf("chmod: %v", err)
			}
		}
	})
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d although a group could not be recorded", got)
	}
	e.hook.Store(func(string) {})
	nonce := readWatermarkFile(t, f).PendingNonce
	x = e.keyOf(nonce, idemTenantA, e.p1())
	if e.storedCount(x) != 1 {
		t.Fatalf("%s is not in the bucket", x)
	}
	if e.m.HasKey(x) {
		t.Fatal("the group whose record failed was committed")
	}
	if wm := readWatermarkFile(t, f); hasKeyIn(wm.PendingUploaded, x) || len(wm.PendingUploaded) != 3 {
		t.Fatalf("pending_uploaded = %v: want the 3 other keys and not %s", wm.PendingUploaded, x)
	}
	return f, x
}

// 4. The crash lands between the PUT and persisting the key: the object is in S3
// but the flusher does not know it. After the restart a listing adopts it, and
// the writer skips it (the manifest has the key): no re-upload, rows exact.
func TestBufferFlusher_CrashBetweenPutAndRecordAdoptedByListingIsNotResent(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	_, x := crashBetweenPutAndRecord(t, e)
	last, end := e.window()

	e.restart()
	f2, restored := e.resume()
	if restored != last || !f2.recovered || len(f2.uploaded) != 3 || f2.pending != end {
		t.Fatalf("restored last=%d recovered=%v uploaded=%d", restored, f2.recovered, len(f2.uploaded))
	}
	e.list()
	if !e.m.HasKey(x) {
		t.Fatal("the listing did not adopt the stored object")
	}
	superseded0 := metrics.InsertRowsSuperseded.Get()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the recovery tick = %d, want %d", got, end)
	}
	if n := e.attempts(x); n != 1 {
		t.Errorf("%s uploaded %d times, want only the first", x, n)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the recovery tick")
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != 0 {
		t.Errorf("superseded counter rose by %d: an adopted live object is not superseded", d)
	}
}

// 4b. As 4, but the adopted object is compacted before the retry: the key is
// retired, not live. The writer skips it and counts its rows as superseded; the
// compacted output carries them.
func TestBufferFlusher_CrashBetweenPutAndRecordThenCompactedIsNotResent(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	_, x := crashBetweenPutAndRecord(t, e)
	_, end := e.window()

	e.restart()
	f2, restored := e.resume()
	e.list()
	e.compact(e.entry(x), e.p1())
	superseded0 := metrics.InsertRowsSuperseded.Get()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the recovery tick = %d, want %d", got, end)
	}
	if n := e.attempts(x); n != 1 {
		t.Errorf("%s uploaded %d times, want only the first", x, n)
	}
	if e.m.HasKey(x) {
		t.Error("the compacted key is live again")
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the recovery tick")
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != idemRowsPerGroup {
		t.Errorf("superseded counter rose by %d, want %d", d, idemRowsPerGroup)
	}
}

// 5. Until a bucket listing has been applied, "the manifest lacks this key" says
// nothing about whether an earlier attempt stored it: a recovered tick waits.
func TestBufferFlusher_RecoveredTickWaitsForTheFirstListing(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.failNone()
	e.restart()
	f2, restored := e.resume()
	fileBefore, err := os.ReadFile(f2.watermarkPath)
	if err != nil {
		t.Fatal(err)
	}
	attempts0 := e.totalAttempts()

	if e.m.Listed() {
		t.Fatal("the manifest was listed before the test wanted it to be")
	}
	for i := 0; i < 3; i++ {
		if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != last {
			t.Fatalf("tick %d before the first listing moved the watermark to %d", i, got)
		}
	}
	if e.totalAttempts() != attempts0 {
		t.Fatalf("uploads were attempted before the first listing: %d -> %d", attempts0, e.totalAttempts())
	}
	if !f2.recovered || f2.pending != end {
		t.Fatalf("the pending window was dropped: recovered=%v pending=%d", f2.recovered, f2.pending)
	}
	if fileAfter, _ := os.ReadFile(f2.watermarkPath); !bytes.Equal(fileBefore, fileAfter) {
		t.Fatal("the watermark file changed while waiting for a listing")
	}

	e.list()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the listing = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the listing")
}

// 6a. K is stored and recorded, then compacted into C, and a real listing
// forgets K's retired record (the object is gone). After a restart the retry
// must not send K again: the persisted set still knows it. Without that, K —
// neither live nor retired any more — would be uploaded and its rows would
// appear twice (in C and in K).
func TestBufferFlusher_RetiredRecordForgottenByListingStillNoResendAfterRestart(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	nonce := readWatermarkFile(t, f).PendingNonce
	k := e.keyOf(nonce, idemTenantA, e.p1())
	e.compact(e.entry(k), e.p1())

	e.failNone()
	e.restart()
	f2, restored := e.resume()
	e.list()
	if e.m.IsRetired(k) || e.m.HasKey(k) {
		t.Fatalf("precondition: K must be neither retired nor live after the listing (retired=%v live=%v)", e.m.IsRetired(k), e.m.HasKey(k))
	}
	superseded0 := metrics.InsertRowsSuperseded.Get()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.attempts(k); n != 1 {
		t.Errorf("%s was sent %d times, want only its first", k, n)
	}
	if e.m.HasKey(k) {
		t.Error("the compacted-away key is live again")
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry: C carries K's rows, the failed group is written")
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != 0 {
		t.Errorf("superseded counter rose by %d: K never reached the writer", d)
	}
}

// 6b. Compaction reads K; a retry of the window happens; then compaction
// publishes. K's object and manifest entry are what compaction read — the retry
// did not rewrite K — so the publish succeeds and no row is lost.
func TestBufferFlusher_RetryBetweenCompactionReadAndPublishLosesNothing(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	nonce := readWatermarkFile(t, f).PendingNonce
	k := e.keyOf(nonce, idemTenantA, e.p1())
	read := e.entry(k) // what compaction read
	bytesRead := e.storedBytes(k)

	e.failNone()
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.attempts(k); n != 1 {
		t.Errorf("%s was sent %d times during the retry, want only its first", k, n)
	}
	if now := e.entry(k); now.RowCount != read.RowCount || now.Size != read.Size {
		t.Errorf("K's manifest entry changed under compaction: %+v -> %+v", read, now)
	}
	if !bytes.Equal(bytesRead, e.storedBytes(k)) {
		t.Error("K's object changed under compaction")
	}
	e.compact(read, e.p1()) // fatals if ReplaceFiles refuses
	e.mustLive(4*idemRowsPerGroup, 4, "after the publish")
}

// 6c. After a restart the buffer returns fewer rows for a group that was already
// stored (the filter drops half of that tenant's rows now). The stored group is
// not sent again — a smaller object must never replace a durable one.
func TestBufferFlusher_FewerRowsAfterRestartDoNotResendAStoredGroup(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	nonce := readWatermarkFile(t, f).PendingNonce
	e.failNone()
	e.restart()

	var seen, dropped atomic.Int64
	keep := func(account, _ uint32, _ string) bool {
		if account != idemTenantA.AccountID {
			return true
		}
		if seen.Add(1)%2 == 0 {
			dropped.Add(1)
			return false
		}
		return true
	}
	f2 := NewBufferFlusher(e.bw, e.st, e.wmDir, keep, 1, time.Nanosecond)
	f2.latencyOffset = 0
	restored := f2.loadWatermark(time.Now().UnixNano())
	e.list()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if dropped.Load() == 0 {
		t.Fatal("the filter dropped nothing: the scenario was not exercised")
	}
	for _, k := range e.allKeys(nonce)[:3] {
		if n := e.attempts(k); n != 1 {
			t.Errorf("%s was sent %d times, want only its first", k, n)
		}
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry")
}

// 7. The watermark cannot be saved after every group is stored and committed:
// further ticks send nothing (the groups are done) and only retry the save; once
// it works, the watermark commits.
func TestBufferFlusher_WatermarkSaveFailureAfterUploadsNeverReuploads(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.blockWatermarkAfter(4)

	for i := 0; i < 4; i++ {
		if got := f.tick(context.Background(), last, end+int64(i)*int64(time.Hour)); got != last {
			t.Fatalf("tick %d moved the watermark to %d although it could not be saved", i, got)
		}
	}
	if n := e.totalAttempts(); n != 4 {
		t.Fatalf("%d upload attempts over 4 ticks, want exactly the 4 of the first", n)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "with the watermark stuck")
	if len(f.failed) != 0 || f.pending != end {
		t.Fatalf("state after the failed saves: failed=%d pending=%d", len(f.failed), f.pending)
	}

	e.unblockWatermark()
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the directory recovered = %d, want %d", got, end)
	}
	if n := e.totalAttempts(); n != 4 {
		t.Fatalf("%d upload attempts in total, want 4", n)
	}
	if wm := readWatermarkFile(t, f); wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file = %+v", wm)
	}
	if got := e.statRows(); got != 4*idemRowsPerGroup {
		t.Fatalf("stats counted %d rows, want %d", got, 4*idemRowsPerGroup)
	}
}

// 8a. Late rows, in-process: rows that arrive for the pending window after its
// first attempt are not added by the retry. The retry sends the same groups; the
// count is exactly what the first attempt collected. (They stay in the buffer,
// served by the read-merge, but a committed window is never collected again.)
func TestBufferFlusher_LateRowsAreNotAddedByAnInProcessRetry(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	const late = 5
	e.late(idemTenantA, 1, late) // into a stored group
	e.late(idemTenantB, 2, late) // into the group that failed

	e.failNone()
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry: exactly the rows of the first collection")
	if _, n, err := f.collectWindow(context.Background(), last, end); err != nil || n != 4*idemRowsPerGroup+2*late {
		t.Fatalf("the late rows should still be in the buffer: n=%d err=%v", n, err)
	}
	if got := e.statRows(); got != 4*idemRowsPerGroup {
		t.Fatalf("stats counted %d rows, want %d", got, 4*idemRowsPerGroup)
	}
}

// 8b. Late rows, after a restart: the window is collected again, so a group that
// was never stored includes them; a stored group does not change.
func TestBufferFlusher_LateRowsAfterARestartReachGroupsNeverStored(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	// The failed group is collected afresh after the restart, with more rows than
	// its failed attempt had: different bytes under the same key are expected (it
	// was never stored); restart() relaxes the attempt-level check accordingly.
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	nonce := readWatermarkFile(t, f).PendingNonce
	const late = 5
	e.late(idemTenantA, 1, late) // its group is stored: not re-sent
	e.late(idemTenantB, 2, late) // its group never was

	e.failNone()
	e.restart()
	f2, restored := e.resume()
	e.list()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup+late, 4, "after the restart")
	b2 := e.keyOf(nonce, idemTenantB, e.p2())
	if got := e.parquetRows(b2); got != idemRowsPerGroup+late {
		t.Errorf("the group never stored has %d rows, want %d (its own plus the late ones)", got, idemRowsPerGroup+late)
	}
	if got := e.parquetRows(e.keyOf(nonce, idemTenantA, e.p1())); got != idemRowsPerGroup {
		t.Errorf("the stored group has %d rows, want it unchanged at %d", got, idemRowsPerGroup)
	}
}

// 9. Two nodes flushing the same window derive different keys: the nonce is
// drawn per pending window, so they never write different bytes to one key.
func TestBufferFlusher_TwoFlushersOfTheSameWindowUseDistinctKeys(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	last, end := e.window()
	f1 := e.flusher(1, time.Nanosecond)
	f2 := NewBufferFlusher(e.bw, e.st, t.TempDir(), nil, 1, time.Nanosecond)
	f2.latencyOffset = 0

	if got := f1.tick(context.Background(), last, end); got != end {
		t.Fatalf("flusher 1 watermark = %d", got)
	}
	n1 := len(e.storedKeys())
	if got := f2.tick(context.Background(), last, end); got != end {
		t.Fatalf("flusher 2 watermark = %d", got)
	}
	keys := e.storedKeys()
	if n1 != 4 || len(keys) != 8 {
		t.Fatalf("stored %d keys after flusher 1 and %d after flusher 2, want 4 and 8 (distinct)", n1, len(keys))
	}
	for _, k := range keys {
		if n := e.storedCount(k); n != 1 {
			t.Errorf("%s stored %d times", k, n)
		}
	}
}

// 10. afterUpload failing fails the group: it is not committed, keeps its key
// and encoded bytes, and its retry sends the same bytes.
func TestUploadGroup_AfterUploadErrorFailsTheGroupAndTheRetrySendsIdenticalBytes(t *testing.T) {
	u := &faultyUploader{}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	groups := bw.buildLogGroups(partitionFromNano(base.UnixNano()), rowsAt(base, 20, 0), nil)
	if len(groups) != 1 {
		t.Fatalf("%d groups, want 1", len(groups))
	}
	up := groups[0]
	calls := 0
	up.afterUpload = func(string) error {
		calls++
		if calls == 1 {
			return errors.New("no space left on device")
		}
		return nil
	}

	if err := bw.uploadLogGroup(context.Background(), up); err == nil {
		t.Fatal("a failing afterUpload must fail the group")
	}
	if rows, files := committedRows(m); rows != 0 || files != 0 {
		t.Fatalf("committed %d rows in %d files although the group failed", rows, files)
	}
	if up.key == "" || up.result == nil {
		t.Fatal("the failed group lost its key or encoded bytes")
	}
	key := up.key
	if err := bw.uploadLogGroup(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	if up.key != key {
		t.Errorf("key changed on retry: %s -> %s", key, up.key)
	}
	u.mu.Lock()
	h := u.attemptHashes[key]
	u.mu.Unlock()
	if len(h) != 2 || h[0] != h[1] {
		t.Errorf("the retry sent different bytes: %v", h)
	}
	if rows, files := committedRows(m); rows != 20 || files != 1 {
		t.Fatalf("committed %d rows in %d files, want 20 in 1", rows, files)
	}
}

// A group whose key is already live, or retired, needs no upload; its
// afterUpload still runs so the caller records it as done. Only a retired one is
// counted as superseded.
func TestUploadGroup_SettledGroupsRunAfterUploadWithoutUploading(t *testing.T) {
	u := &faultyUploader{}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	partition := partitionFromNano(base.UnixNano())
	mk := func(batch string) (*logGroupUpload, *[]string) {
		groups := bw.buildLogGroups(partition, rowsAt(base, 7, 0), func(uint32, uint32) string { return batch })
		var got []string
		groups[0].afterUpload = func(k string) error { got = append(got, k); return nil }
		return groups[0], &got
	}

	live, liveCalls := mk("aaaaaaaaaaaaaaaa")
	m.AddFile(partition, manifest.FileInfo{Key: bw.assignLogKey(live), Size: 10, RowCount: 7, MinTimeNs: base.UnixNano(), MaxTimeNs: base.UnixNano()})
	retired, retiredCalls := mk("bbbbbbbbbbbbbbbb")
	m.Retire(bw.assignLogKey(retired), "compacted", true)

	superseded0 := metrics.InsertRowsSuperseded.Get()
	if err := bw.uploadLogGroup(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != 0 {
		t.Errorf("a live key counted %d superseded rows", d)
	}
	if err := bw.uploadLogGroup(context.Background(), retired); err != nil {
		t.Fatal(err)
	}
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != 7 {
		t.Errorf("a retired key counted %d superseded rows, want 7", d)
	}
	if len(*liveCalls) != 1 || len(*retiredCalls) != 1 {
		t.Errorf("afterUpload calls: live=%d retired=%d, want 1 each", len(*liveCalls), len(*retiredCalls))
	}
	if n := e2eAttempts(u); n != 0 {
		t.Errorf("%d uploads for two settled groups, want 0", n)
	}
	if m.HasKey(retired.key) {
		t.Error("the retired key was added")
	}
}

func e2eAttempts(u *faultyUploader) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, h := range u.attemptHashes {
		n += len(h)
	}
	return n
}

// A pending window is flushed although the size gate would hold it: its objects
// may already exist, so it must reach a commit. A fresh window is still gated.
func TestBufferFlusher_PendingWindowIgnoresTheSizeGate(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	last, end := e.window()

	// Attempt 1 (small gate) fails on every upload and leaves the window pending.
	f1 := e.flusher(1, time.Nanosecond)
	e.failAll()
	if got := f1.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.failNone()

	// After a restart the gate is huge: nothing would flush a fresh window.
	e.restart()
	f2 := e.flusher(1<<40, 24*time.Hour)
	last2 := f2.loadWatermark(time.Now().UnixNano())
	if f2.pending != end {
		t.Fatalf("pending = %d, want %d", f2.pending, end)
	}
	e.list()
	if got := f2.tick(context.Background(), last2, end+int64(time.Hour)); got != end {
		t.Fatalf("the pending window was held by the size gate: watermark = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the gate-ignoring retry")

	// A fresh window is still gated.
	ingestLogAt(t, e.st, idemTenantA, end+int64(time.Minute), end+int64(2*time.Minute), 3)
	if got := f2.tick(context.Background(), end, end+int64(time.Hour)); got != end {
		t.Fatalf("a fresh sub-target window flushed despite the gate: watermark = %d", got)
	}
	if f2.pending != 0 {
		t.Fatalf("a gated window must not be recorded as pending, got %d", f2.pending)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "fresh window held")
}

// windowBatchID is a pure function of (nonce, window, tenant, partition), 16 hex
// characters like any batch id.
func TestWindowBatchID_DeterministicAndSensitiveToEveryInput(t *testing.T) {
	const p = "dt=2026-05-03/hour=14"
	base := windowBatchID("n0", 100, 200, 1, 2, p)
	if again := windowBatchID("n0", 100, 200, 1, 2, p); again != base {
		t.Fatalf("not deterministic: %q vs %q", base, again)
	}
	if len(base) != 16 || strings.Trim(base, "0123456789abcdef") != "" {
		t.Fatalf("batch id %q is not 16 hex characters", base)
	}
	variants := map[string]string{
		"nonce":     windowBatchID("n1", 100, 200, 1, 2, p),
		"start":     windowBatchID("n0", 101, 200, 1, 2, p),
		"end":       windowBatchID("n0", 100, 201, 1, 2, p),
		"account":   windowBatchID("n0", 100, 200, 2, 2, p),
		"project":   windowBatchID("n0", 100, 200, 1, 3, p),
		"partition": windowBatchID("n0", 100, 200, 1, 2, "dt=2026-05-03/hour=15"),
		"swapped":   windowBatchID("n0", 200, 100, 1, 2, p),
		"acct/proj": windowBatchID("n0", 100, 200, 2, 1, p),
	}
	seen := map[string]string{base: "base"}
	for name, id := range variants {
		if prev, dup := seen[id]; dup {
			t.Errorf("%s collides with %s: %q", name, prev, id)
		}
		seen[id] = name
	}
}

// loadWatermark restores a pending window only when it is version 2, has a
// nonce and is ahead of the committed watermark.
func TestBufferFlusher_LoadWatermarkPendingHandling(t *testing.T) {
	cases := []struct {
		name         string
		json         string
		wantLast     int64
		wantPending  int64
		wantUploaded int
	}{
		{"pending ahead is restored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":250,"pending_nonce":"abc","pending_uploaded":["k1","k2"],"version":2}`, 100, 250, 2},
		{"restored without uploaded keys", `{"last_flush_window_end_ns":100,"pending_window_end_ns":250,"pending_nonce":"abc","version":2}`, 100, 250, 0},
		{"pending without a nonce is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":250,"version":2}`, 100, 0, 0},
		{"version 1 pending is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":250,"pending_nonce":"abc","version":1}`, 100, 0, 0},
		{"pending equal to last is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":100,"pending_nonce":"abc","version":2}`, 100, 0, 0},
		{"pending behind last is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":40,"pending_nonce":"abc","version":2}`, 100, 0, 0},
		{"no pending", `{"last_flush_window_end_ns":100,"version":2}`, 100, 0, 0},
		{"corrupt file", `{nope`, 7, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
			if err := os.WriteFile(f.watermarkPath, []byte(tc.json), 0o600); err != nil {
				t.Fatal(err)
			}
			if got := f.loadWatermark(7); got != tc.wantLast {
				t.Fatalf("last = %d, want %d", got, tc.wantLast)
			}
			if f.pending != tc.wantPending || len(f.uploaded) != tc.wantUploaded || f.recovered != (tc.wantPending != 0) {
				t.Fatalf("pending=%d uploaded=%d recovered=%v, want %d/%d/%v", f.pending, len(f.uploaded), f.recovered, tc.wantPending, tc.wantUploaded, tc.wantPending != 0)
			}
			if tc.wantPending == 0 && f.nonce != "" {
				t.Fatalf("nonce %q kept without a pending window", f.nonce)
			}
		})
	}

	// savePending/saveWatermark round trip.
	f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
	f.pending, f.nonce, f.uploaded = 20, "nn", map[string]struct{}{"b": {}, "a": {}}
	if err := f.savePending(10); err != nil {
		t.Fatal(err)
	}
	g := NewBufferFlusher(nil, nil, filepath.Dir(f.watermarkPath), nil, 0, 0)
	if last := g.loadWatermark(1); last != 10 || g.pending != 20 || g.nonce != "nn" || len(g.uploaded) != 2 || !g.recovered {
		t.Fatalf("round trip: last=%d pending=%d nonce=%q uploaded=%v", last, g.pending, g.nonce, g.uploaded)
	}
	if err := g.saveWatermark(20); err != nil {
		t.Fatal(err)
	}
	h := NewBufferFlusher(nil, nil, filepath.Dir(f.watermarkPath), nil, 0, 0)
	if last := h.loadWatermark(1); last != 20 || h.pending != 0 || h.nonce != "" || h.recovered {
		t.Fatalf("after commit: last=%d pending=%d", last, h.pending)
	}
}

// 12. Tenants and partitions are flushed in a fixed order and a failure does not
// stop the rest of the window: with the first group failing, every other group
// is still attempted — and committed — in the same attempt; the retry sends
// only the one that failed.
func TestBufferFlusher_AFailedGroupDoesNotStopTheRestOfTheWindow(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantA, e.p1()) // the first group in flush order

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.mustLive(3*idemRowsPerGroup, 3, "the groups after the failed one")
	if len(e.storedKeys()) != 3 || e.totalAttempts() != 4 {
		t.Fatalf("%d groups stored, %d attempted in the failing tick, want 3 stored of 4 attempted", len(e.storedKeys()), e.totalAttempts())
	}
	e.failNone()
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if e.totalAttempts() != 5 {
		t.Fatalf("%d attempts in total, want 5 (the retry sends only the failed group)", e.totalAttempts())
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry")
}
