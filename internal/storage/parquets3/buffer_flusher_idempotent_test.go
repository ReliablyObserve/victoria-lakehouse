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
	e.newWriter()
	e.open()
	t.Cleanup(func() {
		if e.st != nil {
			e.st.Close()
		}
	})
	return e
}

// newWriter gives the env a new writer over a new, empty manifest and the same
// object store: what a restarted process has before it loads or lists anything.
func (e *idemEnv) newWriter() {
	e.bw, e.m = durabilityWriter(e.t, e.u)
	// Tenants get their own prefix so a key names its tenant.
	e.bw.SetTenantPrefix(func(account, project uint32) string { return fmt.Sprintf("t%d-%d/", account, project) })
	e.bw.SetStatsCallback(func(_, _ uint32, _, _, rows int64, _ string) {
		e.statMu.Lock()
		e.statRow += rows
		e.statMu.Unlock()
		e.statHook.Load().(func())()
	})
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
// attempt of its key. What is still enforced strictly is that the bytes stored
// under a key never change, and the tests assert per key that a stored group is
// never attempted again after a restart.
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
	last, err := f.loadWatermark(time.Now().UnixNano())
	if err != nil {
		e.t.Fatalf("load watermark: %v", err)
	}
	return f, last
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
	e.u.remove(fi.Key)
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

// storeDirect puts a failed group's encoded object into the bucket the way a PUT
// that completed just before a crash would have: no manifest commit, no mark.
func (e *idemEnv) storeDirect(up *logGroupUpload) {
	e.t.Helper()
	if up.result == nil {
		e.t.Fatalf("group %s was never encoded", up.key)
	}
	e.failNone()
	if err := e.u.Upload(context.Background(), up.key, up.result.Data); err != nil {
		e.t.Fatal(err)
	}
}

// crashWithEverythingUnstored runs a tick in which every PUT fails: the intent
// is durable and nothing is stored. It returns the flusher (whose failed groups
// carry the encoded objects) and the window's nonce.
func crashWithEverythingUnstored(t *testing.T, e *idemEnv) (*BufferFlusher, string) {
	t.Helper()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failAll()
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d with every upload failing", got)
	}
	if len(f.failed) != 4 {
		t.Fatalf("%d groups left, want 4", len(f.failed))
	}
	e.failNone()
	return f, readWatermarkFile(t, f).PendingNonce
}

func (e *idemEnv) storedMarkLines() []string {
	e.t.Helper()
	b, err := os.ReadFile(e.wmDir + "/buffer_flush_watermark.json.stored")
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func (e *idemEnv) wmPath(suffix string) string {
	return e.wmDir + "/buffer_flush_watermark.json" + suffix
}

// 1. One of four uploads fails. The pending record was written before any PUT
// and lists all four groups; the next tick retries exactly the group that
// failed, with the very bytes of the first attempt; the three stored objects are
// not sent again; rows are exact and the watermark commits.
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
	if wm.Version != 3 || wm.LastFlushWindowEndNs != last || wm.PendingWindowEndNs != end || wm.PendingNonce == "" {
		t.Fatalf("watermark file = %+v, want v3 with last=%d pending=%d and a nonce", wm, last, end)
	}
	if len(wm.PendingGroups) != 4 {
		t.Fatalf("pending_groups = %v, want all 4 groups recorded (before any PUT)", wm.PendingGroups)
	}
	want := []flushGroupRef{
		{1, 2, e.p1()}, {1, 2, e.p2()}, {2, 3, e.p1()}, {2, 3, e.p2()},
	}
	for i, g := range want {
		if wm.PendingGroups[i].flushGroupRef != g {
			t.Fatalf("pending_groups[%d] = %+v, want %+v (sorted by account, project, partition)", i, wm.PendingGroups[i], g)
		}
	}
	keys := e.allKeys(wm.PendingNonce)
	failedKey := keys[3]
	if marks := e.storedMarkLines(); len(marks) != 3 {
		t.Fatalf("stored marks = %v, want the 3 groups that were stored", marks)
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
		if n := e.u.headCount(k); n != 0 {
			t.Errorf("%s was HEADed %d times in-process, want 0", k, n)
		}
	}
	e.u.mu.Lock()
	h := e.u.attemptHashes[failedKey]
	e.u.mu.Unlock()
	if len(h) != 2 || h[0] != h[1] {
		t.Errorf("the retry of the failed group sent different bytes: %v", h)
	}
	wm = readWatermarkFile(t, f)
	if wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 || wm.PendingNonce != "" || len(wm.PendingGroups) != 0 {
		t.Fatalf("watermark file after success = %+v, want last=%d and nothing pending", wm, end)
	}
	if _, err := os.Stat(e.wmPath(".stored")); err == nil {
		t.Error("the stored marks survived the commit")
	}
	if f.pending != 0 || f.nonce != "" || f.refs != nil || f.failed != nil {
		t.Fatalf("in-memory pending state not cleared: %d %q %v %v", f.pending, f.nonce, f.refs, f.failed)
	}
	if got := e.statRows(); got != 4*idemRowsPerGroup {
		t.Fatalf("stats counted %d rows, want %d", got, 4*idemRowsPerGroup)
	}
}

// 2. A partial failure, then the process restarts. The stored marks settle the
// groups that were stored (no HEAD); the one that failed is looked up by HEAD,
// found absent, collected again and uploaded; rows are exact. No listing is
// needed for any of it.
func TestBufferFlusher_PartialFailureThenRestartSendsOnlyWhatWasNeverStored(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantA, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d despite a failed upload", got)
	}
	// A2 failing blocks tenant A's later partitions only; A has none after p2.
	e.mustLive(3*idemRowsPerGroup, 3, "before the restart")
	nonce := readWatermarkFile(t, f).PendingNonce

	e.failNone()
	e.restart()
	f2, restored := e.resume()
	if restored != last || !f2.recovered || f2.pending != end || f2.nonce != nonce || len(f2.refs) != 4 || len(f2.stored) != 3 {
		t.Fatalf("restored last=%d recovered=%v pending=%d nonce=%q refs=%d stored=%d", restored, f2.recovered, f2.pending, f2.nonce, len(f2.refs), len(f2.stored))
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the restart retry = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the restart retry")
	for i, k := range e.allKeys(nonce) {
		if n := e.storedCount(k); n != 1 {
			t.Errorf("%s stored %d times, want once", k, n)
		}
		want, heads := 1, 0
		if i == 1 { // A/p2, the group that failed
			want, heads = 2, 1
		}
		if n := e.attempts(k); n != want {
			t.Errorf("%s attempted %d times, want %d", k, n, want)
		}
		if n := e.u.headCount(k); n != heads {
			t.Errorf("%s HEADed %d times, want %d", k, n, heads)
		}
	}
	if wm := readWatermarkFile(t, f2); wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file = %+v", wm)
	}
}

// Crash matrix, point 1: the intent is durable and no PUT happened. The restart
// finds every group absent by HEAD, collects the window and uploads all of it.
func TestBufferFlusher_CrashIntentWrittenNoPutUploadsEverything(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	_, nonce := crashWithEverythingUnstored(t, e)
	_, end := e.window()
	if n := len(e.storedKeys()); n != 0 {
		t.Fatalf("%d objects stored, want none", n)
	}

	e.restart()
	f2, restored := e.resume()
	if !f2.recovered || len(f2.refs) != 4 || len(f2.stored) != 0 {
		t.Fatalf("recovered=%v refs=%d stored=%d", f2.recovered, len(f2.refs), len(f2.stored))
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after recovery")
	for _, k := range e.allKeys(nonce) {
		if e.storedCount(k) != 1 || e.u.headCount(k) != 1 {
			t.Errorf("%s stored %d times, HEADed %d times, want 1 and 1", k, e.storedCount(k), e.u.headCount(k))
		}
	}
}

// Crash matrix, point 2: the PUT completed and the process died before the
// stored mark and the manifest commit. The restart's HEAD finds the object and
// does not send it again; a listing then adopts it.
func TestBufferFlusher_CrashPutDoneNoMarkIsFoundByHead(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f, nonce := crashWithEverythingUnstored(t, e)
	_, end := e.window()
	x := f.failed[0] // A/p1, encoded
	e.storeDirect(x)

	e.restart()
	f2, restored := e.resume()
	if len(f2.stored) != 0 {
		t.Fatalf("%d stored marks, want none", len(f2.stored))
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.storedCount(x.key); n != 1 {
		t.Errorf("%s stored %d times, want the crashed process's PUT only", x.key, n)
	}
	if n := e.attempts(x.key); n != 2 { // the failed attempt and the direct PUT; nothing after the restart
		t.Errorf("%s attempted %d times, want 2 (none after the restart)", x.key, n)
	}
	if n := e.u.headCount(x.key); n != 1 {
		t.Errorf("%s HEADed %d times, want 1", x.key, n)
	}
	e.list() // the refresh adopts the object the crashed PUT stored
	e.mustLive(4*idemRowsPerGroup, 4, "after recovery and the listing")
	_ = nonce
}

// Crash matrix, point 3: the stored mark was written but the manifest commit was
// not. The restart trusts the mark: no HEAD, no PUT.
func TestBufferFlusher_CrashStoredMarkNoCommitSettlesWithoutHead(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f, _ := crashWithEverythingUnstored(t, e)
	_, end := e.window()
	x := f.failed[0]
	e.storeDirect(x)
	f.markStored(refOf(x))

	e.restart()
	f2, restored := e.resume()
	if len(f2.stored) != 1 {
		t.Fatalf("%d stored marks, want 1", len(f2.stored))
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.attempts(x.key); n != 2 {
		t.Errorf("%s attempted %d times, want 2 (none after the restart)", x.key, n)
	}
	if n := e.u.headCount(x.key); n != 0 {
		t.Errorf("%s HEADed %d times, want 0 (the mark settles it)", x.key, n)
	}
	e.list()
	e.mustLive(4*idemRowsPerGroup, 4, "after recovery and the listing")
}

// Crash matrix, point 4: every group is stored and committed but the watermark
// cannot be saved, and the process is restarted. The restart sends nothing.
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
	if wm.PendingWindowEndNs != end || len(wm.PendingGroups) != 4 {
		t.Fatalf("watermark file = %+v, want the window pending with its 4 groups", wm)
	}
	e.unblockWatermark()

	e.restart()
	f2, restored := e.resume()
	if restored != last || !f2.recovered || len(f2.stored) != 4 {
		t.Fatalf("restored last=%d recovered=%v stored=%d", restored, f2.recovered, len(f2.stored))
	}
	before := e.totalAttempts()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after the recovery tick = %d, want %d", got, end)
	}
	if after := e.totalAttempts(); after != before || before != 4 {
		t.Fatalf("uploads attempted: %d before the restart, %d after; want 4 and 4", before, after)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the recovery tick")
	if wm := readWatermarkFile(t, f2); wm.LastFlushWindowEndNs != end || wm.PendingWindowEndNs != 0 {
		t.Fatalf("watermark file = %+v", wm)
	}
}

// A HEAD error is "unknown": the recovered tick uploads nothing, keeps the
// pending window and asks again next tick.
func TestBufferFlusher_HeadErrorUploadsNothingAndIsRetried(t *testing.T) {
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
	// The marks are gone with the disk that held them (or were never written):
	// everything has to be checked by HEAD.
	if err := os.Remove(e.wmPath(".stored")); err != nil {
		t.Fatal(err)
	}
	f2, restored := e.resume()
	fileBefore, _ := os.ReadFile(f2.watermarkPath)
	e.u.mu.Lock()
	e.u.headErr = func(string) error { return errors.New("HeadObject: 503 SlowDown") }
	e.u.mu.Unlock()
	attempts0 := e.totalAttempts()
	errs0 := metrics.BufferFlushErrors.Get("head")

	for i := 0; i < 3; i++ {
		if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != last {
			t.Fatalf("tick %d with a failing HEAD moved the watermark to %d", i, got)
		}
	}
	if e.totalAttempts() != attempts0 {
		t.Fatalf("uploads were attempted while HEAD failed: %d -> %d", attempts0, e.totalAttempts())
	}
	if !f2.recovered || f2.pending != end {
		t.Fatalf("the pending window was dropped: recovered=%v pending=%d", f2.recovered, f2.pending)
	}
	if fileAfter, _ := os.ReadFile(f2.watermarkPath); !bytes.Equal(fileBefore, fileAfter) {
		t.Fatal("the watermark file changed while HEAD failed")
	}
	if d := metrics.BufferFlushErrors.Get("head") - errs0; d != 3 {
		t.Errorf("head errors counted = %d, want 3", d)
	}

	e.u.mu.Lock()
	e.u.headErr = nil
	e.u.mu.Unlock()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark after HEAD recovered = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after HEAD recovered")
	if keys := e.storedKeys(); len(keys) != 4 {
		t.Fatalf("%d objects stored, want 4 (the 3 from before + the failed one)", len(keys))
	}
	for _, k := range e.storedKeys() {
		if e.storedCount(k) != 1 {
			t.Errorf("%s stored %d times", k, e.storedCount(k))
		}
	}
}

// The manifest snapshot restored at startup lists many more files than the
// bucket holds now, so the cliff guard rejects every listing and the manifest
// never counts as refreshed. Recovery does not depend on that: it settles by
// marks and HEAD and completes.
func TestBufferFlusher_RecoveryDoesNotNeedAnAcceptedListing(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	for i := 0; i < 20; i++ { // snapshot-era entries whose objects are gone
		e.m.AddFile(e.p1(), manifest.FileInfo{Key: fmt.Sprintf("%s/old-%d.parquet", e.p1(), i), Size: 1, RowCount: 1})
	}
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced")
	}
	e.failNone()
	e.restart()
	f2, restored := e.resume()

	var objs []manifest.ListedObject
	e.u.mu.Lock()
	for k, d := range e.u.data {
		objs = append(objs, manifest.ListedObject{Key: k, Size: int64(len(d))})
	}
	e.u.mu.Unlock()
	time.Sleep(3 * time.Millisecond)
	if e.m.ApplyListing(objs, time.Now()) || e.m.Listed() {
		t.Fatal("precondition: the cliff guard must reject the listing")
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("the recovered flusher stalled behind the rejected listing: watermark = %d, want %d", got, end)
	}
	if keys := e.storedKeys(); len(keys) != 4 {
		t.Fatalf("%d objects stored, want 4", len(keys))
	}
	for _, k := range e.storedKeys() {
		if e.storedCount(k) != 1 {
			t.Errorf("%s stored %d times", k, e.storedCount(k))
		}
	}
}

// The first listing after a restart lacks a whole account (a per-account LIST
// failed and the refresh was still accepted). The object stored before the
// crash is therefore not in the manifest; recovery finds it by HEAD and does not
// send it again — not even with late rows, which would have changed its bytes.
func TestBufferFlusher_ListingMissingAnAccountDoesNotResendAStoredKey(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f, _ := crashWithEverythingUnstored(t, e)
	_, end := e.window()
	x := f.failed[0] // A/p1
	e.storeDirect(x)
	e.late(idemTenantA, 1, 3) // late rows for X's group

	e.restart()
	f2, restored := e.resume()
	prefixA := e.tenantPrefix(idemTenantA)
	var objs []manifest.ListedObject
	e.u.mu.Lock()
	for k, d := range e.u.data {
		if !strings.HasPrefix(k, prefixA) {
			objs = append(objs, manifest.ListedObject{Key: k, Size: int64(len(d))})
		}
	}
	e.u.mu.Unlock()
	time.Sleep(3 * time.Millisecond)
	e.m.ApplyListing(objs, time.Now())
	if e.m.HasKey(x.key) {
		t.Fatal("precondition: the listing must not show X")
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.storedCount(x.key); n != 1 {
		t.Errorf("%s stored %d times, want the crashed process's PUT only", x.key, n)
	}
	if n := e.attempts(x.key); n != 2 {
		t.Errorf("%s attempted %d times, want 2 (none after the restart)", x.key, n)
	}
}

// The stored object was adopted, then compacted (retired). The restart settles
// it as retired, without a HEAD, and counts its rows as superseded.
func TestBufferFlusher_RecoveryOfARetiredGroupIsSettledAndCounted(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f, _ := crashWithEverythingUnstored(t, e)
	_, end := e.window()
	x := f.failed[0] // A/p1
	e.storeDirect(x)
	e.list() // adopts X (bare)
	e.compact(e.entry(x.key), e.p1())

	e.restart()
	f2, restored := e.resume()
	superseded0 := metrics.InsertRowsSuperseded.Get()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.attempts(x.key); n != 2 {
		t.Errorf("%s attempted %d times, want 2 (none after the restart)", x.key, n)
	}
	if n := e.u.headCount(x.key); n != 0 {
		t.Errorf("%s HEADed %d times, want 0 (the manifest knows it is retired)", x.key, n)
	}
	if e.m.HasKey(x.key) {
		t.Error("the compacted key is live again")
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after recovery: the compaction output carries X's rows")
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != idemRowsPerGroup {
		t.Errorf("superseded counter rose by %d, want %d", d, idemRowsPerGroup)
	}
}

// 6a. K is stored and marked, then compacted into C, and a real listing forgets
// K's retired record (the object is gone). After a restart the retry must not
// send K again: its stored mark settles it. HEAD alone would say "absent".
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
// publishes. K's object and manifest entry are what compaction read, so the
// publish succeeds and no row is lost.
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
	restored, err := f2.loadWatermark(time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
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
// further ticks send nothing and only retry the save; once it works, the
// watermark commits.
func TestBufferFlusher_WatermarkSaveFailureAfterUploadsNeverReuploads(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.blockWatermarkAfter(4)
	errs0 := metrics.BufferFlushErrors.Get("watermark")

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
	if d := metrics.BufferFlushErrors.Get("watermark") - errs0; d != 4 {
		t.Errorf("watermark errors counted = %d, want 4", d)
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

// If the pending record cannot be written, nothing is uploaded: the attempt is
// abandoned before any PUT and started afresh once the record can be written.
func TestBufferFlusher_IntentWriteFailureAbortsBeforeAnyPut(t *testing.T) {
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
	if err := os.Chmod(e.wmDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(e.wmDir, 0o700) })
	errs0 := metrics.BufferFlushErrors.Get("intent")

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark = %d, want %d", got, last)
	}
	if e.totalAttempts() != 0 {
		t.Fatalf("%d uploads attempted although the intent could not be recorded", e.totalAttempts())
	}
	if f.pending != 0 || f.refs != nil {
		t.Fatalf("a window is pending in memory without a durable intent: %d", f.pending)
	}
	if d := metrics.BufferFlushErrors.Get("intent") - errs0; d != 1 {
		t.Errorf("intent errors counted = %d, want 1", d)
	}

	if err := os.Chmod(e.wmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := f.tick(context.Background(), last, end); got != end {
		t.Fatalf("watermark after the directory recovered = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry")
}

// Groups the re-collected rows introduce that the pending record lacks were
// never uploaded. They are recorded durably before their first PUT.
func TestBufferFlusher_NewGroupsFoundOnRecoveryAreRecordedBeforeUpload(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	_, nonce := crashWithEverythingUnstored(t, e)
	_, end := e.window()
	tenantC := logstorage.TenantID{AccountID: 3, ProjectID: 4}
	ingestLogAt(t, e.st, tenantC, e.hour.Add(10*time.Minute).UnixNano(), e.hour.Add(20*time.Minute).UnixNano(), idemRowsPerGroup)
	e.st.DebugFlush()

	e.restart()
	f2, restored := e.resume()
	if len(f2.refs) != 4 {
		t.Fatalf("record has %d groups, want the original 4", len(f2.refs))
	}
	cRef := flushGroupRef{Account: 3, Project: 4, Partition: e.p1()}
	recordedFirst := false
	seenC := false
	e.hook.Store(func(key string) {
		if !strings.HasPrefix(key, e.tenantPrefix(tenantC)) {
			return
		}
		seenC = true
		b, err := os.ReadFile(f2.watermarkPath)
		if err != nil {
			return
		}
		wm, err := parseWatermark(b)
		if err == nil {
			for _, g := range wm.PendingGroups {
				if g.flushGroupRef == cRef {
					recordedFirst = true
				}
			}
		}
	})
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if !seenC {
		t.Fatal("the new group was never uploaded")
	}
	if !recordedFirst {
		t.Fatal("the new group's PUT ran before it was in the durable record")
	}
	e.mustLive(5*idemRowsPerGroup, 5, "after recovery")
	_ = nonce
}

// Finding: the buffer's read watermark is a tenant's newest stored MaxTimeNs, so
// storing a newer partition of a tenant whose older partition failed would hide
// that older partition's buffer rows. After a tenant's group fails, its later
// partitions are not attempted in that attempt; other tenants proceed.
func TestBufferFlusher_TenantsLaterPartitionsAreNotAttemptedAfterItsFailure(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantA, e.p1()) // tenant A's oldest partition

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	nonce := readWatermarkFile(t, f).PendingNonce
	keys := e.allKeys(nonce)
	if n := e.attempts(keys[1]); n != 0 {
		t.Errorf("A/p2 was attempted %d times after A/p1 failed, want 0", n)
	}
	for _, k := range keys[2:] {
		if e.storedCount(k) != 1 {
			t.Errorf("tenant B's %s stored %d times, want 1: other tenants proceed", k, e.storedCount(k))
		}
	}
	e.mustLive(2*idemRowsPerGroup, 2, "only tenant B is visible in the manifest")
	if len(f.failed) != 2 || f.failed[1].result != nil {
		t.Fatalf("failed = %d groups, the unattempted one must stay unencoded", len(f.failed))
	}

	e.failNone()
	if got := f.tick(context.Background(), last, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	for _, k := range keys {
		if e.storedCount(k) != 1 {
			t.Errorf("%s stored %d times, want once", k, e.storedCount(k))
		}
	}
	if n := e.attempts(keys[1]); n != 1 {
		t.Errorf("A/p2 attempted %d times, want exactly once (the retry)", n)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the retry")
}

// A failed group of one tenant does not stop the rest of the window: with A's
// newest partition failing, its older one and all of tenant B's still go.
func TestBufferFlusher_AFailedGroupDoesNotStopTheRestOfTheWindow(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.failGroup(idemTenantA, e.p2())

	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.mustLive(3*idemRowsPerGroup, 3, "the groups other than the failed one")
	if len(e.storedKeys()) != 3 || e.totalAttempts() != 4 {
		t.Fatalf("%d groups stored, %d attempted, want 3 stored of 4 attempted", len(e.storedKeys()), e.totalAttempts())
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

// The key is retired while the PUT is in flight (a peer's push, compaction). The
// commit refuses it: not live, the recreated object is owed a delete again with
// the earlier replacement kept, its rows are counted as superseded, and the
// group counts as done so the window commits.
func TestBufferFlusher_KeyRetiredDuringThePutIsNotResurrected(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	prefix := e.tenantPrefix(idemTenantA)
	var target string
	var once sync.Once
	e.hook.Store(func(key string) {
		if strings.HasPrefix(key, prefix) && strings.Contains(key, e.p1()) {
			once.Do(func() {
				target = key
				e.m.Retire(key, "compacted-elsewhere", false)
				e.m.ConfirmDeleted(key) // its delete landed: nothing owed yet
			})
		}
	})
	superseded0 := metrics.InsertRowsSuperseded.Get()

	// The retirement lands between the writer's check and its commit: force the
	// writer past its pre-check by retiring inside the PUT.
	if got := f.tick(context.Background(), last, end); got != end {
		t.Fatalf("watermark = %d, want %d: the refused group counts as done", got, end)
	}
	if target == "" {
		t.Fatal("the hook never ran")
	}
	if e.m.HasKey(target) {
		t.Error("the commit resurrected a key retired during the PUT")
	}
	rk, ok := e.m.LookupRetired(target)
	if !ok || !rk.Reclaim || rk.Deleted || rk.By != "compacted-elsewhere" {
		t.Errorf("LookupRetired = %+v, %v: want Reclaim owed, not Deleted, By kept", rk, ok)
	}
	e.mustLive(3*idemRowsPerGroup, 3, "the other three groups")
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != idemRowsPerGroup {
		t.Errorf("superseded counter rose by %d, want %d", d, idemRowsPerGroup)
	}
}

// 8a. Late rows, in-process: rows that arrive for the pending window after its
// first attempt are not added by the retry.
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
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup+late, 4, "after the restart")
	if got := e.parquetRows(e.keyOf(nonce, idemTenantB, e.p2())); got != idemRowsPerGroup+late {
		t.Errorf("the group never stored has %d rows, want %d (its own plus the late ones)", got, idemRowsPerGroup+late)
	}
	if got := e.parquetRows(e.keyOf(nonce, idemTenantA, e.p1())); got != idemRowsPerGroup {
		t.Errorf("the stored group has %d rows, want it unchanged at %d", got, idemRowsPerGroup)
	}
}

// 9. Two nodes flushing the same window derive different keys: the nonce is
// drawn per pending window.
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

// onStored runs after the PUT, is told about groups that needed no upload, and
// cannot fail the group.
func TestUploadGroup_OnStoredRunsAfterThePutAndCannotFailTheGroup(t *testing.T) {
	u := &faultyUploader{}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	groups := bw.buildLogGroups(partitionFromNano(base.UnixNano()), rowsAt(base, 20, 0), nil)
	if len(groups) != 1 {
		t.Fatalf("%d groups, want 1", len(groups))
	}
	up := groups[0]
	var told []string
	var storedAtCall []int
	up.onStored = func(key string) {
		told = append(told, key)
		storedAtCall = append(storedAtCall, u.uploadedCount(key))
	}
	if err := bw.uploadLogGroup(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	if len(told) != 1 || told[0] != up.key || storedAtCall[0] != 1 {
		t.Fatalf("onStored calls = %v (stored count at the call %v), want once, after the PUT", told, storedAtCall)
	}
	if rows, files := committedRows(m); rows != 20 || files != 1 {
		t.Fatalf("committed %d rows in %d files, want 20 in 1", rows, files)
	}
}

func (u *faultyUploader) uploadedCount(key string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.uploaded[key]
}

// A group whose key is already live, or retired, needs no upload; onStored still
// runs so the caller records it as done. Only a retired one is counted as
// superseded.
func TestUploadGroup_SettledGroupsRunOnStoredWithoutUploading(t *testing.T) {
	u := &faultyUploader{}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	partition := partitionFromNano(base.UnixNano())
	mk := func(batch string) (*logGroupUpload, *[]string) {
		groups := bw.buildLogGroups(partition, rowsAt(base, 7, 0), func(uint32, uint32) string { return batch })
		var got []string
		groups[0].onStored = func(k string) { got = append(got, k) }
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
		t.Errorf("onStored calls: live=%d retired=%d, want 1 each", len(*liveCalls), len(*retiredCalls))
	}
	if n := (func() int {
		u.mu.Lock()
		defer u.mu.Unlock()
		n := 0
		for _, h := range u.attemptHashes {
			n += len(h)
		}
		return n
	})(); n != 0 {
		t.Errorf("%d uploads for two settled groups, want 0", n)
	}
	if m.HasKey(retired.key) {
		t.Error("the retired key was added")
	}
}

// Writer level (the legacy staging path shares it): a key retired while its PUT
// is in flight is not added back.
func TestUploadGroup_KeyRetiredDuringThePutIsNotAddedBack(t *testing.T) {
	var bw *BatchWriter
	var m *manifest.Manifest
	var key string
	u := &faultyUploader{fail: func(k string) error {
		key = k
		m.Retire(k, "compacted", false)
		return nil
	}}
	bw, m = durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	partition := partitionFromNano(base.UnixNano())
	up := bw.buildLogGroups(partition, rowsAt(base, 9, 0), nil)[0]
	superseded0 := metrics.InsertRowsSuperseded.Get()

	if err := bw.uploadLogGroup(context.Background(), up); err != nil {
		t.Fatalf("a refused commit counts as done, got %v", err)
	}
	if m.HasKey(key) {
		t.Error("the key retired during the PUT is live")
	}
	rk, ok := m.LookupRetired(key)
	if !ok || !rk.Reclaim || rk.By != "compacted" {
		t.Errorf("LookupRetired = %+v, %v: want Reclaim owed and By kept", rk, ok)
	}
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != 9 {
		t.Errorf("superseded counter rose by %d, want 9", d)
	}
}

// A pending window is flushed although the size gate would hold it. A fresh
// window is still gated.
func TestBufferFlusher_PendingWindowIgnoresTheSizeGate(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	last, end := e.window()

	f1 := e.flusher(1, time.Nanosecond)
	e.failAll()
	if got := f1.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.failNone()

	// After a restart the gate is huge: nothing would flush a fresh window.
	e.restart()
	f2 := e.flusher(1<<40, 24*time.Hour)
	last2, err := f2.loadWatermark(time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if f2.pending != end {
		t.Fatalf("pending = %d, want %d", f2.pending, end)
	}
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

// loadWatermark restores a pending window only when it is version 3, has a
// nonce and is ahead of the committed watermark; an unreadable file is an error
// unless the previous copy is readable.
func TestBufferFlusher_LoadWatermarkHandling(t *testing.T) {
	const pend = `"pending_window_end_ns":250,"pending_nonce":"abc","pending_groups":[{"account":1,"project":2,"partition":"dt=x/hour=1"},{"account":1,"project":2,"partition":"dt=x/hour=2"}]`
	cases := []struct {
		name        string
		json        string
		wantLast    int64
		wantPending int64
		wantRefs    int
	}{
		{"pending ahead is restored", `{"last_flush_window_end_ns":100,` + pend + `,"version":3}`, 100, 250, 2},
		{"pending without a nonce is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":250,"version":3}`, 100, 0, 0},
		{"version 2 pending is ignored", `{"last_flush_window_end_ns":100,` + pend + `,"version":2}`, 100, 0, 0},
		{"pending equal to last is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":100,"pending_nonce":"abc","version":3}`, 100, 0, 0},
		{"pending behind last is ignored", `{"last_flush_window_end_ns":100,"pending_window_end_ns":40,"pending_nonce":"abc","version":3}`, 100, 0, 0},
		{"no pending", `{"last_flush_window_end_ns":100,"version":3}`, 100, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
			if err := os.WriteFile(f.watermarkPath, []byte(tc.json), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := f.loadWatermark(7)
			if err != nil || got != tc.wantLast {
				t.Fatalf("last = %d, err = %v, want %d", got, err, tc.wantLast)
			}
			if f.pending != tc.wantPending || len(f.refs) != tc.wantRefs || f.recovered != (tc.wantPending != 0) {
				t.Fatalf("pending=%d refs=%d recovered=%v, want %d/%d/%v", f.pending, len(f.refs), f.recovered, tc.wantPending, tc.wantRefs, tc.wantPending != 0)
			}
			if tc.wantPending == 0 && f.nonce != "" {
				t.Fatalf("nonce %q kept without a pending window", f.nonce)
			}
		})
	}

	t.Run("no file is a first start", func(t *testing.T) {
		f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
		if got, err := f.loadWatermark(7); err != nil || got != 7 {
			t.Fatalf("got %d, %v, want the fallback", got, err)
		}
	})
	for name, content := range map[string]string{"corrupt": `{nope`, "empty": ``, "zero last": `{"last_flush_window_end_ns":0,"version":3}`} {
		content := content
		t.Run(name+" file with no backup is an error", func(t *testing.T) {
			f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
			if err := os.WriteFile(f.watermarkPath, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := f.loadWatermark(7)
			if err == nil || !strings.Contains(err.Error(), "delete both files") {
				t.Fatalf("got %d, %v: want an error that says how to reset", got, err)
			}
		})
	}

	// The stored marks: only those of this window's nonce and groups.
	f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
	if err := os.WriteFile(f.watermarkPath, []byte(`{"last_flush_window_end_ns":100,`+pend+`,"version":3}`), 0o600); err != nil {
		t.Fatal(err)
	}
	marks := "abc\t1\t2\tdt=x/hour=1\n" + // ours
		"zzz\t1\t2\tdt=x/hour=2\n" + // another window's
		"abc\t9\t9\tdt=x/hour=9\n" + // not in the record
		"abc\t1\t2\tdt=x/ho" // torn last line
	if err := os.WriteFile(f.storedPath(), []byte(marks), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.loadWatermark(7); err != nil {
		t.Fatal(err)
	}
	if len(f.stored) != 1 {
		t.Fatalf("stored marks = %v, want only the one of this window and record", f.stored)
	}
	if _, ok := f.stored[flushGroupRef{1, 2, "dt=x/hour=1"}]; !ok {
		t.Fatalf("stored marks = %v", f.stored)
	}
}

// A corrupt or empty main file falls back to <name>.prev, which holds the SAME
// content — the pending record included — so recovery resumes the window instead
// of flushing it afresh under a new nonce. Both unreadable is an error and the
// flusher does not start.
func TestBufferFlusher_CorruptMainIntentFallsBackToAnIdenticalPrevAndDoesNotDuplicate(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	// A committed watermark exists before the window (the steady state).
	if err := f.saveWatermark(last); err != nil {
		t.Fatal(err)
	}
	e.failGroup(idemTenantB, e.p2())
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.mustLive(3*idemRowsPerGroup, 3, "after the partial attempt")
	nonce := readWatermarkFile(t, f).PendingNonce

	mainB, err := os.ReadFile(f.watermarkPath)
	if err != nil {
		t.Fatal(err)
	}
	prevB, err := os.ReadFile(f.prevPath())
	if err != nil || !bytes.Equal(mainB, prevB) {
		t.Fatalf("the backup differs from the main file (%v):\nmain %s\nprev %s", err, mainB, prevB)
	}
	if _, err := os.Stat(f.watermarkPath + ".tmp"); err == nil {
		t.Error("a temp file was left behind: the replacement was not an atomic rename")
	}

	for name, content := range map[string]string{"garbage": "{garbage", "empty": "", "truncated": string(mainB[:len(mainB)/2])} {
		if err := os.WriteFile(f.watermarkPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		f2 := e.flusher(1, time.Nanosecond)
		got, err := f2.loadWatermark(time.Now().UnixNano())
		if err != nil || got != last || !f2.recovered || f2.nonce != nonce || len(f2.refs) != 4 {
			t.Fatalf("%s main file: last=%d recovered=%v nonce=%q refs=%d err=%v; want the pending record from the backup", name, got, f2.recovered, f2.nonce, len(f2.refs), err)
		}
	}

	// The repro: corrupt main, restart, resume: no duplicates.
	if err := os.WriteFile(f.watermarkPath, []byte("{garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.failNone()
	e.restart()
	f3, restored := e.resume()
	if got := f3.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after the corrupt-main recovery")
	for _, k := range e.allKeys(nonce)[:3] {
		if n := e.attempts(k); n != 1 {
			t.Errorf("%s was sent %d times, want only its first", k, n)
		}
	}

	// Both unreadable: an error, and Run does not start.
	if err := os.WriteFile(f.watermarkPath, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.prevPath(), []byte("{bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	f4 := e.flusher(1, time.Nanosecond)
	if err := f4.Prepare(time.Now().UnixNano()); err == nil {
		t.Fatal("Prepare accepted a watermark it cannot read")
	}
	attempts0 := e.totalAttempts()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f4.Run(context.Background(), time.Millisecond, time.Now().UnixNano())
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept running on an unreadable watermark")
	}
	if e.totalAttempts() != attempts0 {
		t.Fatal("a flusher that could not read its watermark uploaded")
	}
}

// Every write, whatever it is (commit, intent, recovery additions), puts the same
// content into .prev before main, through a temp file that does not survive, and
// fsyncs the file and the directory.
func TestBufferFlusher_EveryWatermarkWriteMirrorsToPrevFirstAndIsFsynced(t *testing.T) {
	f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)
	var syncs []string
	orig := fsyncFile
	fsyncFile = func(fh *os.File) error {
		syncs = append(syncs, filepath.Base(fh.Name()))
		return orig(fh)
	}
	t.Cleanup(func() { fsyncFile = orig })

	for _, end := range []int64{100, 200} {
		syncs = nil
		if err := f.saveWatermark(end); err != nil {
			t.Fatal(err)
		}
		mainB, _ := os.ReadFile(f.watermarkPath)
		prevB, _ := os.ReadFile(f.prevPath())
		if !bytes.Equal(mainB, prevB) {
			t.Fatalf("after writing %d: main %s, prev %s", end, mainB, prevB)
		}
		if wm, _ := parseWatermark(mainB); wm.LastFlushWindowEndNs != end {
			t.Fatalf("main = %s", mainB)
		}
		// .prev is synced before main; each file and then the directory.
		want := []string{"buffer_flush_watermark.json.prev.tmp", filepath.Base(filepath.Dir(f.watermarkPath)), "buffer_flush_watermark.json.tmp", filepath.Base(filepath.Dir(f.watermarkPath))}
		if len(syncs) != 4 || syncs[0] != want[0] || syncs[1] != want[1] || syncs[2] != want[2] || syncs[3] != want[3] {
			t.Fatalf("fsync sequence = %v, want %v", syncs, want)
		}
	}
	entries, _ := os.ReadDir(filepath.Dir(f.watermarkPath))
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), ".tmp") {
			t.Errorf("temp file %s left behind", ent.Name())
		}
	}
}

// A stored mark is fsynced after the append, and the directory is fsynced when
// the file is created — before the manifest commit, so anything compaction can
// see already has a durable mark.
func TestBufferFlusher_StoredMarkIsFsyncedBeforeTheCommit(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	var mu sync.Mutex
	var syncs []string
	orig := fsyncFile
	fsyncFile = func(fh *os.File) error {
		mu.Lock()
		syncs = append(syncs, filepath.Base(fh.Name()))
		mu.Unlock()
		return orig(fh)
	}
	t.Cleanup(func() { fsyncFile = orig })
	// At each group's commit, its mark must already have been fsynced.
	var atCommit []int
	dirAfterMark := 0
	e.statHook.Store(func() {
		mu.Lock()
		n := 0
		for _, s := range syncs {
			if s == "buffer_flush_watermark.json.stored" {
				n++
			}
		}
		atCommit = append(atCommit, n)
		if len(atCommit) == 1 {
			// The mark file was created by the first group: the directory must
			// have been fsynced after its first fsync, before this commit.
			seenMark := false
			for _, s := range syncs {
				if s == "buffer_flush_watermark.json.stored" {
					seenMark = true
				} else if seenMark && s == filepath.Base(e.wmDir) {
					dirAfterMark++
				}
			}
		}
		mu.Unlock()
	})
	if got := f.tick(context.Background(), last, end); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	for i, n := range atCommit {
		if n < i+1 {
			t.Errorf("group %d committed with only %d mark fsyncs done", i, n)
		}
	}
	if dirAfterMark == 0 {
		t.Error("the directory was not fsynced between the first mark and its group's commit")
	}
}

// R3: K is stored and committed, compacted into C and deleted, and a listing
// forgets K's retired record. Its mark is durable, so the restart does not send
// K again. (Before marks were fsynced, a power loss here lost the mark and K was
// uploaded next to C.)
func TestBufferFlusher_DurableMarkAfterCompactionAndForgottenRecordDoesNotResend(t *testing.T) {
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
		t.Fatal("precondition: K must be neither retired nor live after the listing")
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if n := e.attempts(k); n != 1 {
		t.Errorf("DUPLICATE: %s was sent %d times after its object was compacted away", k, n)
	}
	if rows, files := e.liveRows(); rows != 4*idemRowsPerGroup {
		t.Errorf("DUPLICATES: live %d rows in %d files, want %d", rows, files, 4*idemRowsPerGroup)
	}
}

// R2: recorded groups whose objects HEAD reports absent and whose rows the buffer
// no longer has (retention expired, or the gate filter changed): the window
// commits, and the loss is counted — rows, once per group, and logged.
func TestBufferFlusher_AbsentGroupWithoutRowsIsCounted(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	_, _ = crashWithEverythingUnstored(t, e)
	_, end := e.window()
	e.restart()
	keep := func(account, _ uint32, _ string) bool { return account != idemTenantB.AccountID }
	f2 := NewBufferFlusher(e.bw, e.st, e.wmDir, keep, 1, time.Nanosecond)
	f2.latencyOffset = 0
	restored, err := f2.loadWatermark(time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	lost0 := metrics.InsertRowsLost.Get("buffer_expired")
	missing0 := metrics.BufferFlushErrors.Get("missing")

	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("recovery must never block on lost rows: watermark = %d, want %d", got, end)
	}
	e.mustLive(2*idemRowsPerGroup, 2, "tenant A only")
	if d := metrics.InsertRowsLost.Get("buffer_expired") - lost0; d != 2*idemRowsPerGroup {
		t.Errorf("rows lost counted = %d, want %d", d, 2*idemRowsPerGroup)
	}
	if d := metrics.BufferFlushErrors.Get("missing") - missing0; d != 2 {
		t.Errorf("missing groups counted = %d, want 2 (once per group)", d)
	}
}

// A shortfall, not a total loss: the buffer has some of a group's rows. They are
// uploaded, the rest is counted.
func TestBufferFlusher_PartialShortfallUploadsWhatIsLeftAndCountsTheRest(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	_, _ = crashWithEverythingUnstored(t, e)
	_, end := e.window()
	e.restart()
	var n atomic.Int64
	keep := func(account, _ uint32, _ string) bool {
		if account != idemTenantB.AccountID {
			return true
		}
		return n.Add(1)%2 == 0 // every other row of tenant B
	}
	f2 := NewBufferFlusher(e.bw, e.st, e.wmDir, keep, 1, time.Nanosecond)
	f2.latencyOffset = 0
	restored, err := f2.loadWatermark(time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	lost0 := metrics.InsertRowsLost.Get("buffer_expired")
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	rows, files := e.liveRows()
	lost := int64(metrics.InsertRowsLost.Get("buffer_expired") - lost0)
	if files != 4 || rows+lost != 4*idemRowsPerGroup || lost == 0 || rows <= 2*idemRowsPerGroup {
		t.Fatalf("live %d rows in %d files, lost %d: want 4 files and live+lost = %d with a real shortfall", rows, files, lost, 4*idemRowsPerGroup)
	}
}

// A window whose groups are all settled commits even when the buffer cannot be
// queried, and never reads it.
type failingBuffer struct {
	flusherBuffer
	flushes, queries atomic.Int32
}

func (b *failingBuffer) DebugFlush() { b.flushes.Add(1) }
func (b *failingBuffer) RunQuery(*logstorage.QueryContext, logstorage.WriteDataBlockFunc) error {
	b.queries.Add(1)
	return errors.New("buffer query failed")
}
func (b *failingBuffer) GetTenantIDs(context.Context, int64, int64) ([]logstorage.TenantID, error) {
	b.queries.Add(1)
	return nil, errors.New("buffer query failed")
}

func TestBufferFlusher_RecoveryWithOnlySettledGroupsNeverReadsTheBuffer(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.blockWatermarkAfter(4) // all four stored, marked and committed; the watermark is stuck
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.unblockWatermark()
	e.restart()

	fb := &failingBuffer{flusherBuffer: e.st}
	f2 := NewBufferFlusher(e.bw, fb, e.wmDir, nil, 1, time.Nanosecond)
	f2.latencyOffset = 0
	restored, err := f2.loadWatermark(time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("recovery stalled on a collect error it did not need: watermark = %d, want %d", got, end)
	}
	if fb.flushes.Load() != 0 || fb.queries.Load() != 0 {
		t.Fatalf("the buffer was read (%d flushes, %d queries) although every group was settled", fb.flushes.Load(), fb.queries.Load())
	}
	e.mustLive(4*idemRowsPerGroup, 4, "after recovery")
}

// Groups settled as retired are counted from the record (their recorded rows),
// without reading the buffer.
func TestBufferFlusher_RetiredGroupsAreCountedFromTheRecord(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	e.blockWatermarkAfter(4)
	if got := f.tick(context.Background(), last, end); got != last {
		t.Fatalf("watermark advanced to %d", got)
	}
	e.unblockWatermark()
	nonce := readWatermarkFile(t, f).PendingNonce
	x := e.keyOf(nonce, idemTenantA, e.p1())
	e.compact(e.entry(x), e.p1())
	e.restart()
	if err := os.Remove(e.wmPath(".stored")); err != nil { // force manifest classification
		t.Fatal(err)
	}
	fb := &failingBuffer{flusherBuffer: e.st}
	f2 := NewBufferFlusher(e.bw, fb, e.wmDir, nil, 1, time.Nanosecond)
	f2.latencyOffset = 0
	restored, err := f2.loadWatermark(time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	sup0 := metrics.InsertRowsSuperseded.Get()
	if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
		t.Fatalf("watermark = %d, want %d", got, end)
	}
	if d := metrics.InsertRowsSuperseded.Get() - sup0; d != idemRowsPerGroup {
		t.Errorf("superseded counted %d, want the recorded %d", d, idemRowsPerGroup)
	}
	if fb.flushes.Load() != 0 || fb.queries.Load() != 0 {
		t.Fatal("the buffer was read for a window with nothing to upload")
	}
	if n := e.attempts(x); n != 1 {
		t.Errorf("%s attempted %d times", x, n)
	}
}

// bucketRows counts the rows in every object the bucket holds (not deleted).
func (e *idemEnv) bucketRows() int64 {
	var n int64
	for _, k := range e.storedKeys() {
		if !e.deleted[k] {
			n += e.parquetRows(k)
		}
	}
	return n
}

// Restarts with a manifest that is not the one the attempt ran with: EMPTY (a
// node that lost its disk, never listed, or listed by a real refresh) and OLDER
// (a snapshot from before the attempt, with unrelated entries). Marks or HEAD
// settle the stored groups; nothing stored is sent again and rows are exact.
func TestBufferFlusher_RestartWithAFreshOrOlderManifestDoesNotResend(t *testing.T) {
	type variant struct {
		name     string
		populate func(e *idemEnv)
		list     bool
		noMarks  bool
	}
	variants := []variant{
		{name: "empty never listed", populate: func(*idemEnv) {}},
		{name: "empty then listed", populate: func(*idemEnv) {}, list: true},
		{name: "empty never listed no marks", populate: func(*idemEnv) {}, noMarks: true},
		{name: "empty then listed no marks", populate: func(*idemEnv) {}, list: true, noMarks: true},
		{name: "older snapshot", populate: func(e *idemEnv) {
			e.m.AddFile(e.p1(), manifest.FileInfo{Key: "t9-9/" + e.p1() + "/old.parquet", Size: 1, RowCount: 5})
		}},
		{name: "older snapshot no marks", populate: func(e *idemEnv) {
			e.m.AddFile(e.p1(), manifest.FileInfo{Key: "t9-9/" + e.p1() + "/old.parquet", Size: 1, RowCount: 5})
		}, noMarks: true},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
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
			e.newWriter()
			v.populate(e)
			if v.noMarks {
				if err := os.Remove(e.wmPath(".stored")); err != nil {
					t.Fatal(err)
				}
			}
			f2, restored := e.resume()
			if got := f2.tick(context.Background(), restored, end+int64(time.Hour)); got != end {
				t.Fatalf("watermark = %d, want %d", got, end)
			}
			keys := e.allKeys(nonce)
			for i, k := range keys {
				want := 1
				if i == 3 { // B/p2, the group that failed
					want = 2
				}
				if n := e.attempts(k); n != want {
					t.Errorf("%s attempted %d times, want %d", k, n, want)
				}
				if e.storedCount(k) != 1 {
					t.Errorf("%s stored %d times", k, e.storedCount(k))
				}
				wantHeads := 0
				if i == 3 || v.noMarks {
					wantHeads = 1
				}
				if n := e.u.headCount(k); n != wantHeads {
					t.Errorf("%s HEADed %d times, want %d", k, n, wantHeads)
				}
			}
			if got := e.bucketRows(); got != 4*idemRowsPerGroup {
				t.Fatalf("bucket holds %d rows, want exactly %d", got, 4*idemRowsPerGroup)
			}
			if v.list {
				e.list()
				extra := int64(0)
				if strings.HasPrefix(v.name, "older") {
					extra = 5
				}
				if rows, files := e.liveRows(); rows != 4*idemRowsPerGroup+extra || files < 4 {
					t.Fatalf("after the listing: live %d rows in %d files, want %d", rows, files, 4*idemRowsPerGroup+extra)
				}
			}
		})
	}
}

// The stored mark is best effort: when it cannot be written (here its path is a
// directory) the group is still committed at once and the window still commits.
// A failed record therefore can never leave a stored, uncommitted object behind
// to be adopted, compacted, forgotten and then sent again by the retry.
func TestBufferFlusher_StoredMarkFailureDoesNotFailTheGroup(t *testing.T) {
	e := newIdemEnv(t)
	e.seed()
	f := e.flusher(1, time.Nanosecond)
	last, end := e.window()
	if err := os.Mkdir(f.storedPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	errs0 := metrics.BufferFlushErrors.Get("mark")

	if got := f.tick(context.Background(), last, end); got != end {
		t.Fatalf("watermark = %d, want %d: a lost mark must not fail the window", got, end)
	}
	e.mustLive(4*idemRowsPerGroup, 4, "all groups committed")
	if n := e.totalAttempts(); n != 4 {
		t.Fatalf("%d upload attempts, want 4", n)
	}
	if d := metrics.BufferFlushErrors.Get("mark") - errs0; d != 4 {
		t.Errorf("mark errors counted = %d, want 4", d)
	}
}
