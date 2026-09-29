package parquets3

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// flusherBuffer is the subset of the logstorage-native buffer the flusher needs:
// enumerate tenants with data in a window, and stream their rows back. The
// membuffer.Store satisfies it. Kept separate from LocalBuffer so the read-path
// interface (and its test spies) stay unchanged.
type flusherBuffer interface {
	RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error
	GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error)
	// DebugFlush forces the buffer's in-memory rowsBuffer to a queryable part.
	// The flusher MUST call this before reading: logstorage's RunQuery does NOT
	// see un-flushed rows, so without it the most-recent ingest is invisible and
	// would be skipped past by the advancing watermark (permanent loss).
	DebugFlush()
}

// defaultFlushLatencyOffset is how far behind now() the flusher stops: a window
// is only committed once it is this far in the past, so spans whose _time falls
// in it but which arrive late (ingestion lag, trace completion) have landed
// before the watermark advances past their _time. Rows in (now-offset, now] stay
// in the buffer and are served by the read-merge until then. This is the lateness
// tolerance — set it above the p99 (ingest_time - _time). 2m covers typical trace
// completion + ingest lag; 30s was too small and dropped late spans (the residual
// per-window under-count).
const defaultFlushLatencyOffset = 2 * time.Minute

// FlushRowFilter is the gate-at-flush predicate: it returns true to KEEP a
// reconstructed row, false to drop it. It is injected from main so the buffer
// (a raw query cache) can be filtered to exactly what the legacy authoritative
// path would have written — dropping VT-internal trace_id_idx rows and rows whose
// stream exceeds the per-tenant cardinality limit — WITHOUT this package
// importing the vlstorage gate. A nil filter keeps every row.
type FlushRowFilter func(accountID, projectID uint32, stream string) bool

// BufferFlusher makes the logstorage-native buffer the AUTHORITATIVE Parquet
// producer: on a ticker it queries the buffer for the just-elapsed window,
// reconstructs schema.schema.TraceRow via DataBlockToschema.TraceRows, applies the gate-at-flush
// filter, and uploads the rows through the same per-group machinery the legacy
// flush uses (upload + manifest + bloom + stats + sidecar), so the Parquet it
// writes is identical to the legacy flush.
//
// Durability is the buffer's own restore-on-open plus a persisted watermark.
// The invariant: an object's bytes never change once uploaded, and no key is
// uploaded twice with different content. To keep it:
//
//   - a window is recorded as pending BEFORE any of its uploads, durably (tmp
//     file, fsync, rename, fsync of the directory): its end, a random nonce, and
//     the (account, project, partition) and row count of every group. Keys are
//     derived from those, never stored. Every write of the watermark puts the
//     same content into <name>.prev first and then into the main file, so a
//     corrupt file is only ever one of the two and the other still holds the
//     pending record;
//   - after each PUT a "stored" mark is appended to <name>.stored and fsynced
//     (and the directory when the file is created) before the object is
//     committed to the manifest, so anything compaction can see has a durable
//     mark. A failed mark write never fails the group (its object is stored and
//     committed at once) but is counted under stage "mark";
//   - a group that fails in this process is retried exactly (same key, same
//     bytes), without collecting the window again; a successful PUT is committed
//     to the manifest at once;
//   - after a restart every group of the pending record is settled by, in order:
//     a stored mark, the manifest having retired the key (its recorded rows are
//     counted as superseded), the manifest having the key, or an object-store
//     HEAD finding the object. Only a group whose HEAD says "absent" is uploaded,
//     from a re-collected window; if every group is settled the buffer is not
//     read at all. A HEAD error waits for the next tick: it never guesses.
//     Recovery does not depend on the manifest having been refreshed. HEAD needs
//     s3:ListBucket (else S3 answers 403 for an absent key) and read-after-write
//     consistency;
//   - a group that HEAD reports absent whose rows are gone from the buffer
//     (retention expired, or the flush filter changed) is uploaded with what is
//     left, the window commits, and the shortfall is counted in
//     lakehouse_insert_rows_lost_total{reason="buffer_expired"};
//   - within one attempt, once a tenant's group fails its later partitions are
//     not attempted, so a newer stored partition never hides the buffer rows of
//     an older unstored one (the buffer read watermark is the tenant's newest
//     stored MaxTimeNs); other tenants proceed;
//   - the watermark advances only when every group is stored or settled, and a
//     failed watermark save is retried without any upload.
//
// Residuals: a mark write that failed, followed by compaction of the object and
// the retired record being forgotten before a restart, re-sends the group; a
// peer compacting an unrecorded object while this node is down re-sends it
// (#37); a PUT from the dead process still in flight can land after the recovery
// HEAD; an in-process timed-out PUT that is adopted, compacted and forgotten
// before its retry is re-sent; rows lost to buffer expiry are counted, not
// recovered. Rows that arrive for a pending window after its first attempt are
// never written to Parquet by an in-process retry (like rows later than the
// latency tolerance); they are visible only while they are newer than the
// tenant's cold watermark and within buffer retention. Objects recovered by HEAD
// are registered as bare listing entries until compaction rewrites them (#246).
// The legacy staging path has its own partial-failure visibility gap (#245).
//
// Wired DORMANT (BufferFlushEnabled defaults false): nothing runs until an
// operator opts in, and the legacy path stays authoritative until the cutover.
type BufferFlusher struct {
	writer        *BatchWriter
	buffer        flusherBuffer
	keep          FlushRowFilter
	watermarkPath string
	latencyOffset int64 // ns; flush only up to now-latencyOffset
	targetBytes   int64 // S3 object-size trigger: flush a window once it reaches this
	maxLinger     int64 // ns; force-flush a window this old even if below targetBytes

	// State of the window being flushed, when there is one (see tick).
	pending   int64                      // its end; 0 when none
	start     int64                      // its start (the committed watermark)
	nonce     string                     // names its objects
	refs      map[flushGroupRef]int64    // the groups in the durable pending record, with their row counts
	stored    map[flushGroupRef]struct{} // groups marked stored (best effort)
	failed    []*traceGroupUpload        // groups the last attempt could not write
	recovered bool                       // loaded from disk: not yet resumed
	prepared  bool                       // Prepare loaded the watermark
	startLast int64                      // the watermark Prepare loaded
}

// estBytesPerTraceRow is a rough raw-bytes-per-span estimate used only to decide
// WHEN a window is big enough to flush (the size gate). It needn't be exact — it
// just keeps the flusher producing ~targetBytes S3 objects instead of one tiny
// object per tick.
const estBytesPerTraceRow = 512

// NewBufferFlusher builds a flusher. watermarkDir should be the buffer's data
// dir (persistent). keep may be nil. targetBytes is the S3 object-size flush
// trigger (e.g. insert.target_file_size); maxLinger caps how long a sub-target
// window waits before being flushed anyway. Both fall back to sane defaults.
func NewBufferFlusher(writer *BatchWriter, buffer flusherBuffer, watermarkDir string, keep FlushRowFilter, targetBytes int64, maxLinger time.Duration) *BufferFlusher {
	if targetBytes <= 0 {
		targetBytes = 128 << 20 // 128 MiB
	}
	if maxLinger <= 0 {
		maxLinger = 5 * time.Minute
	}
	return &BufferFlusher{
		writer:        writer,
		buffer:        buffer,
		keep:          keep,
		watermarkPath: filepath.Join(watermarkDir, "buffer_flush_watermark.json"),
		latencyOffset: int64(defaultFlushLatencyOffset),
		targetBytes:   targetBytes,
		maxLinger:     int64(maxLinger),
	}
}

// flushGroupRef identifies one group of a window: one tenant's rows of one
// partition. With the window's nonce it determines the group's object key.
type flushGroupRef struct {
	Account   uint32 `json:"account"`
	Project   uint32 `json:"project"`
	Partition string `json:"partition"`
}

// flushGroupRec is a group as persisted: its identity and the rows it held when
// it was recorded, which recovery uses to count what the buffer no longer has
// and the rows of a group whose object was superseded.
type flushGroupRec struct {
	flushGroupRef
	Rows int64 `json:"rows"`
}

func sortRefs(refs []flushGroupRef) {
	sort.Slice(refs, func(i, j int) bool {
		a, b := refs[i], refs[j]
		if a.Account != b.Account {
			return a.Account < b.Account
		}
		if a.Project != b.Project {
			return a.Project < b.Project
		}
		return a.Partition < b.Partition
	})
}

// flushWatermark is the flusher's persisted state.
//
// A window is committed when LastFlushWindowEndNs reaches its end. While a
// window is being flushed it is also recorded as pending, with the nonce that
// names its objects and every group it will upload, written before the first
// upload so a restart knows what to look for.
type flushWatermark struct {
	LastFlushWindowEndNs int64 `json:"last_flush_window_end_ns"`
	// PendingWindowEndNs is the end of the window being flushed.
	PendingWindowEndNs int64 `json:"pending_window_end_ns,omitempty"`
	// PendingNonce is random, drawn when the window becomes pending. It is part
	// of every object name of the window, so two flushers (or two incarnations
	// of one) never derive the same key for different bytes.
	PendingNonce string `json:"pending_nonce,omitempty"`
	// PendingGroups is the sorted set of groups of the window, each with the
	// number of rows it was recorded with. Keys are derived from the group and
	// the nonce.
	PendingGroups []flushGroupRec `json:"pending_groups,omitempty"`
	Version       int             `json:"version"`
}

// watermarkVersion is the format written. A file of an older version, or a
// pending window without a nonce, is read as having no pending window.
const watermarkVersion = 3

func parseWatermark(b []byte) (flushWatermark, error) {
	var wm flushWatermark
	if err := json.Unmarshal(b, &wm); err != nil {
		return wm, err
	}
	if wm.LastFlushWindowEndNs <= 0 {
		return wm, fmt.Errorf("no valid last_flush_window_end_ns")
	}
	return wm, nil
}

func (f *BufferFlusher) prevPath() string   { return f.watermarkPath + ".prev" }
func (f *BufferFlusher) storedPath() string { return f.watermarkPath + ".stored" }

// loadWatermark returns the persisted window-end, or fallbackNs when there is no
// watermark yet (a first start begins at the flip point rather than re-flushing
// data the legacy path already handled). It reads the main file and falls back
// to the previous good one. A file that exists but cannot be read, with no
// readable backup, is an error: guessing "now" would silently skip data.
//
// A valid pending window is restored and marked recovered.
func (f *BufferFlusher) loadWatermark(fallbackNs int64) (int64, error) {
	f.clearPending()
	mainB, mainErr := os.ReadFile(f.watermarkPath)
	var wm flushWatermark
	if mainErr == nil {
		var perr error
		if wm, perr = parseWatermark(mainB); perr != nil {
			mainErr = perr
		}
	}
	if mainErr != nil {
		prevB, prevErr := os.ReadFile(f.prevPath())
		if prevErr == nil {
			wm, prevErr = parseWatermark(prevB)
		}
		switch {
		case prevErr == nil:
			logger.Warnf("buffer flusher: watermark %s is unreadable (%v); using the previous good copy %s", f.watermarkPath, mainErr, f.prevPath())
		case os.IsNotExist(mainErr) && os.IsNotExist(prevErr):
			return fallbackNs, nil
		default:
			return 0, fmt.Errorf("buffer flush watermark %s is unreadable (%v) and so is its backup %s (%v); delete both files to start from now (data between the last committed window and now that was not flushed will then not be flushed)", f.watermarkPath, mainErr, f.prevPath(), prevErr)
		}
	}
	if wm.Version >= watermarkVersion && wm.PendingNonce != "" && wm.PendingWindowEndNs > wm.LastFlushWindowEndNs {
		f.pending = wm.PendingWindowEndNs
		f.start = wm.LastFlushWindowEndNs
		f.nonce = wm.PendingNonce
		f.refs = make(map[flushGroupRef]int64, len(wm.PendingGroups))
		for _, g := range wm.PendingGroups {
			f.refs[g.flushGroupRef] = g.Rows
		}
		f.stored = f.loadStoredMarks()
		f.recovered = true
	}
	return wm.LastFlushWindowEndNs, nil
}

// clearPending forgets the in-memory pending window.
func (f *BufferFlusher) clearPending() {
	f.pending = 0
	f.start = 0
	f.nonce = ""
	f.refs = nil
	f.stored = nil
	f.failed = nil
	f.recovered = false
}

// loadStoredMarks reads the best-effort "stored" marks of the pending window. A
// torn last line, or a mark of another window (another nonce), is ignored.
func (f *BufferFlusher) loadStoredMarks() map[flushGroupRef]struct{} {
	out := map[flushGroupRef]struct{}{}
	b, err := os.ReadFile(f.storedPath())
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		p := strings.Split(line, "\t")
		if len(p) != 4 || p[0] != f.nonce {
			continue
		}
		a, errA := strconv.ParseUint(p[1], 10, 32)
		pr, errP := strconv.ParseUint(p[2], 10, 32)
		if errA != nil || errP != nil || p[3] == "" {
			continue
		}
		ref := flushGroupRef{Account: uint32(a), Project: uint32(pr), Partition: p[3]}
		if _, ok := f.refs[ref]; ok {
			out[ref] = struct{}{}
		}
	}
	return out
}

// fsyncFile is os.File.Sync; a variable so a test can observe that marks and
// watermarks are made durable.
var fsyncFile = func(fh *os.File) error { return fh.Sync() }

// markStored appends a group to the stored marks and makes the mark durable:
// the file is fsynced after the append, and the directory when the file is
// created. onStored runs before the manifest commit, so anything compaction can
// see already has a durable mark. A failure never fails the group — its object
// is stored and is committed at once — but it is counted (stage "mark") because
// a group without a mark depends on HEAD after a restart.
func (f *BufferFlusher) markStored(ref flushGroupRef) {
	if f.stored != nil {
		f.stored[ref] = struct{}{}
	}
	_, statErr := os.Stat(f.storedPath())
	created := os.IsNotExist(statErr)
	fh, err := os.OpenFile(f.storedPath(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err == nil {
		_, err = fmt.Fprintf(fh, "%s\t%d\t%d\t%s\n", f.nonce, ref.Account, ref.Project, ref.Partition)
		if err == nil {
			err = fsyncFile(fh)
		}
		if cerr := fh.Close(); err == nil {
			err = cerr
		}
	}
	if err == nil && created {
		err = syncDir(filepath.Dir(f.storedPath()))
	}
	if err != nil {
		metrics.BufferFlushErrors.Inc("mark")
		logger.Warnf("buffer flusher: cannot record a durable stored mark for %s (recovery will HEAD the object instead): %s", ref.Partition, err)
	}
}

// saveWatermark persists the window-end durably, which also clears the pending
// record, and then drops the stored marks.
func (f *BufferFlusher) saveWatermark(endNs int64) error {
	if err := f.writeWatermark(flushWatermark{LastFlushWindowEndNs: endNs, Version: watermarkVersion}); err != nil {
		return err
	}
	_ = os.Remove(f.storedPath())
	metrics.InsertFlushWatermarkNs.Set(endNs)
	return nil
}

// writeIntent records the pending window and the groups in f.refs, durably,
// before any of their uploads.
func (f *BufferFlusher) writeIntent(lastNs int64) error {
	refs := make([]flushGroupRef, 0, len(f.refs))
	for r := range f.refs {
		refs = append(refs, r)
	}
	sortRefs(refs)
	recs := make([]flushGroupRec, len(refs))
	for i, r := range refs {
		recs[i] = flushGroupRec{flushGroupRef: r, Rows: f.refs[r]}
	}
	return f.writeWatermark(flushWatermark{
		LastFlushWindowEndNs: lastNs,
		PendingWindowEndNs:   f.pending,
		PendingNonce:         f.nonce,
		PendingGroups:        recs,
		Version:              watermarkVersion,
	})
}

// writeWatermark replaces the watermark durably. Every write puts the SAME
// content into <name>.prev first and then into the main file, each through a
// temp file: write, fsync, rename, fsync of the directory. A torn or corrupt
// file can therefore only ever be one of the two, and the other holds a state
// that is safe to resume from — including the pending record. Main is preferred
// on load: when a crash separates the two writes, main is the older, still valid
// state, and nothing of the newer one had started yet (the record is written
// before the PUTs it describes).
func (f *BufferFlusher) writeWatermark(wm flushWatermark) error {
	b, err := json.Marshal(wm)
	if err != nil {
		return err
	}
	if err := writeFileDurable(f.prevPath(), b); err != nil {
		return fmt.Errorf("write the watermark backup: %w", err)
	}
	return writeFileDurable(f.watermarkPath, b)
}

// syncDir fsyncs a directory so a rename or a created file in it survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return fsyncFile(d)
}

// writeFileDurable writes data to path via a temp file: write, fsync, rename,
// fsync of the directory.
func writeFileDurable(path string, data []byte) error {
	tmp := path + ".tmp"
	fh, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := fh.Write(data); err != nil {
		_ = fh.Close()
		return err
	}
	if err := fsyncFile(fh); err != nil {
		_ = fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// collectWindow gathers (startNs, endNs] from the buffer, per tenant, applying
// the gate-at-flush filter. It returns the per-tenant rows and the total row
// count (for the size gate). It does NOT write anything — the caller decides
// whether the window is big enough (or old enough) to flush.
func (f *BufferFlusher) collectWindow(ctx context.Context, startNs, endNs int64) (map[logstorage.TenantID][]schema.TraceRow, int, error) {
	tenants, err := f.buffer.GetTenantIDs(ctx, startNs, endNs)
	if err != nil {
		return nil, 0, err
	}
	out := make(map[logstorage.TenantID][]schema.TraceRow, len(tenants))
	total := 0
	for _, tenant := range tenants {
		rows, err := f.collectTenantRows(ctx, tenant, startNs, endNs)
		if err != nil {
			return nil, 0, err
		}
		if len(rows) > 0 {
			out[tenant] = rows
			total += len(rows)
		}
	}
	return out, total, nil
}

// windowBatchID names the object one tenant's rows of one partition get when
// the window (startNs, endNs] is flushed under nonce. The nonce is drawn when
// the window becomes pending and persisted with it, so a resumed window names
// the same objects while a different flusher, or a later window over the same
// range, cannot collide with them. 16 hex characters, like any batch id.
func windowBatchID(nonce string, startNs, endNs int64, accountID, projectID uint32, partition string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d-%d-%d-%s", nonce, startNs, endNs, accountID, projectID, partition)))
	return fmt.Sprintf("%x", sum[:8])
}

// keyFor is the object key of a group of the pending window.
func (f *BufferFlusher) keyFor(ref flushGroupRef) string {
	up := &traceGroupUpload{partition: ref.Partition, accountID: ref.Account, projectID: ref.Project,
		batchID: windowBatchID(f.nonce, f.start, f.pending, ref.Account, ref.Project, ref.Partition)}
	return f.writer.assignTraceKey(up)
}

// buildGroups splits the rows collected for the pending window into one upload
// per tenant and partition, keyed by windowBatchID and ordered by (account,
// project, partition) — so a tenant's partitions come oldest first and which
// group an error hits first does not depend on map iteration. Each group marks
// itself stored through markStored.
func (f *BufferFlusher) buildGroups(collected map[logstorage.TenantID][]schema.TraceRow) []*traceGroupUpload {
	tenants := make([]logstorage.TenantID, 0, len(collected))
	for t := range collected {
		tenants = append(tenants, t)
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].AccountID != tenants[j].AccountID {
			return tenants[i].AccountID < tenants[j].AccountID
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	var groups []*traceGroupUpload
	for _, tenant := range tenants {
		byPartition := map[string][]schema.TraceRow{}
		for _, r := range collected[tenant] {
			p := partitionFromNano(r.TimestampUnixNano)
			byPartition[p] = append(byPartition[p], r)
		}
		parts := make([]string, 0, len(byPartition))
		for p := range byPartition {
			parts = append(parts, p)
		}
		sort.Strings(parts)
		for _, p := range parts {
			name := func(accountID, projectID uint32) string {
				return windowBatchID(f.nonce, f.start, f.pending, accountID, projectID, p)
			}
			for _, up := range f.writer.buildTraceGroups(p, byPartition[p], name) {
				f.writer.assignTraceKey(up)
				ref := refOf(up)
				up.onStored = func(string) { f.markStored(ref) }
				groups = append(groups, up)
			}
		}
	}
	return groups
}

func refOf(up *traceGroupUpload) flushGroupRef {
	return flushGroupRef{Account: up.accountID, Project: up.projectID, Partition: up.partition}
}

// collectTenantRows queries the buffer for one tenant over (startNs, endNs],
// reconstructs TraceRows, and applies the gate-at-flush filter (drop trace_id_idx
// + cardinality-exceeding rows) so the authoritative Parquet matches the legacy
// path exactly.
func (f *BufferFlusher) collectTenantRows(ctx context.Context, tenant logstorage.TenantID, startNs, endNs int64) ([]schema.TraceRow, error) {
	q, err := logstorage.ParseQueryAtTimestamp("*", endNs)
	if err != nil {
		return nil, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)

	// logstorage invokes writeBlock from MULTIPLE goroutines concurrently, so the
	// shared slice (and any filter side effects) must be synchronized.
	var mu sync.Mutex
	var rows []schema.TraceRow
	err = f.buffer.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		local := make([]schema.TraceRow, 0, db.RowsCount())
		for _, r := range vlstorage.DataBlockToTraceRows(db, tenant) {
			if f.keep != nil && !f.keep(r.AccountID, r.ProjectID, r.Stream) {
				continue
			}
			local = append(local, r)
		}
		mu.Lock()
		rows = append(rows, local...)
		mu.Unlock()
	})
	return rows, err
}

// Run drives the flusher until ctx is cancelled. checkInterval is how often the
// flusher wakes to (re-)evaluate the window for the size/linger gate — NOT the
// flush cadence (a window flushes on targetBytes OR maxLinger).
//
// CRASH-SAFETY: the watermark advances (and is persisted, atomically) ONLY after
// a window's Parquet is fully written. So a crash mid-window leaves the watermark
// at the last committed boundary; the rows live in the persisted buffer
// (logstorage parts on disk, restored on open), and on restart loadWatermark
// resumes from that boundary: it finishes the pending window if there is one, then
// flushes (last, now-offset] — no loss, no LH WAL. The only requirement is buffer_retention > maxLinger + downtime so the
// un-flushed data hasn't aged out of the buffer before recovery. A FRESH flip
// (no watermark file) starts at nowNs (the flip point) so pre-flip data — already
// owned by the legacy path — is never double-flushed.
// Prepare loads the persisted watermark (falling back to nowNs on a first
// start). A watermark that exists but cannot be read is an error: the caller
// must not start the flusher, because starting from "now" would skip data. Run
// calls it when the caller has not, and does not run on an error.
func (f *BufferFlusher) Prepare(nowNs int64) error {
	last, err := f.loadWatermark(nowNs)
	if err != nil {
		return err
	}
	f.startLast = last
	f.prepared = true
	return nil
}

func (f *BufferFlusher) Run(ctx context.Context, checkInterval time.Duration, nowNs int64) {
	if checkInterval <= 0 {
		checkInterval = 30 * time.Second
	}
	if !f.prepared {
		if err := f.Prepare(nowNs); err != nil {
			logger.Errorf("buffer flusher NOT started: %s", err)
			return
		}
	}
	last := f.startLast
	t := time.NewTicker(checkInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			last = f.tick(ctx, last, tick.UnixNano())
		}
	}
}

// tick runs one flush attempt and returns the watermark after it.
//
// A window is either fresh (nothing pending), pending after a restart
// (recovered), or pending in this process after a failed attempt:
//
//   - fresh: gate, collect, record the window and its groups durably, then
//     attempt them;
//   - recovered: settle each recorded group (stored mark, retired, live in the
//     manifest, or found by HEAD); re-collect (start, pending] only if some
//     group is absent, record groups the rows introduce that the record lacks,
//     and upload just the absent ones;
//   - in-process retry: attempt the groups that failed, as they are (same keys,
//     same bytes), without collecting again.
//
// The watermark moves to the window's end only when every group is stored or
// settled. If saving it fails, the next tick uploads nothing and saves again.
func (f *BufferFlusher) tick(ctx context.Context, last, nowNs int64) int64 {
	switch {
	case f.pending == 0:
		return f.tickFresh(ctx, last, nowNs)
	case f.recovered:
		return f.tickRecovered(ctx, last)
	default:
		return f.attempt(ctx, last, f.failed)
	}
}

func (f *BufferFlusher) tickFresh(ctx context.Context, last, nowNs int64) int64 {
	// Stop short of now by latencyOffset so in-flight/late rows land before
	// their window is committed.
	flushEnd := nowNs - f.latencyOffset
	if flushEnd <= last {
		return last
	}
	// Make all ingested rows queryable; RunQuery does NOT see the un-flushed
	// rowsBuffer, so without this the recent window is invisible and the
	// watermark would skip past it (the under-production bug).
	f.buffer.DebugFlush()
	collected, nRows, err := f.collectWindow(ctx, last, flushEnd)
	if err != nil {
		metrics.BufferFlushErrors.Inc("collect")
		logger.Warnf("buffer flusher: collect (%d,%d] failed, will retry: %s", last, flushEnd, err)
		return last
	}
	aged := flushEnd-last >= f.maxLinger
	// Size gate: hold a sub-target window open across ticks so the S3 object
	// lands at ~targetBytes instead of one tiny file per tick.
	if int64(nRows)*estBytesPerTraceRow < f.targetBytes && !aged {
		return last // accumulate — don't flush, don't advance the watermark
	}
	if nRows == 0 {
		if err := f.saveWatermark(flushEnd); err != nil {
			metrics.BufferFlushErrors.Inc("watermark")
			logger.Warnf("buffer flusher: watermark persist failed, will retry: %s", err)
			return last
		}
		return flushEnd
	}
	f.pending, f.start, f.nonce, f.failed, f.recovered = flushEnd, last, randomBatchID(), nil, false
	f.refs, f.stored = map[flushGroupRef]int64{}, map[flushGroupRef]struct{}{}
	groups := f.buildGroups(collected)
	for _, up := range groups {
		f.refs[refOf(up)] = int64(len(up.rows))
	}
	// The intent is durable before any PUT: a crash from here on knows which
	// objects to look for.
	if err := f.writeIntent(last); err != nil {
		metrics.BufferFlushErrors.Inc("intent")
		logger.Warnf("buffer flusher: cannot record window (%d,%d] before flushing it, will retry: %s", last, flushEnd, err)
		f.clearPending()
		return last
	}
	return f.attempt(ctx, last, groups)
}

func (f *BufferFlusher) tickRecovered(ctx context.Context, last int64) int64 {
	m := f.writer.manifest
	refs := make([]flushGroupRef, 0, len(f.refs))
	for r := range f.refs {
		refs = append(refs, r)
	}
	sortRefs(refs)
	need := map[flushGroupRef]bool{}
	var retired []flushGroupRef
	for _, ref := range refs {
		if _, ok := f.stored[ref]; ok {
			continue
		}
		key := f.keyFor(ref)
		switch {
		case m.IsRetired(key):
			retired = append(retired, ref)
		case m.HasKey(key):
		default:
			// Not in this manifest: ask the object store. The manifest may be
			// behind (a snapshot, a listing that missed the object), so its
			// silence proves nothing; an error proves nothing either.
			ok, err := f.writer.objectExists(ctx, ref.Account, ref.Project, key)
			if err != nil {
				metrics.BufferFlushErrors.Inc("head")
				logger.Warnf("buffer flusher: cannot check whether %s exists, will retry: %s", key, err)
				return last
			}
			if !ok {
				need[ref] = true
			}
		}
	}
	// The buffer is read only when some group has to be uploaded: a window whose
	// groups are all settled must commit even if the buffer cannot be queried.
	var todo []*traceGroupUpload
	var added []flushGroupRef
	var shortfalls []flushShortfall
	if len(need) > 0 {
		f.buffer.DebugFlush()
		collected, _, err := f.collectWindow(ctx, last, f.pending)
		if err != nil {
			metrics.BufferFlushErrors.Inc("collect")
			logger.Warnf("buffer flusher: collect pending window (%d,%d] failed, will retry: %s", last, f.pending, err)
			return last
		}
		found := map[flushGroupRef]bool{}
		for _, up := range f.buildGroups(collected) {
			ref := refOf(up)
			found[ref] = true
			switch {
			case need[ref]:
				todo = append(todo, up)
				if got := int64(len(up.rows)); got < f.refs[ref] {
					shortfalls = append(shortfalls, flushShortfall{ref: ref, recorded: f.refs[ref], got: got})
				}
			default:
				if _, known := f.refs[ref]; !known {
					// Rows the record never had: never uploaded. Record before
					// uploading.
					added = append(added, ref)
					f.refs[ref] = int64(len(up.rows))
					todo = append(todo, up)
				}
			}
		}
		for _, ref := range refs {
			if need[ref] && !found[ref] {
				shortfalls = append(shortfalls, flushShortfall{ref: ref, recorded: f.refs[ref], got: 0})
			}
		}
		if len(added) > 0 {
			if err := f.writeIntent(last); err != nil {
				for _, ref := range added {
					delete(f.refs, ref)
				}
				metrics.BufferFlushErrors.Inc("intent")
				logger.Warnf("buffer flusher: cannot record the new groups of window (%d,%d], will retry: %s", last, f.pending, err)
				return last
			}
		}
	}
	// Everything that could fail and be retried has passed: count now, once.
	for _, sf := range shortfalls {
		lost := sf.recorded - sf.got
		metrics.InsertRowsLost.Add("buffer_expired", int(lost))
		metrics.BufferFlushErrors.Inc("missing")
		logger.Errorf("buffer flusher: group %s of tenant %d:%d in window (%d,%d] was recorded with %d rows but the buffer now has %d and the object store does not have it: %d rows are lost (buffer retention expired, or the flush filter changed); uploading what is left",
			sf.ref.Partition, sf.ref.Account, sf.ref.Project, last, f.pending, sf.recorded, sf.got, lost)
	}
	for _, ref := range retired {
		// Superseded by whatever retired the key: its rows live there. The
		// record knows how many there were, so nothing is collected for this.
		metrics.InsertRowsSuperseded.Add(int(f.refs[ref]))
		logger.Warnf("buffer flusher: group %s of tenant %d:%d was retired (compacted, rewritten or removed) before its window committed; rows=%d", ref.Partition, ref.Account, ref.Project, f.refs[ref])
	}
	f.recovered = false
	return f.attempt(ctx, last, todo)
}

// flushShortfall is a group recovery must upload but whose rows the buffer no
// longer fully has.
type flushShortfall struct {
	ref           flushGroupRef
	recorded, got int64
}

// attempt uploads groups and, if every one is stored or settled, commits the
// pending window. Once a tenant's group fails its later partitions are not
// attempted (they join the failed set, unencoded): the buffer read watermark is
// the tenant's newest stored MaxTimeNs, so storing a newer partition would hide
// the buffer rows of the older one that failed. Other tenants proceed.
func (f *BufferFlusher) attempt(ctx context.Context, last int64, groups []*traceGroupUpload) int64 {
	var failed []*traceGroupUpload
	var firstErr error
	blocked := map[[2]uint32]bool{}
	for _, up := range groups {
		tenant := [2]uint32{up.accountID, up.projectID}
		if blocked[tenant] {
			failed = append(failed, up)
			continue
		}
		err := ctx.Err()
		if err == nil {
			err = f.writer.uploadTraceGroup(ctx, up)
		}
		if err != nil {
			failed = append(failed, up)
			blocked[tenant] = true
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	f.failed = failed
	if len(failed) > 0 {
		metrics.BufferFlushErrors.Inc("upload")
		logger.Warnf("buffer flusher: flush (%d,%d] left %d group(s) unwritten, will retry them: %v", last, f.pending, len(failed), firstErr)
		return last
	}
	end := f.pending
	if err := f.saveWatermark(end); err != nil {
		metrics.BufferFlushErrors.Inc("watermark")
		logger.Warnf("buffer flusher: watermark persist failed after the window was stored; the next tick saves it again without uploading: %s", err)
		return last
	}
	f.clearPending()
	return end
}
