package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// FlushRowFilter is the gate-at-flush predicate: it returns true to KEEP a
// row, false to drop it. It is injected from main so this package does not
// import the vlstorage gates. A nil filter keeps every row.
type FlushRowFilter func(accountID, projectID uint32, stream string) bool

// flushSegments is the part of the segmented insert buffer the flusher drives.
// *membuffer.Segments has it.
type flushSegments interface {
	Active() *membuffer.Segment
	Seal() (*membuffer.Segment, bool)
	Pending() []*membuffer.Segment
	Commit(g *membuffer.Segment, at time.Time)
	CommitThrough(seq uint64, at time.Time)
	Reap(now time.Time, grace time.Duration) int
	Stats(now time.Time) membuffer.Stats
}

// BufferFlusher drains the insert buffer to Parquet on S3, one sealed segment
// at a time, and is the only producer of Parquet from ingested rows.
//
// The buffer is cut by ingest time (membuffer.Segments): every acknowledged
// row is in exactly one segment, so draining every segment completely writes
// every row once, whatever its _time — late and backfilled rows included.
//
// The invariant: an object's bytes never change once uploaded, and no key is
// uploaded twice with different content.
//
//   - A sealed segment is immutable and durable (upstream closed and reopened
//     it), so its groups — one tenant, one hour partition, one slice of at most
//     maxRows rows — and their keys (<nonce>-<slice>) are the same in every
//     process. The segment itself is the record of what has to be written; no
//     list of groups is persisted.
//   - Before a segment's first PUT, the state file records it as draining,
//     durably (temp file, fsync, rename, fsync of the directory, mirrored to
//     .prev first). After each PUT a "stored" mark is appended to
//     <state>.stored and fsynced before the object is committed to the
//     manifest.
//   - A group is settled, in order, by: its stored mark, the manifest having
//     retired its key (its rows live in what replaced it), the manifest having
//     the key, or — only for a segment a previous process was draining — an
//     object-store HEAD finding the object. A HEAD error waits for the next
//     attempt: it never guesses. Every other group is collected from the
//     segment and uploaded; a failed upload is retried with the same bytes.
//   - When every group is settled, a marker {prefix}_segments/<nonce> is
//     written to the object store (compaction and delete rewrites on any node
//     leave the segment's objects alone until the grace after it), the state
//     records the segment as committed (committed_through_seq), the marks are
//     dropped and the segment is kept readable for the grace period, then
//     removed.
//
// Memory is bounded by maxRows per group, whatever the segment's size: the
// slices are planned from per-second row counts of the segment.
//
// Residuals: a PUT still in flight from a dead process can land after the
// recovery HEAD said the object was absent, and the restarted flusher then
// uploads the same key with possibly different bytes; a failed mark write
// followed by compaction of the object and the retired record being forgotten
// before a restart re-sends that group (counted under stage "mark"); a peer
// compacting an object of a draining segment while this node is down (#37).
type BufferFlusher struct {
	writer    *BatchWriter
	segs      flushSegments
	keep      FlushRowFilter
	statePath string
	maxRows   int64         // rows per group
	maxAge    time.Duration // seal the active segment after this long
	sealBytes int64         // ... or once its estimated raw bytes reach this
	grace     time.Duration // keep a committed segment readable this long

	state      flushState
	marksFor   string                     // nonce the in-memory marks belong to
	marks      map[flushGroupRef]struct{} // groups of the draining segment with a durable mark
	head       bool                       // a previous process was draining marksFor: ask HEAD
	markSynced bool                       // the marks file's directory entry is fsynced
	retry      map[string]*logGroupUpload // groups whose upload failed in this process, by key
	nextTry    time.Time                  // back-off after a failed drain
	backoff    time.Duration

	stopOnce sync.Once
	cancel   context.CancelFunc
	done     chan struct{}
}

// flushState is the flusher's persisted record.
type flushState struct {
	Version int `json:"version"`
	// CommittedThroughSeq: every segment with seq <= this is in Parquet.
	CommittedThroughSeq uint64 `json:"committed_through_seq"`
	// DrainingSeq is the segment whose uploads have begun; 0 when none.
	DrainingSeq uint64 `json:"draining_seq,omitempty"`
}

const flushStateVersion = 5

// flushGroupRef identifies one group of a segment.
type flushGroupRef struct {
	Account, Project uint32
	Partition        string
	Slice            int
}

// estBytesPerLogRow is a rough raw size of a row, for sealing by size.
const estBytesPerLogRow = 512

// maxSizeSealBacklog: with this many sealed segments not yet drained, the
// active segment is sealed by age only, so an object-store outage builds a few
// large segments rather than many small ones (each open segment costs about
// 150 KiB and 8 goroutines, measured).
const maxSizeSealBacklog = 64

// BufferFlusherConfig sizes the flusher.
type BufferFlusherConfig struct {
	TargetBytes int64         // object size target (insert.target_file_size)
	MaxAge      time.Duration // insert.buffer_flush_interval
	Grace       time.Duration // committed segments stay readable this long
}

func newBufferFlusher(writer *BatchWriter, segs flushSegments, stateDir string, keep FlushRowFilter, c BufferFlusherConfig) *BufferFlusher {
	if c.TargetBytes <= 0 {
		c.TargetBytes = 128 << 20
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 5 * time.Minute
	}
	if c.Grace < 0 {
		c.Grace = 0
	}
	maxRows := c.TargetBytes / estBytesPerLogRow
	if maxRows < 1 {
		maxRows = 1
	}
	return &BufferFlusher{
		writer:    writer,
		segs:      segs,
		keep:      keep,
		statePath: filepath.Join(stateDir, "buffer_flush_state.json"),
		maxRows:   maxRows,
		maxAge:    c.MaxAge,
		sealBytes: c.TargetBytes,
		grace:     c.Grace,
		retry:     map[string]*logGroupUpload{},
		done:      make(chan struct{}),
	}
}

// fatalf reports a startup error that must stop the process; a variable so tests
// can observe it.
var fatalf = logger.Fatalf

// NewBufferFlusher builds the flusher and loads its state. A state file that
// exists but cannot be read stops the process: guessing would skip or repeat
// data.
func NewBufferFlusher(writer *BatchWriter, segs flushSegments, stateDir string, keep FlushRowFilter, c BufferFlusherConfig) *BufferFlusher {
	f := newBufferFlusher(writer, segs, stateDir, keep, c)
	if err := f.load(time.Now()); err != nil {
		fatalf("the insert buffer's flush state cannot be read: %s", err)
	}
	return f
}

func (f *BufferFlusher) prevPath() string   { return f.statePath + ".prev" }
func (f *BufferFlusher) storedPath() string { return f.statePath + ".stored" }

// load reads the state (falling back to .prev) and tells the segments which of
// them are committed. A missing state means nothing is committed: every
// segment found is drained.
func (f *BufferFlusher) load(now time.Time) error {
	st, err := readFlushState(f.statePath)
	if err != nil {
		prev, perr := readFlushState(f.prevPath())
		switch {
		case perr == nil:
			logger.Warnf("buffer flusher: state %s is unreadable (%v); using the previous good copy %s", f.statePath, err, f.prevPath())
			st = prev
		case os.IsNotExist(err) && os.IsNotExist(perr):
			st = flushState{Version: flushStateVersion}
		default:
			return fmt.Errorf("%s is unreadable (%v) and so is its backup %s (%v); delete both to drain every segment present (objects already uploaded from a segment being drained are then uploaded again)", f.statePath, err, f.prevPath(), perr)
		}
	}
	f.state = st
	f.segs.CommitThrough(st.CommittedThroughSeq, now)
	return nil
}

func readFlushState(path string) (flushState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return flushState{}, err
	}
	var st flushState
	if err := json.Unmarshal(b, &st); err != nil {
		return flushState{}, err
	}
	if st.Version != flushStateVersion {
		return flushState{}, fmt.Errorf("version %d, want %d", st.Version, flushStateVersion)
	}
	return st, nil
}

// writeState replaces the state durably: the same content goes to .prev
// first and then to the main file, each through a temp file (write, fsync,
// rename, fsync of the directory). A crash leaves at most one of them torn.
func (f *BufferFlusher) writeState(st flushState) error {
	st.Version = flushStateVersion
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if err := writeFileDurable(f.prevPath(), b); err != nil {
		return fmt.Errorf("write the state backup: %w", err)
	}
	return writeFileDurable(f.statePath, b)
}

// fsyncFile is os.File.Sync; a variable so a test can observe that marks and
// the state are made durable.
var fsyncFile = func(fh *os.File) error { return fh.Sync() }

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

// loadMarks reads the stored marks of the segment named nonce. A torn last
// line, or a mark of another segment, is ignored.
func (f *BufferFlusher) loadMarks(nonce string) map[flushGroupRef]struct{} {
	out := map[flushGroupRef]struct{}{}
	b, err := os.ReadFile(f.storedPath())
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		p := strings.Split(line, "\t")
		if len(p) != 5 || p[0] != nonce || p[3] == "" {
			continue
		}
		a, errA := strconv.ParseUint(p[1], 10, 32)
		pr, errP := strconv.ParseUint(p[2], 10, 32)
		sl, errS := strconv.Atoi(p[4])
		if errA != nil || errP != nil || errS != nil {
			continue
		}
		out[flushGroupRef{Account: uint32(a), Project: uint32(pr), Partition: p[3], Slice: sl}] = struct{}{}
	}
	return out
}

// markStored appends a durable "stored" mark for ref of the draining segment:
// the file is fsynced after the append, and its directory until that succeeds
// once. It runs before the manifest commit. A failure never fails the group —
// the object is stored and is committed at once — but it is counted (stage
// "mark"): that group then depends on HEAD after a restart.
func (f *BufferFlusher) markStored(ref flushGroupRef) {
	f.marks[ref] = struct{}{}
	fh, err := os.OpenFile(f.storedPath(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err == nil {
		_, err = fmt.Fprintf(fh, "%s\t%d\t%d\t%s\t%d\n", f.marksFor, ref.Account, ref.Project, ref.Partition, ref.Slice)
		if err == nil {
			err = fsyncFile(fh)
		}
		if cerr := fh.Close(); err == nil {
			err = cerr
		}
	}
	if err == nil && !f.markSynced {
		if err = syncDir(filepath.Dir(f.storedPath())); err == nil {
			f.markSynced = true
		}
	}
	if err != nil {
		metrics.BufferFlushErrors.Inc("mark")
		logger.Warnf("buffer flusher: cannot record a durable stored mark for %s (a restart will HEAD the object instead): %s", ref.Partition, err)
	}
}

// segmentBatchID names a group's object: the segment nonce, then the slice.
// The nonce lets readers tell the objects of a live segment from every other
// object (SegmentNonceOfKey).
func segmentBatchID(nonce string, slice int) string {
	return fmt.Sprintf("%s-%x", nonce, slice)
}

// flushSlice is one group of a tenant: rows of one hour partition with
// _time in [start, end].
type flushSlice struct {
	partition  string
	slice      int
	start, end int64
	rows       int64
}

// planSlices cuts a tenant's per-second row counts into groups of at most
// maxRows rows that never cross an hour partition. A single second with more
// rows than maxRows is one group. Deterministic for the same counts.
func planSlices(perSecond map[int64]int64, maxRows int64) []flushSlice {
	secs := make([]int64, 0, len(perSecond))
	for s := range perSecond {
		secs = append(secs, s)
	}
	sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })
	var out []flushSlice
	next := map[string]int{}
	for _, sec := range secs {
		n := perSecond[sec]
		p := partitionFromNano(sec)
		if k := len(out) - 1; k >= 0 && out[k].partition == p && out[k].rows+n <= maxRows {
			out[k].rows += n
			continue
		}
		if k := len(out) - 1; k >= 0 {
			out[k].end = sec - 1
		}
		out = append(out, flushSlice{partition: p, slice: next[p], start: sec, rows: n})
		next[p]++
	}
	if k := len(out) - 1; k >= 0 {
		out[k].end = secs[len(secs)-1] + int64(time.Second) - 1
	}
	return out
}

// tenantSecondCounts returns the tenant's rows of g per second of _time
// (upstream `stats by (_time:1s) count()`).
func tenantSecondCounts(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID) (map[int64]int64, error) {
	q, err := logstorage.ParseQueryAtTimestamp(`* | stats by (_time:1s) count() as n`, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, math.MaxInt64)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	var mu sync.Mutex
	out := map[int64]int64{}
	var perr error
	err = g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		var ts, ns []string
		for _, c := range db.GetColumns(false) {
			switch c.Name {
			case "_time":
				ts = c.Values
			case "n":
				ns = c.Values
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for i := range ts {
			t, err := time.Parse(time.RFC3339Nano, ts[i])
			if err != nil {
				perr = fmt.Errorf("per-second count: _time %q: %w", ts[i], err)
				return
			}
			n, err := strconv.ParseInt(ns[i], 10, 64)
			if err != nil {
				perr = fmt.Errorf("per-second count: n %q: %w", ns[i], err)
				return
			}
			out[t.UnixNano()] += n
		}
	})
	if err == nil {
		err = perr
	}
	return out, err
}

// collectSlice reads one group's rows from g and applies the keep filter.
func (f *BufferFlusher) collectSlice(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID, sl flushSlice) ([]schema.LogRow, error) {
	q, err := logstorage.ParseQueryAtTimestamp("*", sl.end)
	if err != nil {
		return nil, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), sl.start, sl.end)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	// logstorage calls writeBlock from several goroutines.
	var mu sync.Mutex
	var rows []schema.LogRow
	err = g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		local := make([]schema.LogRow, 0, db.RowsCount())
		for _, r := range vlstorage.DataBlockToLogRows(db, tenant) {
			if f.keep != nil && !f.keep(r.AccountID, r.ProjectID, r.Stream) {
				continue
			}
			local = append(local, r)
		}
		mu.Lock()
		rows = append(rows, local...)
		mu.Unlock()
	})
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano })
	return rows, err
}

// Start runs the flusher until Stop. checkInterval is how often it checks
// whether to seal the active segment and drains what is sealed.
func (f *BufferFlusher) Start(checkInterval time.Duration) {
	if checkInterval <= 0 {
		checkInterval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go func() {
		defer close(f.done)
		t := time.NewTicker(checkInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				f.tick(ctx, now)
			}
		}
	}()
}

// Stop ends the flusher and waits for its current step to finish, so the
// segments can be closed after it. A drain cut short resumes after the
// restart.
func (f *BufferFlusher) Stop() {
	f.stopOnce.Do(func() {
		if f.cancel == nil {
			close(f.done)
			return
		}
		f.cancel()
		<-f.done
	})
}

// tick seals the active segment when it is due, drains the sealed segments in
// order (stopping at the first that cannot be finished now), and removes the
// committed segments whose grace has passed.
func (f *BufferFlusher) tick(ctx context.Context, now time.Time) {
	f.maybeSeal(now)
	if now.After(f.nextTry) || now.Equal(f.nextTry) {
		for _, g := range f.segs.Pending() {
			if ctx.Err() != nil {
				return
			}
			if err := f.drain(ctx, g); err != nil {
				if ctx.Err() != nil {
					return
				}
				f.failed(now, g, err)
				break
			}
			f.backoff = 0
			f.maybeSeal(time.Now())
		}
	}
	f.segs.Reap(time.Now(), f.grace)
	f.observe(time.Now())
}

func (f *BufferFlusher) failed(now time.Time, g *membuffer.Segment, err error) {
	switch {
	case f.backoff == 0:
		f.backoff = time.Second
	case f.backoff < 30*time.Second:
		f.backoff *= 2
	}
	f.nextTry = now.Add(f.backoff)
	logger.Warnf("buffer flusher: segment %d (%s) is not fully written yet, retrying in %s: %s", g.Seq(), g.Nonce(), f.backoff, err)
}

// maybeSeal seals the active segment once it is maxAge old, or has reached
// the size target while few segments wait to be drained.
func (f *BufferFlusher) maybeSeal(now time.Time) {
	a := f.segs.Active()
	rows := a.Rows()
	if rows == 0 {
		return
	}
	due := now.Sub(a.Created()) >= f.maxAge
	if !due && rows*estBytesPerLogRow >= f.sealBytes {
		due = len(f.segs.Pending()) < maxSizeSealBacklog
	}
	if due {
		if g, ok := f.segs.Seal(); ok {
			metrics.BufferSegmentsSealed.Inc()
			logger.Infof("buffer flusher: sealed segment %d (%s) with %d rows", g.Seq(), g.Nonce(), g.Rows())
		}
	}
}

// drain writes every group of g that is not settled yet and then commits g.
func (f *BufferFlusher) drain(ctx context.Context, g *membuffer.Segment) error {
	switch {
	case f.state.DrainingSeq != g.Seq():
		// The first upload of g in any process: record it first, durably.
		st := f.state
		st.DrainingSeq = g.Seq()
		if err := f.writeState(st); err != nil {
			metrics.BufferFlushErrors.Inc("intent")
			return fmt.Errorf("record segment %d as draining: %w", g.Seq(), err)
		}
		f.state = st
		_ = os.Remove(f.storedPath())
		f.marksFor, f.marks, f.head, f.markSynced = g.Nonce(), map[flushGroupRef]struct{}{}, false, false
	case f.marksFor != g.Nonce():
		// A previous process was draining g: its marks say what it stored;
		// any other group may have been stored too, so ask HEAD.
		f.marksFor, f.marks, f.head, f.markSynced = g.Nonce(), f.loadMarks(g.Nonce()), true, false
	}

	tenants, err := g.GetTenantIDs(ctx, 0, math.MaxInt64)
	if err != nil {
		metrics.BufferFlushErrors.Inc("collect")
		return fmt.Errorf("list tenants: %w", err)
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].AccountID != tenants[j].AccountID {
			return tenants[i].AccountID < tenants[j].AccountID
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	for _, tenant := range tenants {
		counts, err := tenantSecondCounts(ctx, g, tenant)
		if err != nil {
			metrics.BufferFlushErrors.Inc("collect")
			return fmt.Errorf("plan tenant %d:%d: %w", tenant.AccountID, tenant.ProjectID, err)
		}
		for _, sl := range planSlices(counts, f.maxRows) {
			if err := f.writeGroup(ctx, g, tenant, sl); err != nil {
				return err
			}
		}
	}

	// The marker tells compaction and delete rewrites on every node that the
	// segment's objects are complete; they leave them alone until the grace
	// after it has passed (manifest.SegmentGuard). Written before the local
	// commit: a segment committed here always has its marker.
	if err := f.writer.putSegmentMarker(ctx, g.Nonce(), g.Seq()); err != nil {
		metrics.BufferFlushErrors.Inc("marker")
		return fmt.Errorf("write the commit marker of segment %d: %w", g.Seq(), err)
	}
	st := flushState{CommittedThroughSeq: g.Seq()}
	if err := f.writeState(st); err != nil {
		metrics.BufferFlushErrors.Inc("watermark")
		return fmt.Errorf("record segment %d as committed: %w", g.Seq(), err)
	}
	f.state = st
	_ = os.Remove(f.storedPath())
	f.marksFor, f.marks, f.head = "", nil, false
	f.segs.Commit(g, time.Now())
	metrics.BufferSegmentsCommitted.Inc()
	return nil
}

// writeGroup settles or uploads one group.
func (f *BufferFlusher) writeGroup(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID, sl flushSlice) error {
	ref := flushGroupRef{Account: tenant.AccountID, Project: tenant.ProjectID, Partition: sl.partition, Slice: sl.slice}
	if _, ok := f.marks[ref]; ok {
		return nil
	}
	up := &logGroupUpload{partition: sl.partition, accountID: tenant.AccountID, projectID: tenant.ProjectID,
		batchID: segmentBatchID(g.Nonce(), sl.slice)}
	key := f.writer.assignLogKey(up)
	m := f.writer.manifest
	switch {
	case m.IsRetired(key):
		// Compacted, rewritten or removed: its rows live in what replaced it.
		f.writer.noteSuperseded(key, int(sl.rows))
		return nil
	case m.HasKey(key):
		return nil
	case f.head:
		ok, err := f.writer.objectExists(ctx, tenant.AccountID, tenant.ProjectID, key)
		if err != nil {
			metrics.BufferFlushErrors.Inc("head")
			return fmt.Errorf("check whether %s exists: %w", key, err)
		}
		if ok {
			return nil // a listing adopts it
		}
	}
	if prev, ok := f.retry[key]; ok {
		up = prev // the same bytes as the failed attempt
	} else {
		rows, err := f.collectSlice(ctx, g, tenant, sl)
		if err != nil {
			metrics.BufferFlushErrors.Inc("collect")
			return fmt.Errorf("collect %s: %w", key, err)
		}
		if len(rows) == 0 {
			return nil // every row dropped by the keep filter
		}
		up.rows = rows
	}
	up.onStored = func(string) { f.markStored(ref) }
	if err := f.writer.uploadLogGroup(ctx, up); err != nil {
		f.retry[key] = up
		metrics.BufferFlushErrors.Inc("upload")
		return fmt.Errorf("upload %s: %w", key, err)
	}
	delete(f.retry, key)
	return nil
}

// observe publishes the segment gauges.
func (f *BufferFlusher) observe(now time.Time) {
	st := f.segs.Stats(now)
	metrics.BufferSegments.Set("active", int64(st.Active))
	metrics.BufferSegments.Set("pending", int64(st.Pending))
	metrics.BufferSegments.Set("committed", int64(st.Committed))
	metrics.BufferPendingRows.Set(st.PendingRows)
	metrics.BufferOldestPendingAge.Set(int64(st.OldestPendingAge.Seconds()))
	metrics.InsertFlushCommittedSeq.Set(int64(f.state.CommittedThroughSeq))
}
