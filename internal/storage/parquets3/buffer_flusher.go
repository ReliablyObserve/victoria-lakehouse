package parquets3

import (
	"context"
	"encoding/json"
	"errors"
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
	CommitThroughAt(seq uint64, at func(seq uint64) time.Time)
	HoldCommitted(at time.Time) int
	ReleaseHeld() int
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
// Memory is bounded by maxRows per group, whatever the segment's size or the
// number of rows that share a second: the groups are planned from summaries of
// bounded size (rows per hour; per second in an hour that is over maxRows; per
// hash bucket in a second that is over maxRows), see planGroups.
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

	// What the current drain attempt's checks found, for the stalled gauge (see
	// drain). Only the flusher's goroutine touches them.
	checkRan    bool // a plan or group check compared the plan with the segment
	checkFailed bool // ... and one of them did not match

	// planQueries counts the summary queries planGroups ran (a test pins that a
	// segment whose hours fit in a group costs one per tenant).
	planQueries int

	// secondCounts, when set, replaces the hour/second/bucket summaries the plan
	// is built from (planGroups) by per-second counts cut by planSlices; a test
	// sets it to build a plan that passes the plan check and then
	// disagrees with the segment in a group. nil in production.
	secondCounts func(context.Context, *membuffer.Segment, logstorage.TenantID) (map[int64]int64, error)

	// The committed segments restored at load are held (membuffer HoldCommitted):
	// none of them is retired until this process has applied a complete S3
	// manifest refresh (ManifestRefreshed), because the manifest it started from
	// is a snapshot that can lack their objects, or what compaction made of them
	// (#379). Segments committed after the start are not held.
	createdAt     time.Time        // refreshes that started before this do not release the hold
	clock         func() time.Time // nil: time.Now; tests simulate a restart after downtime
	holdWarnAfter time.Duration    // WARN when a restored segment has been held this long
	lastHoldWarn  time.Time

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
	// Commits are the commit times of the segments committed within the last
	// two graces (the compaction guard's age): a restart gives each segment
	// the remaining grace of its real commit, not a fresh one (#379).
	Commits []commitRecord `json:"commits,omitempty"`
}

// commitRecord is when one segment was committed (wall clock, Unix ns).
type commitRecord struct {
	Seq        uint64 `json:"seq"`
	AtUnixNano int64  `json:"at_unix_nano"`
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
	// HoldWarnAfter is how long restored segments may stay held, waiting for a
	// complete manifest refresh, before a warning is logged (every HoldWarnAfter
	// after that). Zero: 10 minutes.
	HoldWarnAfter time.Duration
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
	if c.HoldWarnAfter <= 0 {
		c.HoldWarnAfter = 10 * time.Minute
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
		createdAt: time.Now(),

		holdWarnAfter: c.HoldWarnAfter,
		retry:         map[string]*logGroupUpload{},
		done:          make(chan struct{}),
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
	f.restoreCommitted(st, now)
	return nil
}

// restoreCommitted tells the segments which of them are committed, each at its
// recorded commit time, so the grace of each keeps counting from its real
// commit and not from this restart. Nothing is retired here: each restored
// segment is held, and stays served until ManifestRefreshed, because retiring it
// earlier, from a manifest snapshot that predates its objects, would hide its
// rows until the first complete S3 refresh. Only the restored segments are held;
// the ones committed after this start follow the normal grace.
//
// A committed segment with no record, next to records, is older than the
// records' retention and is treated as long committed. A state with no records
// at all was written by a release before the records existed: its segments
// get their grace from now, as that release gave them, for this one start. A
// commit time in the future (the clock went back) is taken as now.
func (f *BufferFlusher) restoreCommitted(st flushState, now time.Time) {
	at := make(map[uint64]time.Time, len(st.Commits))
	for _, c := range st.Commits {
		at[c.Seq] = time.Unix(0, c.AtUnixNano)
	}
	legacy := len(st.Commits) == 0
	restored := 0
	f.segs.CommitThroughAt(st.CommittedThroughSeq, func(seq uint64) time.Time {
		restored++
		t, ok := at[seq]
		switch {
		case legacy:
			return now
		case !ok:
			return time.Unix(0, 1)
		case t.After(now):
			return now
		}
		return t
	})
	if restored > 0 {
		f.segs.HoldCommitted(f.now())
	}
}

func (f *BufferFlusher) now() time.Time {
	if f.clock != nil {
		return f.clock()
	}
	return time.Now()
}

// ManifestRefreshed is called after every COMPLETE S3 manifest refresh has
// been applied (not rejected by the cliff guard, not partial); started is when
// that refresh began. The first one that began after this flusher was built
// releases the segments restored at load: the manifest now holds what the
// bucket holds (their objects, and anything compaction made of them while the
// process was down), so retiring them whose grace has passed loses nothing.
// Until then, if no such refresh succeeds, they stay served: duplicates are
// possible only if a peer compacted their objects and are bounded by their
// size (not in time), while retiring them could hide rows.
func (f *BufferFlusher) ManifestRefreshed(started time.Time) {
	if started.Before(f.createdAt) || f.segs.ReleaseHeld() == 0 {
		return
	}
	if n := f.segs.Reap(f.now(), f.grace); n > 0 {
		logger.Infof("buffer flusher: removed %d committed segment(s) whose grace passed while the process was down", n)
	}
}

// commitRecordsWith returns the commit records to persist after committing seq
// at now: the earlier ones younger than two graces, plus the new one.
func (f *BufferFlusher) commitRecordsWith(seq uint64, now time.Time) []commitRecord {
	out := make([]commitRecord, 0, len(f.state.Commits)+1)
	for _, c := range f.state.Commits {
		if now.Sub(time.Unix(0, c.AtUnixNano)) < 2*f.grace && c.Seq != seq {
			out = append(out, c)
		}
	}
	return append(out, commitRecord{Seq: seq, AtUnixNano: now.UnixNano()})
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
// _time in [start, end]. A bucketed group is a share of one second that holds
// more rows than a group may (see splitSecond): of the rows in [start, end], those
// whose bucket (splitBucketPipes) is in [bucketLo, bucketHi].
type flushSlice struct {
	partition  string
	slice      int
	start, end int64
	rows       int64

	bucketed           bool
	bucketLo, bucketHi int64
}

// planSlices cuts a tenant's per-second row counts into groups of at most
// maxRows rows that never cross an hour partition. A single second with more
// rows than maxRows is one group here; planGroups splits it (splitSecond).
// Deterministic for the same counts.
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
	return tenantTimeCounts(ctx, g, tenant, "1s", 0, math.MaxInt64)
}

// tenantTimeCounts returns the tenant's rows of g with _time in [from, to] per
// bucket of _time (upstream `stats by (_time:<step>) count()`), keyed by the
// bucket's start. It is the bounded summary the plan is built from: one entry
// per hour of the segment, and one per second of an hour that holds more rows
// than a group.
func tenantTimeCounts(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID, step string, from, to int64) (map[int64]int64, error) {
	var mu sync.Mutex
	out := map[int64]int64{}
	err := runCounts(ctx, g, tenant, "* | stats by (_time:"+step+") count() as n", from, to, "_time", func(key string, n int64) error {
		t, err := time.Parse(time.RFC3339Nano, key)
		if err != nil {
			return fmt.Errorf("per-%s count: _time %q: %w", step, key, err)
		}
		mu.Lock()
		out[t.UnixNano()] += n
		mu.Unlock()
		return nil
	})
	return out, err
}

// runCounts runs a `stats by (<keyCol>) count() as n` query over [from, to] and
// hands every (key, n) to add, from several goroutines.
func runCounts(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID, query string, from, to int64, keyCol string, add func(key string, n int64) error) error {
	q, err := logstorage.ParseQueryAtTimestamp(query, math.MaxInt64)
	if err != nil {
		return err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), from, to)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	var mu sync.Mutex
	var perr error
	err = g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		var ks, ns []string
		for _, c := range db.GetColumns(false) {
			switch c.Name {
			case keyCol:
				ks = c.Values
			case "n":
				ns = c.Values
			}
		}
		mu.Lock()
		defer mu.Unlock()
		for i := range ks {
			n, err := strconv.ParseInt(ns[i], 10, 64)
			if err != nil {
				perr = fmt.Errorf("count: n %q: %w", ns[i], err)
				return
			}
			if err := add(ks[i], n); err != nil {
				perr = err
				return
			}
		}
	})
	if err == nil {
		err = perr
	}
	return err
}

// The bucket of a row, for splitting a second that holds more rows than a group:
// upstream's `hash` pipe (xxhash64, cut to 53 bits so it is exact in a float) of
// the row's time, stream and message, cut into splitBuckets equal ranges. The key is
// built from named fields on purpose: a pipe that packs all of a row's fields
// follows the order of the block's columns, which changes when upstream merges
// the parts of a segment, and the plan and the collect of a group must give a
// row the same bucket. The helper fields are deleted again before the rows are
// used.
const (
	splitBuckets      = 1024
	splitBucketDiv    = (1 << 53) / splitBuckets // a power of two, so the division is exact in a float
	splitKeyFormat    = `<_time>|<_stream_id>|<_msg>`
	splitBucketDelete = `delete _lh_split_k, _lh_split_h, _lh_split_b`
)

var splitBucketPipes = fmt.Sprintf(`format "%s" as _lh_split_k | hash(_lh_split_k) as _lh_split_h | math floor(_lh_split_h / %d) as _lh_split_b`, splitKeyFormat, splitBucketDiv)

// hourEnd is the last nanosecond of the hour starting at h.
func hourEnd(h int64) int64 {
	if h > math.MaxInt64-int64(time.Hour) {
		return math.MaxInt64
	}
	return h + int64(time.Hour) - 1
}

// planGroups cuts one tenant's rows of g into groups of at most f.maxRows rows,
// from summaries whose size does not depend on the number of distinct seconds:
// the rows per hour (one query, one entry per hour of the segment); for an hour
// that holds more than maxRows rows, the rows per second of that hour (at most
// 3,600 entries); for a second that holds more than maxRows rows, the rows per
// bucket of that second (at most splitBuckets+1 entries). An hour that fits is
// one group, which is what planSlices makes of it too, so the plan of a segment
// without a crowded second is the plan planSlices made from every second.
func (f *BufferFlusher) planGroups(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID) ([]flushSlice, error) {
	f.planQueries++
	hours, err := tenantTimeCounts(ctx, g, tenant, "1h", 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	starts := make([]int64, 0, len(hours))
	for h := range hours {
		starts = append(starts, h)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	var out []flushSlice
	for _, h := range starts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if n := hours[h]; n <= f.maxRows {
			out = append(out, flushSlice{partition: partitionFromNano(h), start: h, end: hourEnd(h), rows: n})
			continue
		}
		f.planQueries++
		secs, err := tenantTimeCounts(ctx, g, tenant, "1s", h, hourEnd(h))
		if err != nil {
			return nil, err
		}
		next := 0
		for _, sl := range planSlices(secs, f.maxRows) {
			parts := []flushSlice{sl}
			if sl.rows > f.maxRows {
				f.planQueries++
				if parts, err = f.splitSecond(ctx, g, tenant, sl); err != nil {
					return nil, err
				}
			}
			for _, p := range parts {
				p.slice = next
				next++
				out = append(out, p)
			}
		}
	}
	return out, nil
}

// splitSecond cuts a second that holds more than f.maxRows rows into bucketed
// groups of at most f.maxRows rows each, of consecutive buckets. A bucket that
// alone holds more than maxRows rows (rows identical in the fields the bucket is
// made of, see splitKeyFormat) cannot be cut further by content: it is one group
// over the cap, and is logged.
func (f *BufferFlusher) splitSecond(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID, sl flushSlice) ([]flushSlice, error) {
	counts := map[int64]int64{}
	err := runCounts(ctx, g, tenant, "* | "+splitBucketPipes+" | stats by (_lh_split_b) count() as n", sl.start, sl.end, "_lh_split_b", func(key string, n int64) error {
		b, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return fmt.Errorf("bucket count: bucket %q: %w", key, err)
		}
		counts[b] += n
		return nil
	})
	if err != nil {
		return nil, err
	}
	bs := make([]int64, 0, len(counts))
	for b := range counts {
		bs = append(bs, b)
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i] < bs[j] })
	var out []flushSlice
	for _, b := range bs {
		n := counts[b]
		if k := len(out) - 1; k >= 0 && out[k].rows+n <= f.maxRows {
			out[k].rows += n
			out[k].bucketHi = b
			continue
		}
		if n > f.maxRows {
			oversizedGroupLogger.Warnf("buffer flusher: %d rows of tenant %d:%d in %s share one bucket of a crowded second (identical in the row's time, stream and message): they stay one group of %d rows, over the %d-row cap",
				n, tenant.AccountID, tenant.ProjectID, time.Unix(0, sl.start).UTC().Format(time.RFC3339), n, f.maxRows)
		}
		out = append(out, flushSlice{partition: sl.partition, start: sl.start, end: sl.end, rows: n, bucketed: true, bucketLo: b, bucketHi: b})
	}
	return out, nil
}

var oversizedGroupLogger = logger.WithThrottler("oversized_group", time.Minute)

// sliceQuery is the query that returns a group's rows.
func sliceQuery(sl flushSlice) string {
	if !sl.bucketed {
		return "*"
	}
	return fmt.Sprintf("* | %s | filter _lh_split_b:range[%d, %d] | %s", splitBucketPipes, sl.bucketLo, sl.bucketHi, splitBucketDelete)
}

// collectSlice reads one group's rows from g and applies the keep filter. It
// returns the rows to write and the number of rows the filter left out.
func (f *BufferFlusher) collectSlice(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID, sl flushSlice) (rows []schema.LogRow, filtered int, err error) {
	q, err := logstorage.ParseQueryAtTimestamp(sliceQuery(sl), sl.end)
	if err != nil {
		return nil, 0, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), sl.start, sl.end)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	// The plan knows how many rows the group has: one allocation, no doubling.
	rows = make([]schema.LogRow, 0, sl.rows)
	// logstorage calls writeBlock from several goroutines.
	var mu sync.Mutex
	var seen int
	err = g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		local := make([]schema.LogRow, 0, db.RowsCount())
		skipped := 0
		for _, r := range vlstorage.DataBlockToLogRows(db, tenant) {
			if f.keep != nil && !f.keep(r.AccountID, r.ProjectID, r.Stream) {
				skipped++
				continue
			}
			local = append(local, r)
		}
		mu.Lock()
		rows = append(rows, local...)
		filtered += skipped
		seen += db.RowsCount()
		mu.Unlock()
	})
	// Upstream's RunQuery returns nil, with partial or no rows, when the context
	// is cancelled (a Stop): that is a cancelled step, not rows that went missing.
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, 0, err
	}
	// The rows the query returned add up to the group the plan promised: a query
	// that disagrees with the plan is not written as if nothing happened. The
	// second clause is defensive: the conversion is one row to one row, so
	// rows+filtered always equals seen today (an equivalent mutant); it stays to
	// catch a conversion that ever drops a row.
	f.checkRan = true // defensive: planTenant already set it for every tenant in plans (an equivalent mutant)
	if int64(seen) != sl.rows || len(rows)+filtered != seen {
		return nil, 0, f.invariantViolation(g, fmt.Sprintf("group %s slice %d of tenant %d:%d: the plan has %d rows, the segment returned %d, %d were converted and %d filtered",
			sl.partition, sl.slice, tenant.AccountID, tenant.ProjectID, sl.rows, seen, len(rows), filtered))
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano })
	return rows, filtered, nil
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
	f.segs.Reap(f.now(), f.grace)
	f.writer.persistCatalog(ctx)
	f.observe(f.now())
}

func (f *BufferFlusher) failed(now time.Time, g *membuffer.Segment, err error) {
	metrics.InsertFlushErrorsTotal.Inc()
	switch {
	case f.backoff == 0:
		f.backoff = time.Second
	case f.backoff < 30*time.Second:
		f.backoff *= 2
	}
	f.nextTry = now.Add(f.backoff)
	logger.Warnf("buffer flusher: segment %d (%s) is not fully written yet, retrying in %s: %s", g.Seq(), g.Nonce(), f.backoff, err)
}

// maybeSeal seals the active segment once its first row is maxAge old, or it
// has reached the size target while few segments wait to be drained. The age
// counts from the first row, not from the segment's creation: a segment that
// sat empty through an idle period would otherwise be sealed, and written as a
// tiny object, the moment its first rows arrived.
func (f *BufferFlusher) maybeSeal(now time.Time) {
	a := f.segs.Active()
	rows := a.Rows()
	if rows == 0 {
		return
	}
	due := now.Sub(a.FirstRowAt()) >= f.maxAge
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

// tenantPlan is the groups one tenant's rows of a segment are cut into.
type tenantPlan struct {
	tenant logstorage.TenantID
	slices []flushSlice
	rows   int64 // the rows the slices add up to
}

// planSegment cuts every tenant's rows of g into groups. It lists the tenants
// over the whole of int64 time, not only from the Unix epoch, so that a row the
// plan cannot reach (a _time before the epoch) is found, and it returns an error
// that stalls the segment when the plan does not cover the rows the segment holds
// (errDrainInvariant). A tenant whose groups cannot be planned now is reported in
// planErr: the other tenants are still written and the drain is retried.
func (f *BufferFlusher) planSegment(ctx context.Context, g *membuffer.Segment) (plans []tenantPlan, planErr, err error) {
	return f.planSegmentWith(ctx, g, f.secondCounts, tenantRowCount)
}

// planSegmentWith is planSegment with its two per-tenant queries as parameters.
func (f *BufferFlusher) planSegmentWith(ctx context.Context, g *membuffer.Segment,
	secondCounts func(context.Context, *membuffer.Segment, logstorage.TenantID) (map[int64]int64, error),
	rowCount func(context.Context, *membuffer.Segment, logstorage.TenantID) (int64, error)) (plans []tenantPlan, planErr, err error) {
	tenants, err := g.GetTenantIDs(ctx, math.MinInt64, math.MaxInt64)
	if err != nil {
		metrics.BufferFlushErrors.Inc("collect")
		return nil, nil, fmt.Errorf("list tenants: %w", err)
	}
	// A cancelled context can make the listing come back empty without an error;
	// an empty plan is not a plan.
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].AccountID != tenants[j].AccountID {
			return tenants[i].AccountID < tenants[j].AccountID
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		tp, err := f.planTenant(ctx, g, tenant, secondCounts, rowCount)
		switch {
		case err == nil:
			plans = append(plans, tp)
		case errors.Is(err, errDrainInvariant):
			return nil, nil, err
		default:
			metrics.BufferFlushErrors.Inc("collect")
			if planErr == nil {
				planErr = err
			}
		}
	}
	return plans, planErr, nil
}

// planTenant cuts one tenant's rows of g into groups and checks the plan against
// a count of all of the tenant's rows. The two queries are parameters so the
// checks can be tested with queries that fail or disagree.
func (f *BufferFlusher) planTenant(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID,
	secondCounts func(context.Context, *membuffer.Segment, logstorage.TenantID) (map[int64]int64, error),
	rowCount func(context.Context, *membuffer.Segment, logstorage.TenantID) (int64, error)) (tenantPlan, error) {
	tp := tenantPlan{tenant: tenant}
	if secondCounts != nil {
		// A test's plan: its own per-second counts, cut by planSlices.
		counts, err := secondCounts(ctx, g, tenant)
		if err != nil {
			return tenantPlan{}, fmt.Errorf("plan tenant %d:%d: %w", tenant.AccountID, tenant.ProjectID, err)
		}
		tp.slices = planSlices(counts, f.maxRows)
	} else {
		slices, err := f.planGroups(ctx, g, tenant)
		if err != nil {
			return tenantPlan{}, fmt.Errorf("plan tenant %d:%d: %w", tenant.AccountID, tenant.ProjectID, err)
		}
		tp.slices = slices
	}
	for _, sl := range tp.slices {
		tp.rows += sl.rows
	}
	n, err := rowCount(ctx, g, tenant)
	if err == nil {
		// Checked before the comparison: a cancelled query returns what it had,
		// without an error, in the plan's query as well as in this one, and a
		// cancelled step is not a mismatch.
		err = ctx.Err()
	}
	if err != nil {
		return tenantPlan{}, fmt.Errorf("count tenant %d:%d: %w", tenant.AccountID, tenant.ProjectID, err)
	}
	f.checkRan = true
	if n != tp.rows {
		return tenantPlan{}, f.invariantViolation(g, fmt.Sprintf("tenant %d:%d: the plan covers %d rows, the segment holds %d", tenant.AccountID, tenant.ProjectID, tp.rows, n))
	}
	return tp, nil
}

// errDrainInvariant is wrapped by the error that stalls a segment whose drain
// would commit without every row it holds being planned for Parquet.
var errDrainInvariant = errors.New("drain invariant violated")

var drainInvariantLogger = logger.WithThrottler("drain_invariant", time.Minute)

// invariantViolation counts and logs a segment the drain refuses to commit and
// returns the error that stalls it. The segment stays sealed on disk, pending,
// and is planned again from scratch after the back-off; it is not committed
// or removed and gets no commit marker (a group-level mismatch can fire after
// earlier groups of the segment were uploaded and marked as stored). A mismatch that does not repeat clears on that
// retry; one that does repeat (a row the plan cannot reach, or a bug) blocks the
// segments after it, since they are drained in order, until the disk floor
// refuses inserts (429): the buffer never loses the rows silently. A cancelled
// context is never a mismatch (see collectSlice and planTenant).
func (f *BufferFlusher) invariantViolation(g *membuffer.Segment, what string) error {
	metrics.BufferFlushErrors.Inc("invariant")
	f.checkFailed = true // drain publishes the gauge when the attempt ends
	drainInvariantLogger.Errorf("buffer flusher: segment %d (%s) is stalled and will not be committed: %s; "+
		"the segment stays in insert.buffer_dir and is planned again after the back-off, so a mismatch that does not repeat clears itself; "+
		"one that repeats blocks the segments after it (lakehouse_buffer_stalled_segments, runbook: docs/durability.md#a-stalled-segment-runbook)",
		g.Seq(), g.Nonce(), what)
	return fmt.Errorf("segment %d: %w: %s", g.Seq(), errDrainInvariant, what)
}

// tenantRowCount returns the number of rows the tenant has in g over the whole
// of int64 time (upstream `stats count()`), the count the drain's plan is
// checked against: the plan itself is built from [0, MaxInt64] only.
func tenantRowCount(ctx context.Context, g *membuffer.Segment, tenant logstorage.TenantID) (int64, error) {
	q, err := logstorage.ParseQueryAtTimestamp(`* | stats count() as n`, math.MaxInt64)
	if err != nil {
		return 0, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), math.MinInt64, math.MaxInt64)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	var mu sync.Mutex
	var total int64
	var perr error
	err = g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range db.GetColumns(false) {
			if c.Name == "n" {
				for _, v := range c.Values {
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						perr = fmt.Errorf("row count: n %q: %w", v, err)
					}
					total += n
				}
			}
		}
	})
	if err == nil {
		err = perr
	}
	return total, err
}

// drain writes every group of g that is not settled yet and then commits g.
//
// It also publishes lakehouse_buffer_stalled_segments, once, when the attempt
// ends, never in the middle of it: a plan or group check that failed in this
// attempt sets 1; an attempt that completed its checks without a mismatch sets
// 0, even if an upload then failed (the cause of the stall is gone, an outage
// is another matter and has its own alerts); an attempt cut short by Stop (even
// one whose checks had found a mismatch), or that failed before any check ran,
// leaves the gauge as it was. Writing 0 in the
// middle of an attempt would flip a persistent group-level mismatch 0 -> 1 on
// every attempt, and a scrape that saw the 0 would restart the alert's 15m.
func (f *BufferFlusher) drain(ctx context.Context, g *membuffer.Segment) error {
	f.checkRan, f.checkFailed = false, false
	err := f.drainAttempt(ctx, g)
	switch {
	case err != nil && ctx.Err() != nil:
		// Stop cut the attempt short: leave the gauge as it was, whatever the
		// part of the attempt that ran found.
	case f.checkFailed:
		metrics.BufferStalledSegments.Set(1)
	case err == nil, f.checkRan:
		metrics.BufferStalledSegments.Set(0)
	}
	return err
}

func (f *BufferFlusher) drainAttempt(ctx context.Context, g *membuffer.Segment) error {
	started := time.Now()
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

	// Plan every tenant's groups before anything is uploaded, and check that the
	// plan covers every row the segment holds (see planSegment): a segment that
	// does not is stalled, with nothing of it written to the bucket. The group
	// check (collectSlice) runs later, inside writeGroup, after earlier groups
	// of the same attempt were uploaded.
	plans, planErr, err := f.planSegment(ctx, g)
	if err != nil {
		return err
	}
	// A tenant whose group fails waits for the retry from that group on; the
	// other tenants' groups are still written in this pass, so one tenant's
	// failing bucket holds back the commit but not everyone's objects.
	firstErr := planErr
	for _, tp := range plans {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, sl := range tp.slices {
			if err := f.writeGroup(ctx, g, tp.tenant, sl); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				break
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}

	// The marker tells compaction and delete rewrites on every node that the
	// segment's objects are complete; they leave them alone until the grace
	// after it has passed (manifest.SegmentGuard). Written before the local
	// commit: a segment committed here always has its marker.
	if err := f.writer.putSegmentMarker(ctx, g.Nonce(), g.Seq()); err != nil {
		metrics.BufferFlushErrors.Inc("marker")
		return fmt.Errorf("write the commit marker of segment %d: %w", g.Seq(), err)
	}
	commitAt := time.Now()
	st := flushState{CommittedThroughSeq: g.Seq(), Commits: f.commitRecordsWith(g.Seq(), commitAt)}
	if err := f.writeState(st); err != nil {
		metrics.BufferFlushErrors.Inc("commit")
		return fmt.Errorf("record segment %d as committed: %w", g.Seq(), err)
	}
	f.state = st
	_ = os.Remove(f.storedPath())
	f.marksFor, f.marks, f.head = "", nil, false
	f.segs.Commit(g, commitAt)
	metrics.BufferSegmentsCommitted.Inc()
	metrics.InsertFlushTotal.Inc()
	metrics.InsertFlushDuration.Observe(time.Since(started).Seconds())
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
		rows, _, err := f.collectSlice(ctx, g, tenant, sl)
		if err != nil {
			if !errors.Is(err, errDrainInvariant) {
				metrics.BufferFlushErrors.Inc("collect")
			}
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
	metrics.BufferSegments.Set("retired", int64(st.Retired))
	metrics.BufferPendingRows.Set(st.PendingRows)
	metrics.BufferOldestPendingAge.Set(int64(st.OldestPendingAge.Seconds()))
	metrics.BufferHeldSegments.Set(int64(st.Held))
	metrics.BufferOldestHeldAge.Set(int64(st.OldestHeldAge.Seconds()))
	if st.Held > 0 && st.OldestHeldAge >= f.holdWarnAfter && now.Sub(f.lastHoldWarn) >= f.holdWarnAfter {
		f.lastHoldWarn = now
		logger.Warnf("buffer flusher: %d committed segment(s) restored at start are still held after %s: no complete S3 manifest refresh has finished since (listing failing, rejected by the cliff guard, or slower than its timeout); they stay served, so their rows may be duplicated if a peer compacted their objects, and they keep their disk (lakehouse_buffer_held_segments)", st.Held, st.OldestHeldAge.Round(time.Second))
	}
	metrics.InsertFlushCommittedSeq.Set(int64(f.state.CommittedThroughSeq))
}
