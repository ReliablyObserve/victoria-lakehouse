package membuffer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// Segments is the insert buffer cut by INGEST time: a short sequence of
// upstream logstorage.Storage instances ("segments"), each in its own
// directory and each used exactly as upstream uses its storage.
//
// Every acknowledged row goes to the one active segment, so it is durable on
// upstream's terms (in memory, then an fsynced on-disk part within
// FlushInterval). The flusher seals the active segment by age or size: a new
// segment takes the writes from then on, and the sealed one is closed and
// reopened, which makes every row of it durable and the segment immutable. A
// sealed segment is drained completely to Parquet, committed, kept readable for
// a grace period, and removed.
//
// Because a segment holds the rows that ARRIVED in its time — whatever their
// _time — a late or backfilled row is written to Parquet with the segment it
// arrived in; nothing depends on a row's timestamp being recent.
//
// Readers take a Snapshot: the live segments, held until Release, and their
// nonces. Every object a segment produced carries its nonce in its key, so a
// query reads a live segment's rows from the segment and excludes its objects
// from the cold scan: each row is answered exactly once at every instant. A
// segment is removed only after it is committed, its grace has passed and no
// snapshot holds it.
type Segments struct {
	root string
	cfg  Config

	// add is held for reading by every MustAddRows and for writing by Seal, so
	// a sealed segment never takes another row.
	add sync.RWMutex

	mu      sync.Mutex // guards everything below
	active  *Segment
	list    []*Segment // live segments in seq order, the active one last
	nextSeq uint64
	closed  bool
}

// Segment is one ingest-time slice of the buffer.
type Segment struct {
	seq     uint64
	nonce   string
	dir     string
	created time.Time

	// stMu guards st and closed. Every use of st holds it for reading, for as
	// long as the upstream call runs, and every close of st holds it for
	// writing, so a close waits for the calls in flight and a later call sees
	// closed instead of touching a closed storage.
	stMu sync.RWMutex
	st   *logstorage.Storage
	// closed is set, under stMu, when st is closed for good (Segments.Close or
	// Reap); not by the close/reopen at seal time.
	closed bool

	rows atomic.Int64 // rows added (sizing; reopened segments start at 0)

	// guarded by Segments.mu
	sealed    bool
	committed time.Time // zero until committed
	refs      int
}

// Seq is the segment's sequence number; segments are drained in seq order.
func (g *Segment) Seq() uint64 { return g.seq }

// Nonce is the random name every object of the segment carries in its key.
func (g *Segment) Nonce() string { return g.nonce }

// Created is when the segment was created; for a segment found at startup it
// is the time its nonce carries, so its pending age survives a restart.
func (g *Segment) Created() time.Time { return g.created }

// Rows is the number of rows added to the segment by this process.
func (g *Segment) Rows() int64 { return g.rows.Load() }

// errSegmentClosed is returned for a query on a segment whose storage was
// closed for good (the buffer was closed, or the segment was reaped).
var errSegmentClosed = errors.New("insert buffer is closed")

// closeForGood closes the segment's storage once. It waits for the queries and
// writes in flight; later calls on the segment see closed.
func (g *Segment) closeForGood() {
	g.stMu.Lock()
	defer g.stMu.Unlock()
	if g.closed {
		return
	}
	g.closed = true
	g.st.MustClose()
}

// RunQuery runs q over the segment's rows with the upstream engine. It returns
// an error wrapping errSegmentClosed once the segment's storage is closed.
func (g *Segment) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	g.stMu.RLock()
	defer g.stMu.RUnlock()
	if g.closed {
		return fmt.Errorf("segment %s: %w", g.nonce, errSegmentClosed)
	}
	return g.st.RunQuery(qctx, writeBlock)
}

// GetTenantIDs returns the tenants with rows in [start, end].
func (g *Segment) GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	g.stMu.RLock()
	defer g.stMu.RUnlock()
	if g.closed {
		return nil, fmt.Errorf("segment %s: %w", g.nonce, errSegmentClosed)
	}
	return g.st.GetTenantIDs(ctx, start, end)
}

// segDirRe names a segment directory: seg-<seq, 16 hex>-<nonce, 16 hex>.
var segDirRe = regexp.MustCompile(`^seg-([0-9a-f]{16})-([0-9a-f]{16})$`)

// NonceLen is the length of a segment nonce in hex characters.
const NonceLen = 16

func (s *Segments) storageConfig() *logstorage.StorageConfig {
	return &logstorage.StorageConfig{
		Retention:             s.cfg.Retention,
		FlushInterval:         s.cfg.FlushInterval,
		FutureRetention:       s.cfg.FutureRetention,
		MaxBackfillAge:        0, // accept any age within Retention, like upstream with no -maxBackfillAge
		MinFreeDiskSpaceBytes: s.cfg.MinFreeDiskBytes,
	}
}

// newNonce names a new segment: 8 hex characters of the creation time in Unix
// seconds, then 8 random ones (see manifest.SegmentNonceOfKey and
// manifest.SegmentNonceTime, which read it back from object keys).
func newNonce() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("membuffer: cannot draw a segment nonce: %w", err))
	}
	return fmt.Sprintf("%08x", uint32(time.Now().Unix())) + hex.EncodeToString(b)
}

// nonceTime is the creation time a nonce carries, or now if it carries none.
func nonceTime(nonce string) time.Time {
	if len(nonce) < 8 {
		return time.Now()
	}
	sec, err := strconv.ParseUint(nonce[:8], 16, 32)
	if err != nil {
		return time.Now()
	}
	return time.Unix(int64(sec), 0)
}

// OpenSegments opens the buffer at cfg.Path: every segment directory found is
// opened (upstream restores its parts) and is sealed — it takes no new rows —
// and a new active segment is created for the writes. Which of the found
// segments are already committed is the flusher's record (CommitThrough).
//
// A directory left by an earlier, single-store buffer (upstream parts directly
// under cfg.Path) is moved aside, not read: its rows were written to Parquet by
// that release's own flush path.
func OpenSegments(cfg Config) (*Segments, error) {
	cfg.withDefaults()
	if cfg.Path == "" {
		return nil, fmt.Errorf("membuffer: empty Path")
	}
	if err := os.MkdirAll(cfg.Path, 0o750); err != nil {
		return nil, fmt.Errorf("membuffer: mkdir %q: %w", cfg.Path, err)
	}
	s := &Segments{root: cfg.Path, cfg: cfg, nextSeq: 1}
	if err := s.moveAsideSingleStore(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("membuffer: read %q: %w", cfg.Path, err)
	}
	for _, e := range entries {
		m := segDirRe.FindStringSubmatch(e.Name())
		if m == nil || !e.IsDir() {
			continue
		}
		seq, err := strconv.ParseUint(m[1], 16, 64)
		if err != nil {
			continue
		}
		dir := filepath.Join(cfg.Path, e.Name())
		g := &Segment{seq: seq, nonce: m[2], dir: dir, created: nonceTime(m[2]), sealed: true}
		g.st = logstorage.MustOpenStorage(dir, s.storageConfig())
		s.list = append(s.list, g)
		if seq >= s.nextSeq {
			s.nextSeq = seq + 1
		}
	}
	sort.Slice(s.list, func(i, j int) bool { return s.list[i].seq < s.list[j].seq })
	s.active = s.newSegment()
	s.list = append(s.list, s.active)
	return s, nil
}

// moveAsideSingleStore renames the parts of a pre-segment buffer (a
// logstorage directory at the root) into legacy-<unix-seconds>/ under the root.
func (s *Segments) moveAsideSingleStore() error {
	var found []string
	for _, name := range []string{"partitions", "flock.lock", "buffer_flush_watermark.json", "buffer_flush_watermark.json.prev", "buffer_flush_watermark.json.stored"} {
		if _, err := os.Stat(filepath.Join(s.root, name)); err == nil {
			found = append(found, name)
		}
	}
	if len(found) == 0 {
		return nil
	}
	dst := filepath.Join(s.root, fmt.Sprintf("legacy-%d", time.Now().Unix()))
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return fmt.Errorf("membuffer: mkdir %q: %w", dst, err)
	}
	for _, name := range found {
		if err := os.Rename(filepath.Join(s.root, name), filepath.Join(dst, name)); err != nil {
			return fmt.Errorf("membuffer: move %q aside: %w", name, err)
		}
	}
	logger.Warnf("membuffer: %s held a buffer of an earlier release (%v); moved it to %s. It is not read or flushed: delete it when it is no longer needed", s.root, found, dst)
	return nil
}

// newSegment creates and opens the next segment. Callers hold s.mu or own s.
func (s *Segments) newSegment() *Segment {
	g := &Segment{seq: s.nextSeq, nonce: newNonce(), created: time.Now()}
	s.nextSeq++
	g.dir = filepath.Join(s.root, fmt.Sprintf("seg-%016x-%s", g.seq, g.nonce))
	g.st = logstorage.MustOpenStorage(g.dir, s.storageConfig())
	return g
}

// MustAddRows adds lr to the active segment (upstream MustAddRows).
func (s *Segments) MustAddRows(lr *logstorage.LogRows) {
	s.add.RLock()
	g := s.active
	g.stMu.RLock()
	if g.closed {
		// Unreachable in production: the insert handlers stop before the
		// buffer is closed. Never touch a closed storage; drop and say so.
		g.stMu.RUnlock()
		s.add.RUnlock()
		logger.Errorf("membuffer: %d rows added after the buffer was closed; dropped", lr.RowsCount())
		return
	}
	g.st.MustAddRows(lr)
	g.stMu.RUnlock()
	g.rows.Add(int64(lr.RowsCount()))
	s.add.RUnlock()
}

// IsReadOnly reports whether the buffer's volume is below its free-space floor
// (upstream Storage.IsReadOnly on the active segment; all segments share the
// volume). Inserts are refused while it is, as upstream refuses them. A closed
// buffer is read-only too: it takes no rows.
func (s *Segments) IsReadOnly() bool {
	s.add.RLock()
	defer s.add.RUnlock()
	g := s.active
	g.stMu.RLock()
	defer g.stMu.RUnlock()
	if g.closed {
		return true
	}
	return g.st.IsReadOnly()
}

// Active returns the active segment.
func (s *Segments) Active() *Segment {
	s.add.RLock()
	defer s.add.RUnlock()
	return s.active
}

// testHookSealBeforeReopen, when a test sets it, runs in Seal after the
// active segment was swapped out and before it is closed and reopened.
var testHookSealBeforeReopen func()

// Seal hands the writes to a new segment and makes the active one immutable
// and fully durable: it is closed (upstream flushes every in-memory part to
// disk) and reopened read-only in practice, since nothing adds to it any more.
// An empty active segment is not sealed; ok is false then.
func (s *Segments) Seal() (sealed *Segment, ok bool) {
	s.mu.Lock()
	if s.closed || s.active.rows.Load() == 0 {
		s.mu.Unlock()
		return nil, false
	}
	next := s.newSegment()
	s.add.Lock()
	old := s.active
	s.active = next
	s.add.Unlock()
	old.sealed = true
	s.list = append(s.list, next)
	s.mu.Unlock()
	if testHookSealBeforeReopen != nil {
		testHookSealBeforeReopen()
	}

	old.stMu.Lock()
	defer old.stMu.Unlock()
	if old.closed {
		// Close won the race for this segment: nothing to reopen.
		return nil, false
	}
	old.st.MustClose()
	old.st = logstorage.MustOpenStorage(old.dir, s.storageConfig())
	return old, true
}

// Pending returns the sealed segments that are not committed, oldest first.
func (s *Segments) Pending() []*Segment {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Segment
	for _, g := range s.list {
		if g.sealed && g.committed.IsZero() {
			out = append(out, g)
		}
	}
	return out
}

// Commit records that every object of g is stored and in the manifest. g stays
// readable until Reap removes it after the grace period.
func (s *Segments) Commit(g *Segment, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g.committed.IsZero() {
		g.committed = at
	}
}

// CommitThrough marks every sealed segment with seq <= seq as committed at at:
// what a restart learns from the flusher's record.
func (s *Segments) CommitThrough(seq uint64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.list {
		if g.sealed && g.seq <= seq && g.committed.IsZero() {
			g.committed = at
		}
	}
}

// Reap removes the committed segments whose grace has passed and that no
// snapshot holds: upstream MustClose, then the directory. It returns how many
// it removed.
func (s *Segments) Reap(now time.Time, grace time.Duration) int {
	s.mu.Lock()
	var gone []*Segment
	keep := s.list[:0]
	for _, g := range s.list {
		if !g.committed.IsZero() && g.refs == 0 && now.Sub(g.committed) >= grace {
			gone = append(gone, g)
			continue
		}
		keep = append(keep, g)
	}
	s.list = keep
	s.mu.Unlock()
	for _, g := range gone {
		g.closeForGood()
		if err := os.RemoveAll(g.dir); err != nil {
			logger.Errorf("membuffer: cannot remove committed segment %s: %s", g.dir, err)
		}
	}
	return len(gone)
}

// DebugFlush makes the rows added so far searchable at once (upstream
// DebugFlush on every segment); upstream does it within about a second on its
// own. Tests use it instead of sleeping.
func (s *Segments) DebugFlush() {
	s.mu.Lock()
	list := append([]*Segment(nil), s.list...)
	s.mu.Unlock()
	for _, g := range list {
		g.stMu.RLock()
		if !g.closed {
			g.st.DebugFlush()
		}
		g.stMu.RUnlock()
	}
}

// Stats is a point-in-time view of the segments for metrics.
type Stats struct {
	Active, Pending, Committed int
	PendingRows                int64
	OldestPendingAge           time.Duration
}

// Stats counts the segments by state.
func (s *Segments) Stats(now time.Time) Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st Stats
	for _, g := range s.list {
		switch {
		case !g.sealed:
			st.Active++
		case g.committed.IsZero():
			st.Pending++
			st.PendingRows += g.rows.Load()
			if age := now.Sub(g.created); age > st.OldestPendingAge {
				st.OldestPendingAge = age
			}
		default:
			st.Committed++
		}
	}
	return st
}

// Snapshot holds the live segments for one query: they are not removed until
// Release.
type Snapshot struct {
	s    *Segments
	segs []*Segment
}

// Snapshot returns the segments live now, held until Release.
func (s *Segments) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := &Snapshot{s: s, segs: make([]*Segment, len(s.list))}
	copy(p.segs, s.list)
	for _, g := range p.segs {
		g.refs++
	}
	return p
}

// Release lets the snapshot's segments be removed again.
func (p *Snapshot) Release() {
	if p == nil || p.s == nil {
		return
	}
	p.s.mu.Lock()
	for _, g := range p.segs {
		g.refs--
	}
	p.s.mu.Unlock()
	p.s = nil
}

// Nonces returns the nonces of the snapshot's segments. A cold object whose
// key carries one of them holds rows the snapshot serves itself.
func (p *Snapshot) Nonces() map[string]struct{} {
	out := make(map[string]struct{}, len(p.segs))
	for _, g := range p.segs {
		if g.nonce != "" {
			out[g.nonce] = struct{}{}
		}
	}
	return out
}

// RunQuery runs q over every segment of the snapshot in turn.
func (p *Snapshot) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	for _, g := range p.segs {
		if err := g.RunQuery(qctx, writeBlock); err != nil {
			return err
		}
	}
	return nil
}

// GetTenantIDs returns the tenants with rows in [start, end] in any segment of
// the snapshot, sorted.
func (p *Snapshot) GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	seen := map[logstorage.TenantID]struct{}{}
	for _, g := range p.segs {
		ids, err := g.GetTenantIDs(ctx, start, end)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}
	out := make([]logstorage.TenantID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	return out, nil
}

// Close closes every segment (upstream MustClose writes the in-memory parts to
// disk), so a clean restart reopens them all.
func (s *Segments) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	list := append([]*Segment(nil), s.list...)
	s.mu.Unlock()
	s.add.Lock()
	defer s.add.Unlock()
	for _, g := range list {
		g.closeForGood()
	}
}

// Path is the buffer's root directory.
func (s *Segments) Path() string { return s.root }
