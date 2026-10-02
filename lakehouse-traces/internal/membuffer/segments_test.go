package membuffer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var nonceRe = regexp.MustCompile(`^[0-9a-f]{16}$`)

func openSegs(t *testing.T, dir string) *Segments {
	t.Helper()
	s, err := OpenSegments(Config{Path: dir, Retention: 24 * time.Hour})
	if err != nil {
		t.Fatalf("OpenSegments: %v", err)
	}
	return s
}

// addRows adds n rows for tenant tid with the given message prefix.
func addRows(s *Segments, tid logstorage.TenantID, prefix string, n int) {
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	now := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		lr.MustAdd(tid, now+int64(i), []logstorage.Field{
			{Name: "service.name", Value: "svc"},
			{Name: "_msg", Value: fmt.Sprintf("%s-%d", prefix, i)},
		}, 1)
	}
	s.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

func countSnap(t *testing.T, p *Snapshot, tids ...logstorage.TenantID) int64 {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp("*", time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	var rows atomic.Int64
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, tids, q, false, nil)
	if err := p.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) { rows.Add(int64(db.RowsCount())) }); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	return rows.Load()
}

func segDirs(t *testing.T, root string) []string {
	t.Helper()
	ents, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "seg-") {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestSegments_OpenEmptyDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "buf") // must be created
	s := openSegs(t, dir)
	defer s.Close()

	if s.Path() != dir {
		t.Fatalf("Path = %q, want %q", s.Path(), dir)
	}
	a := s.Active()
	if a == nil || a.Seq() != 1 {
		t.Fatalf("active = %+v, want seq 1", a)
	}
	if got := s.Pending(); len(got) != 0 {
		t.Fatalf("pending on empty open = %d, want 0", len(got))
	}
	if st := s.Stats(time.Now()); st != (Stats{Active: 1}) {
		t.Fatalf("stats = %+v, want only one active", st)
	}
	if s.IsReadOnly() {
		t.Fatal("fresh buffer must not be read-only")
	}
	if a.Rows() != 0 || a.Created().IsZero() {
		t.Fatalf("active rows=%d created=%v", a.Rows(), a.Created())
	}
	if d := segDirs(t, dir); len(d) != 1 || d[0] != fmt.Sprintf("seg-%016x-%s", a.Seq(), a.Nonce()) {
		t.Fatalf("segment dirs = %v", d)
	}
}

func TestSegments_OpenEmptyPath(t *testing.T) {
	if _, err := OpenSegments(Config{}); err == nil {
		t.Fatal("empty Path must be an error")
	}
}

func TestSegments_OpenUnusablePath(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A path below a regular file cannot be created.
	if _, err := OpenSegments(Config{Path: filepath.Join(f, "sub")}); err == nil {
		t.Fatal("mkdir below a file must fail")
	}
}

func TestSegments_NonceFormat(t *testing.T) {
	before := time.Now().Unix()
	s := openSegs(t, t.TempDir())
	defer s.Close()
	after := time.Now().Unix()

	n := s.Active().Nonce()
	if len(n) != NonceLen || !nonceRe.MatchString(n) {
		t.Fatalf("nonce %q is not %d lowercase hex", n, NonceLen)
	}
	ts, err := strconv.ParseUint(n[:8], 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	if int64(ts) < before || int64(ts) > after {
		t.Fatalf("nonce time %d outside [%d,%d]", ts, before, after)
	}
	// Two nonces drawn back to back share the time prefix at most and differ in the random half.
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		x := newNonce()
		if !nonceRe.MatchString(x) || seen[x] {
			t.Fatalf("bad or repeated nonce %q", x)
		}
		seen[x] = true
	}
}

func TestSegments_AddQuerySnapshot(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	t1 := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	t2 := logstorage.TenantID{AccountID: 0, ProjectID: 9}
	t3 := logstorage.TenantID{AccountID: 1, ProjectID: 1}

	addRows(s, t1, "a", 5)
	addRows(s, t2, "b", 3)
	addRows(s, t3, "c", 1)
	if got := s.Active().Rows(); got != 9 {
		t.Fatalf("active rows = %d, want 9", got)
	}
	s.DebugFlush()

	p := s.Snapshot()
	defer p.Release()
	if got := countSnap(t, p, t1); got != 5 {
		t.Fatalf("tenant1 rows = %d, want 5", got)
	}
	if got := countSnap(t, p, t1, t2, t3); got != 9 {
		t.Fatalf("all rows = %d, want 9", got)
	}
	now := time.Now().UnixNano()
	ids, err := p.GetTenantIDs(context.Background(), now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	want := []logstorage.TenantID{t2, t3, t1} // sorted by account then project
	if len(ids) != len(want) {
		t.Fatalf("tenants = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("tenants = %v, want %v", ids, want)
		}
	}
	// Outside the data's time range there are none.
	ids, err = p.GetTenantIDs(context.Background(), 0, 1000)
	if err != nil || len(ids) != 0 {
		t.Fatalf("out-of-range tenants = %v, %v", ids, err)
	}
}

func TestSegments_SealRotates(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	tid := logstorage.TenantID{}

	if _, ok := s.Seal(); ok {
		t.Fatal("empty active segment must not be sealed")
	}

	first := s.Active()
	addRows(s, tid, "one", 4)
	sealed, ok := s.Seal()
	if !ok || sealed != first {
		t.Fatalf("Seal = %v,%v, want the first segment", sealed, ok)
	}
	if s.Active() == first || s.Active().Seq() != first.Seq()+1 {
		t.Fatalf("active did not rotate: %d -> %d", first.Seq(), s.Active().Seq())
	}
	if s.Active().Nonce() == first.Nonce() {
		t.Fatal("new segment must have a new nonce")
	}
	pend := s.Pending()
	if len(pend) != 1 || pend[0] != first {
		t.Fatalf("pending = %v, want only the sealed segment", pend)
	}

	// New writes land in the new active segment; the sealed one is immutable.
	addRows(s, tid, "two", 2)
	if first.Rows() != 4 || s.Active().Rows() != 2 {
		t.Fatalf("rows sealed=%d active=%d, want 4 and 2", first.Rows(), s.Active().Rows())
	}
	s.DebugFlush()

	// Sealed segment is durable and readable alone.
	q, _ := logstorage.ParseQueryAtTimestamp("*", time.Now().UnixNano())
	var n atomic.Int64
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{tid}, q, false, nil)
	if err := first.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) { n.Add(int64(db.RowsCount())) }); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 4 {
		t.Fatalf("sealed segment holds %d rows, want 4", n.Load())
	}
	ids, err := first.GetTenantIDs(context.Background(), 0, time.Now().UnixNano()+int64(time.Hour))
	if err != nil || len(ids) != 1 || ids[0] != tid {
		t.Fatalf("sealed tenants = %v, %v", ids, err)
	}

	// A snapshot sees both segments' rows exactly once.
	p := s.Snapshot()
	defer p.Release()
	if got := countSnap(t, p, tid); got != 6 {
		t.Fatalf("snapshot rows = %d, want 6", got)
	}
	nonces := p.Nonces()
	if len(nonces) != 2 {
		t.Fatalf("nonces = %v", nonces)
	}
	if _, ok := nonces[first.Nonce()]; !ok {
		t.Fatal("snapshot nonces miss the sealed segment")
	}
	if _, ok := nonces[s.Active().Nonce()]; !ok {
		t.Fatal("snapshot nonces miss the active segment")
	}
	for n := range nonces {
		if !nonceRe.MatchString(n) {
			t.Fatalf("bad nonce %q", n)
		}
	}

	// Sealing with new rows works again and keeps seq order.
	second, ok := s.Seal()
	if !ok || second.Seq() != first.Seq()+1 {
		t.Fatalf("second seal = %v,%v", second, ok)
	}
	pend = s.Pending()
	if len(pend) != 2 || pend[0].Seq() >= pend[1].Seq() {
		t.Fatalf("pending order wrong: %v", pend)
	}
}

func TestSegments_CommitAndStats(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	tid := logstorage.TenantID{}

	addRows(s, tid, "a", 3)
	g1, _ := s.Seal()
	addRows(s, tid, "b", 2)
	g2, _ := s.Seal()
	addRows(s, tid, "c", 1)

	now := time.Now().Add(time.Hour)
	st := s.Stats(now)
	if st.Active != 1 || st.Pending != 2 || st.Committed != 0 || st.PendingRows != 5 {
		t.Fatalf("stats = %+v", st)
	}
	if st.OldestPendingAge < time.Hour || st.OldestPendingAge > 2*time.Hour {
		t.Fatalf("oldest pending age = %v", st.OldestPendingAge)
	}

	at := time.Now()
	s.Commit(g1, at)
	if p := s.Pending(); len(p) != 1 || p[0] != g2 {
		t.Fatalf("pending after commit = %v", p)
	}
	st = s.Stats(now)
	if st.Active != 1 || st.Pending != 1 || st.Committed != 1 || st.PendingRows != 2 {
		t.Fatalf("stats after commit = %+v", st)
	}

	// Commit is idempotent: the first commit time stands.
	s.Commit(g1, at.Add(time.Hour))
	if !g1.committed.Equal(at) {
		t.Fatalf("recommit moved the commit time to %v", g1.committed)
	}
}

func TestSegments_CommitThrough(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	tid := logstorage.TenantID{}
	var segs []*Segment
	for i := 0; i < 3; i++ {
		addRows(s, tid, "x", 1)
		g, ok := s.Seal()
		if !ok {
			t.Fatal("seal failed")
		}
		segs = append(segs, g)
	}
	active := s.Active()

	at := time.Now()
	s.CommitThrough(segs[1].Seq(), at) // inclusive
	for i, g := range segs {
		want := i <= 1
		if got := !g.committed.IsZero(); got != want {
			t.Fatalf("segment %d committed=%v, want %v", i, got, want)
		}
	}
	if p := s.Pending(); len(p) != 1 || p[0] != segs[2] {
		t.Fatalf("pending = %v, want only the last sealed", p)
	}
	// The active segment is never committed, whatever the seq.
	s.CommitThrough(active.Seq()+100, at.Add(time.Minute))
	if !active.committed.IsZero() {
		t.Fatal("active segment must not be committed")
	}
	// Already committed segments keep their time.
	if !segs[0].committed.Equal(at) {
		t.Fatalf("commit time overwritten: %v", segs[0].committed)
	}
	if p := s.Pending(); len(p) != 0 {
		t.Fatalf("pending = %v, want none", p)
	}
	// seq 0 commits nothing.
	s2 := openSegs(t, t.TempDir())
	defer s2.Close()
	addRows(s2, tid, "x", 1)
	s2.Seal()
	s2.CommitThrough(0, at)
	if len(s2.Pending()) != 1 {
		t.Fatal("CommitThrough(0) must commit nothing")
	}
}

func TestSegments_ReapGraceAndSnapshot(t *testing.T) {
	root := t.TempDir()
	s := openSegs(t, root)
	defer s.Close()
	tid := logstorage.TenantID{}

	addRows(s, tid, "a", 2)
	g, _ := s.Seal()
	grace := time.Minute
	at := time.Now()

	// Not committed: never reaped, however late.
	if n := s.Reap(at.Add(24*time.Hour), grace); n != 0 {
		t.Fatalf("reaped uncommitted segment (%d)", n)
	}
	s.Commit(g, at)

	// Within grace: kept.
	if n := s.Reap(at.Add(grace-time.Second), grace); n != 0 {
		t.Fatalf("reaped inside grace (%d)", n)
	}
	// A snapshot taken now pins the segment past its grace.
	p := s.Snapshot()
	if n := s.Reap(at.Add(time.Hour), grace); n != 0 {
		t.Fatalf("reaped a segment a snapshot holds (%d)", n)
	}
	if _, err := os.Stat(g.dir); err != nil {
		t.Fatalf("held segment dir gone: %v", err)
	}
	if got := countSnap(t, p, tid); got != 2 {
		t.Fatalf("held snapshot rows = %d, want 2", got)
	}
	p.Release()
	p.Release() // idempotent: must not underflow the refcount
	if g.refs != 0 {
		t.Fatalf("refs = %d after double release", g.refs)
	}

	// Exactly at grace is enough (>=); the active segment is untouched.
	if n := s.Reap(at.Add(grace), grace); n != 1 {
		t.Fatalf("reaped %d at grace boundary, want 1", n)
	}
	if _, err := os.Stat(g.dir); !os.IsNotExist(err) {
		t.Fatalf("reaped segment dir still there: %v", err)
	}
	st := s.Stats(time.Now())
	if st.Active != 1 || st.Pending != 0 || st.Committed != 0 {
		t.Fatalf("stats after reap = %+v", st)
	}
	if n := s.Reap(at.Add(time.Hour), grace); n != 0 {
		t.Fatalf("second reap removed %d", n)
	}
	// New snapshots no longer include the reaped nonce.
	p2 := s.Snapshot()
	defer p2.Release()
	if _, ok := p2.Nonces()[g.Nonce()]; ok {
		t.Fatal("reaped nonce still in snapshots")
	}
	if d := segDirs(t, root); len(d) != 1 {
		t.Fatalf("segment dirs after reap = %v", d)
	}
}

func TestSegments_ReapZeroGraceAndMultiple(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	tid := logstorage.TenantID{}
	at := time.Now()
	for i := 0; i < 3; i++ {
		addRows(s, tid, "x", 1)
		g, _ := s.Seal()
		if i < 2 {
			s.Commit(g, at) // the third stays pending
		}
	}
	if n := s.Reap(at, 0); n != 2 {
		t.Fatalf("reaped %d, want the 2 committed", n)
	}
	if len(s.Pending()) != 1 {
		t.Fatal("pending segment must survive reap")
	}
}

func TestSegments_ReopenRestoresInSeqOrder(t *testing.T) {
	root := t.TempDir()
	s := openSegs(t, root)
	tid := logstorage.TenantID{AccountID: 3}
	var nonces []string
	for i := 0; i < 3; i++ {
		addRows(s, tid, fmt.Sprintf("r%d", i), i+1)
		g, _ := s.Seal()
		nonces = append(nonces, g.Nonce())
	}
	addRows(s, tid, "tail", 4) // in the active segment at close
	activeNonce := s.Active().Nonce()
	s.Close()

	s2 := openSegs(t, root)
	defer s2.Close()
	pend := s2.Pending()
	if len(pend) != 4 {
		t.Fatalf("pending after reopen = %d, want 3 sealed + the old active", len(pend))
	}
	for i := 1; i < len(pend); i++ {
		if pend[i-1].Seq() >= pend[i].Seq() {
			t.Fatalf("pending not in seq order: %d then %d", pend[i-1].Seq(), pend[i].Seq())
		}
	}
	for i, n := range nonces {
		if pend[i].Nonce() != n {
			t.Fatalf("segment %d nonce %q, want %q", i, pend[i].Nonce(), n)
		}
	}
	if pend[3].Nonce() != activeNonce {
		t.Fatalf("old active nonce %q, want %q", pend[3].Nonce(), activeNonce)
	}
	// Fresh active segment continues the sequence and is empty.
	a := s2.Active()
	if a.Seq() != pend[3].Seq()+1 || a.Rows() != 0 {
		t.Fatalf("new active seq=%d rows=%d", a.Seq(), a.Rows())
	}
	// The data came back.
	s2.DebugFlush()
	p := s2.Snapshot()
	defer p.Release()
	if got := countSnap(t, p, tid); got != 1+2+3+4 {
		t.Fatalf("restored rows = %d, want 10", got)
	}
	st := s2.Stats(time.Now())
	if st.Active != 1 || st.Pending != 4 {
		t.Fatalf("stats = %+v", st)
	}
	// The flusher's record commits what it already stored.
	s2.CommitThrough(pend[2].Seq(), time.Now())
	if p := s2.Pending(); len(p) != 1 || p[0].Nonce() != activeNonce {
		t.Fatalf("pending after CommitThrough = %v", p)
	}
}

func TestSegments_ReopenIgnoresForeignEntries(t *testing.T) {
	root := t.TempDir()
	s := openSegs(t, root)
	s.Close()
	// None of these are segment directories.
	for _, d := range []string{"seg-xyz", "seg-0000000000000009-short", "seg-000000000000000G-0123456789abcdef", "other"} {
		if err := os.Mkdir(filepath.Join(root, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	// A regular file with a valid segment name is skipped too.
	if err := os.WriteFile(filepath.Join(root, "seg-00000000000000ff-0123456789abcdef"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2 := openSegs(t, root)
	defer s2.Close()
	if got := len(s2.Pending()); got != 1 {
		t.Fatalf("pending = %d, want only the one real segment", got)
	}
	if s2.Active().Seq() != 2 {
		t.Fatalf("active seq = %d, junk names must not advance the sequence", s2.Active().Seq())
	}
}

func TestSegments_MovesAsideSingleStore(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"partitions"} {
		if err := os.MkdirAll(filepath.Join(root, d, "20260101"), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"flock.lock", "buffer_flush_watermark.json", "buffer_flush_watermark.json.prev", "buffer_flush_watermark.json.stored"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte(f), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "unrelated.txt"), []byte("u"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := time.Now().Unix()
	s := openSegs(t, root)
	defer s.Close()
	after := time.Now().Unix()

	var legacy string
	ents, _ := os.ReadDir(root)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "legacy-") {
			legacy = e.Name()
		}
	}
	if legacy == "" {
		t.Fatal("no legacy-<unix> directory")
	}
	ts, err := strconv.ParseInt(strings.TrimPrefix(legacy, "legacy-"), 10, 64)
	if err != nil || ts < before || ts > after {
		t.Fatalf("legacy dir %q: time not in [%d,%d] (%v)", legacy, before, after, err)
	}
	for _, f := range []string{"partitions/20260101", "flock.lock", "buffer_flush_watermark.json", "buffer_flush_watermark.json.prev", "buffer_flush_watermark.json.stored"} {
		if _, err := os.Stat(filepath.Join(root, legacy, f)); err != nil {
			t.Fatalf("%s not moved aside: %v", f, err)
		}
	}
	for _, f := range []string{"partitions", "flock.lock", "buffer_flush_watermark.json"} {
		if _, err := os.Stat(filepath.Join(root, f)); !os.IsNotExist(err) {
			t.Fatalf("%s still at the root: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "unrelated.txt")); err != nil {
		t.Fatalf("unrelated file must stay: %v", err)
	}
	// Legacy content is not read as a segment, and the buffer is empty.
	if len(s.Pending()) != 0 {
		t.Fatal("legacy store must not appear as pending segments")
	}
	// A second open must not move a legacy dir again nor read it.
	s.Close()
	s2 := openSegs(t, root)
	defer s2.Close()
	if len(s2.Pending()) != 1 { // only the previous (empty) active segment
		t.Fatalf("pending = %d", len(s2.Pending()))
	}
}

func TestSegments_MoveAsideFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "flock.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o500); err != nil { // cannot create legacy-<unix>
		t.Fatal(err)
	}
	defer os.Chmod(root, 0o750) //nolint:errcheck
	if _, err := OpenSegments(Config{Path: root}); err == nil {
		t.Fatal("expected an error when the legacy dir cannot be created")
	}
}

func TestSegments_ReadDirFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o300); err != nil { // writable, not listable
		t.Fatal(err)
	}
	defer os.Chmod(root, 0o750) //nolint:errcheck
	if _, err := OpenSegments(Config{Path: root}); err == nil {
		t.Fatal("expected an error for an unreadable buffer directory")
	}
}

func TestSegments_CloseIdempotentAndBlocksSeal(t *testing.T) {
	s := openSegs(t, t.TempDir())
	addRows(s, logstorage.TenantID{}, "x", 2)
	s.Close()
	s.Close() // second close is a no-op
	if _, ok := s.Seal(); ok {
		t.Fatal("a closed buffer must not seal")
	}
}

func TestSegments_IsReadOnlyBelowFreeSpaceFloor(t *testing.T) {
	// An unreachable free-space floor puts the volume below it.
	s, err := OpenSegments(Config{Path: t.TempDir(), MinFreeDiskBytes: 1 << 62})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.IsReadOnly() {
		t.Fatal("buffer must be read-only below its free-space floor")
	}
}

func TestSnapshot_ReleaseNilAndRefcount(t *testing.T) {
	var nilSnap *Snapshot
	nilSnap.Release() // must not panic

	s := openSegs(t, t.TempDir())
	defer s.Close()
	p1, p2 := s.Snapshot(), s.Snapshot()
	g := s.Active()
	if g.refs != 2 {
		t.Fatalf("refs = %d, want 2", g.refs)
	}
	p1.Release()
	if g.refs != 1 {
		t.Fatalf("refs = %d, want 1", g.refs)
	}
	p2.Release()
	if g.refs != 0 {
		t.Fatalf("refs = %d, want 0", g.refs)
	}
}

func TestSegments_ConcurrentAddAndSeal(t *testing.T) {
	s := openSegs(t, t.TempDir())
	defer s.Close()
	tid := logstorage.TenantID{}
	const writers, per = 4, 50
	done := make(chan struct{})
	for w := 0; w < writers; w++ {
		go func() {
			for i := 0; i < per; i++ {
				addRows(s, tid, "c", 1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 5; i++ {
		s.Seal()
		time.Sleep(time.Millisecond)
	}
	for w := 0; w < writers; w++ {
		<-done
	}
	s.DebugFlush()
	p := s.Snapshot()
	defer p.Release()
	// Every acknowledged row is in exactly one segment.
	if got := countSnap(t, p, tid); got != writers*per {
		t.Fatalf("rows across segments = %d, want %d", got, writers*per)
	}
	var sum int64
	for _, g := range p.segs {
		sum += g.Rows()
	}
	if sum != writers*per {
		t.Fatalf("segment row counters sum %d, want %d", sum, writers*per)
	}
}

// A segment found at startup keeps the creation time its nonce carries, so the
// pending-age alert does not restart from zero on every restart.
func TestSegments_ReopenKeepsCreationTimeFromNonce(t *testing.T) {
	root := t.TempDir()
	s := openSegs(t, root)
	tid := logstorage.TenantID{AccountID: 4}
	addRows(s, tid, "old", 3)
	g, _ := s.Seal()
	oldDir, seq := g.dir, g.Seq()
	s.Close()

	created := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	nonce := fmt.Sprintf("%08x", uint32(created.Unix())) + "0badc0de"
	if err := os.Rename(oldDir, filepath.Join(root, fmt.Sprintf("seg-%016x-%s", seq, nonce))); err != nil {
		t.Fatal(err)
	}

	s2 := openSegs(t, root)
	defer s2.Close()
	var found *Segment
	for _, p := range s2.Pending() {
		if p.Nonce() == nonce {
			found = p
		}
	}
	if found == nil {
		t.Fatalf("segment %s not pending after reopen", nonce)
	}
	if !found.Created().Equal(created) {
		t.Fatalf("created = %v, want the nonce's %v", found.Created(), created)
	}
	if age := s2.Stats(time.Now()).OldestPendingAge; age < 3*time.Hour || age > 3*time.Hour+time.Minute {
		t.Fatalf("oldest pending age after restart = %v, want about 3h", age)
	}
	if !nonceTime("x").After(created) {
		t.Fatal("a nonce without a time must fall back to now")
	}
}
