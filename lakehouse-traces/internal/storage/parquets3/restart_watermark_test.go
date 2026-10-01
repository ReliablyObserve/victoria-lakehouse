package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// ---------------------------------------------------------------------------
// Graceful restart (spans): rows buffered after it, in the same UTC hour as the data
// the shutdown's final flush wrote, must be visible exactly once.
//
// The manifest learns the final flush's objects only from the S3 listing (the
// snapshot taken before Close() lacks them), with time bounds inferred from the
// partition hour. Those inferred bounds must never feed the buffer watermark:
// its MaxTimeNs would be the END of the hour and hide every buffered row of the
// rest of it. And the rows the buffer held before the shutdown are restored on
// open AND are in the flushed object, so exact bounds must keep them from being
// counted twice.
//
// Twin of internal/storage/parquets3/restart_watermark_test.go (the logs module).
// ---------------------------------------------------------------------------

var rwHour = time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC)

// rwBuffer stands in for the logstorage-native buffer: it keeps every row it
// was given, across restarts (its parts are restored on open), and answers a
// query with the rows inside the query's time window.
type rwBuffer struct {
	mu   sync.Mutex
	rows []rwBufRow
}

type rwBufRow struct {
	ts    int64
	level string
}

func (b *rwBuffer) add(level string, at ...time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range at {
		b.rows = append(b.rows, rwBufRow{ts: t.UnixNano(), level: level})
	}
}

func (b *rwBuffer) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	start, end := qctx.Query.GetFilterTimeRange()
	b.mu.Lock()
	var msgs, levels, times []string
	for _, r := range b.rows {
		if r.ts >= start && r.ts <= end {
			msgs = append(msgs, "buffered")
			levels = append(levels, r.level)
			times = append(times, time.Unix(0, r.ts).UTC().Format(time.RFC3339Nano))
		}
	}
	b.mu.Unlock()
	if len(msgs) == 0 {
		return nil
	}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: times},
		{Name: "_msg", Values: msgs},
		{Name: "name", Values: levels},
	})
	writeBlock(0, db)
	return nil
}

func (b *rwBuffer) Close() {}

// rwRig is one Lakehouse pod across restarts: the bucket and the buffer's data
// directory survive, the process state (manifest, caches, writer) does not.
type rwRig struct {
	t        *testing.T
	mock     *mockS3Server
	buf      *rwBuffer
	snapshot string
	s        *Storage
	bw       *BatchWriter
}

// rwFreezeClock pins the clock the buffer-retention floor reads to just before
// the fixture hour: the fake buffer keeps every row it was given, regardless of
// the retention a real one applies, and the fixture's times are fixed.
func rwFreezeClock(t *testing.T) {
	t.Helper()
	old := nowFn
	nowFn = func() time.Time { return rwHour.Add(-time.Hour) }
	t.Cleanup(func() { nowFn = old })
}

func newRWRig(t *testing.T) *rwRig {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	rwFreezeClock(t)
	r := &rwRig{t: t, mock: mock, buf: &rwBuffer{}, snapshot: filepath.Join(t.TempDir(), "manifest.snapshot")}
	r.s, r.bw = r.boot(manifest.New("test-bucket", "logs/"))
	return r
}

func (r *rwRig) boot(m *manifest.Manifest) (*Storage, *BatchWriter) {
	r.t.Helper()
	s := testStorageWithS3(r.t, r.mock.url())
	s.cfg.Mode = config.ModeTraces // as the traces binary runs (the buffer bridge decodes spans)
	s.manifest = m
	s.localBuffer = r.buf
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	s.writer = bw
	return s, bw
}

// ingest writes rows the way the insert path does: into the buffer and into the
// writer awaiting its next flush.
func (r *rwRig) ingest(level string, at ...time.Time) {
	r.t.Helper()
	rows := make([]schema.TraceRow, len(at))
	for i, ts := range at {
		n := ts.UnixNano()
		rows[i] = schema.TraceRow{
			TimestampUnixNano: n, StartTimeUnixNano: n, TraceID: fmt.Sprintf("t%d", n), SpanID: fmt.Sprintf("s%d", n),
			SpanName: level, ServiceName: "svc-" + level,
			Stream: `{resource_attr:service.name="svc-` + level + `"}`, StreamID: "id-" + level,
		}
	}
	r.bw.AddTraceRows(rows)
	r.buf.add(level, at...)
}

// bufferOnly adds rows to the buffer alone: they arrive after the restart and
// have not been flushed.
func (r *rwRig) bufferOnly(level string, at ...time.Time) { r.buf.add(level, at...) }

// restart is a graceful shutdown followed by a boot. snapshotBefore persists the
// manifest before the final flush (runShutdown's first persist); snapshotAfter
// persists it again after Close (the fix). The new pod loads the snapshot, then
// learns whatever else the bucket holds from the S3 listing.
func (r *rwRig) restart(snapshotBefore, snapshotAfter bool) {
	r.t.Helper()
	if snapshotBefore {
		if err := r.s.manifest.SaveTo(r.snapshot); err != nil {
			r.t.Fatal(err)
		}
	}
	if err := r.s.Close(); err != nil { // final flush
		r.t.Fatal(err)
	}
	if snapshotAfter {
		if err := r.s.manifest.SaveTo(r.snapshot); err != nil {
			r.t.Fatal(err)
		}
	}
	m := manifest.New("test-bucket", "logs/")
	if snapshotBefore || snapshotAfter {
		if err := m.LoadFrom(r.snapshot); err != nil {
			r.t.Fatal(err)
		}
	}
	s, bw := r.boot(m)
	if err := m.RefreshFromS3(context.Background(), s.pool.S3Client()); err != nil {
		r.t.Fatal(err)
	}
	r.s, r.bw = s, bw
}

func (r *rwRig) window(queryStr string, from, to time.Time) *logstorage.Query {
	r.t.Helper()
	return mustParseQueryWithTime(r.t, queryStr, from.UnixNano(), to.UnixNano())
}

// rows runs the query and returns how many rows the select path emitted.
func (r *rwRig) rows(ctx context.Context, from, to time.Time) int {
	r.t.Helper()
	total := 0
	var mu sync.Mutex
	err := r.s.RunQuery(ctx, nil, r.window("*", from, to), func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		total += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		r.t.Fatalf("RunQuery: %v", err)
	}
	return total
}

func (r *rwRig) levelHits(from, to time.Time) map[string]uint64 {
	r.t.Helper()
	got, err := r.s.GetFieldValues(context.Background(), nil, r.window("*", from, to), "name", 0)
	if err != nil {
		r.t.Fatalf("GetFieldValues: %v", err)
	}
	out := map[string]uint64{}
	for _, v := range got {
		out[v.Value] = v.Hits
	}
	return out
}

func rwSpan(ts time.Time, name string) schema.TraceRow {
	n := ts.UnixNano()
	return schema.TraceRow{TimestampUnixNano: n, StartTimeUnixNano: n, TraceID: fmt.Sprintf("t%d", n), SpanID: fmt.Sprintf("s%d", n), SpanName: name, ServiceName: "svc-" + name}
}

func at(base time.Time, d time.Duration) time.Time { return base.Add(d) }

// checkVisible asserts every select-path surface (query=*, the timestamp-only
// count path, field_values) sees exactly wantCold cold rows and wantNew buffered
// rows.
func (r *rwRig) checkVisible(from, to time.Time, wantCold, wantNew int) {
	r.t.Helper()
	want := wantCold + wantNew
	if got := r.rows(context.Background(), from, to); got != want {
		r.t.Errorf("query=* emitted %d rows, want %d (%d cold + %d buffered after the restart)", got, want, wantCold, wantNew)
	}
	if got := r.rows(storage.WithTimestampOnlyHint(context.Background()), from, to); got != want {
		r.t.Errorf("stats count path emitted %d rows, want %d", got, want)
	}
	hits := r.levelHits(from, to)
	if hits["COLD"] != uint64(wantCold) || hits["NEW"] != uint64(wantNew) {
		r.t.Errorf("field_values name = %v, want COLD=%d NEW=%d", hits, wantCold, wantNew)
	}
}

// The reported bug: cold rows in hour H, graceful shutdown with a final flush,
// restart, rows buffered in hour H afterwards.
func TestRestartWatermark_SameHourBufferedRowsVisibleExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		snapshotBefore, snapshotAfter bool
	}{
		{"snapshot before the final flush only (the object is learned by listing)", true, false},
		{"snapshot after the final flush (the object is in the snapshot)", true, true},
		{"no snapshot at all (cold manifest, listing only)", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRWRig(t)
			r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), at(rwHour, 10*time.Minute+2*time.Second))
			r.restart(tc.snapshotBefore, tc.snapshotAfter)
			// The pre-shutdown rows are still in the restored buffer AND in the
			// flushed object; these three arrive after the restart.
			r.bufferOnly("NEW", at(rwHour, 30*time.Minute), at(rwHour, 30*time.Minute+time.Second), at(rwHour, 45*time.Minute))
			r.checkVisible(rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour), 3, 3)
		})
	}
}

// Control: cold rows in the previous hour were never affected.
func TestRestartWatermark_PreviousHourControl(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, -40*time.Minute), at(rwHour, -39*time.Minute), at(rwHour, -38*time.Minute))
	r.restart(true, false)
	r.bufferOnly("NEW", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute), at(rwHour, 32*time.Minute))
	r.checkVisible(rwHour.Add(-2*time.Hour), rwHour.Add(2*time.Hour), 3, 3)
}

// Rows that straddle the hour boundary: the object of the earlier hour must not
// hide the buffered rows after it, nor those of the next hour.
func TestRestartWatermark_StraddlingTheHourBoundary(t *testing.T) {
	r := newRWRig(t)
	boundary := rwHour.Add(time.Hour) // 08:00
	r.ingest("COLD", at(boundary, -3*time.Second), at(boundary, -2*time.Second), at(boundary, -time.Second))
	r.restart(true, false)
	r.bufferOnly("NEW", at(boundary, -500*time.Millisecond), at(boundary, time.Second), at(boundary, 20*time.Minute))
	r.checkVisible(rwHour, boundary.Add(time.Hour), 3, 3)
	// Narrow windows on either side of the boundary.
	if got := r.rows(context.Background(), boundary, boundary.Add(time.Hour)); got != 2 {
		t.Errorf("rows at or after the boundary = %d, want 2", got)
	}
}

// Cold rows spanning two hours (two objects): both are exact, the buffered rows
// after the newest are visible.
func TestRestartWatermark_TwoObjectsTwoHours(t *testing.T) {
	r := newRWRig(t)
	boundary := rwHour.Add(time.Hour)
	r.ingest("COLD", at(boundary, -time.Minute), at(boundary, time.Minute), at(boundary, 2*time.Minute))
	r.restart(true, false)
	r.bufferOnly("NEW", at(boundary, 10*time.Minute), at(boundary, 11*time.Minute), at(boundary, 12*time.Minute))
	r.checkVisible(rwHour, boundary.Add(time.Hour), 3, 3)
}

// No double counting: a row that was in the buffer before the shutdown and in
// the final flush is counted once, however many times the pod restarts.
func TestRestartWatermark_NoDoubleCountAcrossRestarts(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute), at(rwHour, 7*time.Minute))
	r.restart(true, false)
	r.checkVisible(rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour), 3, 0)
	r.restart(true, true) // nothing new: the object is already exact
	r.checkVisible(rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour), 3, 0)
	r.bufferOnly("NEW", at(rwHour, 20*time.Minute))
	r.checkVisible(rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour), 3, 1)
}

// A peer's flush learned by listing in a multi-pod deployment takes the same
// path: the bridge's rows after the peer's real newest row stay visible.
func TestRestartWatermark_PeerObjectLearnedByListing(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute), at(rwHour, 12*time.Minute))
	// "This pod" never saw the flush: it learns the peer's object from a listing.
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ := r.boot(manifest.New("test-bucket", "logs/"))
	if err := s.manifest.RefreshFromS3(context.Background(), s.pool.S3Client()); err != nil {
		t.Fatal(err)
	}
	r.s = s
	r.s.localBuffer = nil
	r.s.bufferBridge = fvcPeer(t, []schema.TraceRow{
		rwSpan(at(rwHour, 11*time.Minute), "COLD"), // in the object
		rwSpan(at(rwHour, 30*time.Minute), "NEW"),
		rwSpan(at(rwHour, 31*time.Minute), "NEW"),
	})
	if got := r.rows(context.Background(), rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour)); got != 5 {
		t.Errorf("query=* emitted %d rows, want 5 (3 flushed by the peer + 2 buffered on it)", got)
	}
	if hits := r.levelHits(rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour)); hits["COLD"] != 3 || hits["NEW"] != 2 {
		t.Errorf("field_values name = %v, want COLD=3 NEW=2", hits)
	}
}

// Property: random sequences of (write, flush+restart, buffer-write) with random
// times across hour boundaries; every written row is visible exactly once.
func TestRestartWatermark_Property_EveryRowVisibleExactlyOnce(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rwPropertyRun(t, seed, 12)
		})
	}
}

func FuzzRestartWatermark(f *testing.F) {
	for _, seed := range []int64{0, 1, 7, 42, 1 << 40} {
		f.Add(seed, uint8(10))
	}
	f.Fuzz(func(t *testing.T, seed int64, steps uint8) {
		rwPropertyRun(t, seed, int(steps%10)+1) // short: the fuzz engine kills an input that runs over 10s
	})
}

// rwPropertyRun drives a random lifecycle: rows are written with strictly
// increasing times (a live clock; jumps cross hour boundaries), the pod
// restarts gracefully at random points (its final flush writes what the writer
// holds, the buffer keeps everything), with or without each of the two
// snapshots. Invariant, checked after every step: each written row is visible
// exactly once, whether it is still only in the buffer or already also in an
// object.
func rwPropertyRun(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	r := newRWRig(t)
	clock := rwHour.Add(time.Duration(rng.Intn(50*60)) * time.Second)
	next := func() time.Time {
		step := time.Duration(1+rng.Intn(40)) * time.Second
		if rng.Intn(5) == 0 {
			step = time.Duration(1+rng.Intn(50)) * time.Minute
		}
		clock = clock.Add(step)
		return clock
	}
	epoch := 0
	written := map[string]uint64{}
	total := 0
	check := func(step int) {
		from, to := rwHour.Add(-time.Hour), clock.Add(2*time.Hour)
		if got := r.rows(context.Background(), from, to); got != total {
			t.Fatalf("seed %d step %d: query=* emitted %d rows, want %d", seed, step, got, total)
		}
		hits := r.levelHits(from, to)
		for lvl, n := range written {
			if hits[lvl] != n {
				t.Fatalf("seed %d step %d: field_values name %s = %d, want %d (all: %v)", seed, step, lvl, hits[lvl], n, hits)
			}
		}
	}
	for i := 0; i < steps; i++ {
		if rng.Intn(3) == 0 {
			r.restart(rng.Intn(2) == 0, rng.Intn(2) == 0)
			epoch++
		} else {
			n := 1 + rng.Intn(3)
			ts := make([]time.Time, n)
			for j := range ts {
				ts[j] = next()
			}
			lvl := fmt.Sprintf("E%d", epoch)
			r.ingest(lvl, ts...)
			written[lvl] += uint64(n)
			total += n
		}
		check(i)
	}
}
