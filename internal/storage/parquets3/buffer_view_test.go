package parquets3

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

// viewEnv is a logs Storage serving reads from a mock S3 bucket and its own
// segmented insert buffer, with the flusher writing into that bucket.
type viewEnv struct {
	t    *testing.T
	srv  *mockS3Server
	s    *Storage
	dir  string
	segs *membuffer.Segments
	f    *BufferFlusher
	n    int
}

func newViewEnv(t *testing.T) *viewEnv {
	t.Helper()
	e := &viewEnv{t: t, srv: newMockS3Server(), dir: t.TempDir()}
	t.Cleanup(e.srv.srv.Close)
	e.s = testStorageWithS3(t, e.srv.url())
	e.s.writer = NewBatchWriter(testInsertConfig(), e.s.pool, e.s.manifest, "logs/", config.ModeLogs)
	e.open()
	t.Cleanup(func() { e.segs.Close() })
	return e
}

func (e *viewEnv) open() {
	e.t.Helper()
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: filepath.Join(e.dir, "buffer")})
	if err != nil {
		e.t.Fatal(err)
	}
	e.segs = segs
	e.s.localBuffer = segs
	e.f = newBufferFlusher(e.s.writer, segs, filepath.Join(e.dir, "buffer"), nil, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerLogRow, MaxAge: time.Hour, Grace: time.Minute})
	if err := e.f.load(time.Now()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *viewEnv) ingest(ts time.Time, n int) {
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		e.n++
		lr.MustAdd(logstorage.TenantID{}, ts.Add(time.Duration(i)*time.Second).UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "api"},
			{Name: "_msg", Value: fmt.Sprintf("row-%d", e.n)},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush() // upstream makes rows searchable within about a second on its own
}

// answerOn is answer on another Storage (a select pod reading the same bucket).
func (e *viewEnv) answerOn(s *Storage) map[string]int {
	e.t.Helper()
	now := time.Now()
	q, err := logstorage.ParseQueryAtTimestamp("*", now.UnixNano())
	if err != nil {
		e.t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), now.Add(-48*time.Hour).UnixNano(), now.UnixNano())
	var mu sync.Mutex
	got := map[string]int{}
	err = s.RunQuery(context.Background(), []logstorage.TenantID{{}}, q, func(_ uint, db *logstorage.DataBlock) {
		for _, c := range db.GetColumns(false) {
			if c.Name != "_msg" {
				continue
			}
			mu.Lock()
			for _, v := range c.Values {
				got[v]++
			}
			mu.Unlock()
		}
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return got
}

// exact fails unless the answer holds every ingested row exactly once.
func (e *viewEnv) exact(when string) { e.exactOn(e.s, when) }

func (e *viewEnv) exactOn(s *Storage, when string) {
	e.t.Helper()
	got := e.answerOn(s)
	var problems []string
	for i := 1; i <= e.n; i++ {
		k := fmt.Sprintf("row-%d", i)
		if got[k] != 1 {
			problems = append(problems, fmt.Sprintf("%s×%d", k, got[k]))
		}
	}
	if len(got) != e.n || len(problems) > 0 {
		e.t.Errorf("%s: %d distinct rows, want %d; wrong counts: %v", when, len(got), e.n, head(problems, 6))
	}
}

// Every row is answered exactly once at every step of its life: in the
// active segment, sealed, while its segment drains (after each group), once
// the segment is committed and still readable, after the segment is removed,
// and after a restart — with late rows mixed in, whose _time is older than
// rows already in Parquet. The time-watermark read path hid such rows and
// served flushed rows twice after a restart (#279's class).
func TestBufferView_EachRowOnceThroughTheWholeHandoff(t *testing.T) {
	e := newViewEnv(t)
	base := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	e.ingest(base.Add(10*time.Minute), 20)
	e.ingest(base.Add(70*time.Minute), 20)
	e.exact("active segment")

	g, _ := e.segs.Seal()
	e.exact("sealed")

	// Queries between the groups of the drain.
	var mid atomic.Int32
	e.s.writer.SetStatsCallback(func(_, _ uint32, _, _, _ int64, _ string) {
		mid.Add(1)
		e.exact(fmt.Sprintf("mid-drain after group %d", mid.Load()))
	})
	if err := e.f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	e.s.writer.SetStatsCallback(nil)
	if mid.Load() < 2 {
		t.Fatalf("the drain wrote %d groups; want several", mid.Load())
	}
	e.exact("committed, in grace")

	// Late rows: older than everything already in Parquet.
	e.ingest(base.Add(5*time.Minute), 7)
	e.exact("late rows in the active segment")

	if n := e.segs.Reap(time.Now().Add(time.Hour), time.Minute); n != 1 {
		t.Fatalf("reaped %d", n)
	}
	e.exact("first segment removed")

	g2, _ := e.segs.Seal()
	if err := e.f.drain(context.Background(), g2); err != nil {
		t.Fatal(err)
	}
	e.exact("late rows written")

	// Restart: the committed segment comes back in grace, the manifest is
	// rebuilt from a listing.
	e.segs.Close()
	e.open()
	e.exact("after the restart")
	e.segs.Reap(time.Now().Add(time.Hour), time.Minute)
	e.exact("after the restart, segments removed")
}

// Queries running concurrently with seals, drains and removals always see
// every row exactly once.
func TestBufferView_ConcurrentQueriesDuringDrains(t *testing.T) {
	e := newViewEnv(t)
	base := time.Now().Add(-2 * time.Hour).Truncate(time.Hour)
	for i := 0; i < 4; i++ {
		e.ingest(base.Add(time.Duration(i)*17*time.Minute), 25)
	}
	e.segs.DebugFlush()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var queries atomic.Int32
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				e.exact("concurrent")
				queries.Add(1)
			}
		}()
	}
	for i := 0; i < 3; i++ {
		g, ok := e.segs.Seal()
		if ok {
			if err := e.f.drain(context.Background(), g); err != nil {
				t.Error(err)
			}
		}
		e.segs.Reap(time.Now().Add(time.Hour), time.Minute)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if queries.Load() == 0 {
		t.Fatal("no concurrent query ran")
	}
}

// A select node without a buffer of its own (peers) drops the objects of the
// segments a peer served and keeps every other object.
func TestBufferView_ExcludeOnlyLiveSegmentObjects(t *testing.T) {
	live := "65000000aaaabbbb"
	v := &bufferView{nonces: map[string]struct{}{live: {}}}
	files := []manifest.FileInfo{
		{Key: "logs/dt=2026-10-02/hour=06/" + live + "-0.parquet"},
		{Key: "logs/dt=2026-10-02/hour=06/" + live + "-1a.parquet"},
		{Key: "logs/dt=2026-10-02/hour=06/65000000ccccdddd-0.parquet"}, // committed, not served
		{Key: "logs/dt=2026-10-02/hour=06/0123456789abcdef.parquet"},   // compaction output / legacy
	}
	got := v.exclude(files)
	if len(got) != 2 || got[0].Key != files[2].Key || got[1].Key != files[3].Key {
		t.Errorf("exclude kept %v", got)
	}
	// An empty nonce (a peer that sent none) never matches an object without one.
	v.nonces[""] = struct{}{}
	if got := v.exclude(files); len(got) != 2 {
		t.Errorf("with an empty nonce in the set, exclude kept %d objects; want 2", len(got))
	}
}

// A select node with no segments of its own reads an insert pod through the
// buffer bridge: the pod's rows and the nonces of the segments they come from
// travel together, so the objects the pod flushed from them are not read on top
// — before, during and after the flush, and once the pod removes the segment.
func TestBufferView_BridgedPeerHandoff(t *testing.T) {
	e := newViewEnv(t) // the insert pod: segments, flusher, writer
	peer := httptest.NewServer(buffer.NewHandler(BridgeSource{Segments: e.segs}, ""))
	t.Cleanup(peer.Close)

	sel := testStorageWithS3(t, e.srv.url()) // the select pod
	sel.cfg.Mode = config.ModeLogs
	sel.bufferBridge = NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 5 * time.Second}, config.ModeLogs)
	sel.bufferBridge.SetEndpoints([]string{peer.URL})
	if sel.useLocalBuffer() {
		t.Fatal("a select pod with a peer read its own buffer")
	}
	listing := func() {
		t.Helper()
		if err := sel.manifest.RefreshFromS3(context.Background(), sel.pool.S3Client()); err != nil {
			t.Fatal(err)
		}
	}

	base := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	e.ingest(base.Add(10*time.Minute), 20)
	e.ingest(base.Add(70*time.Minute), 20)
	e.exactOn(sel, "unflushed rows through the bridge")

	g, _ := e.segs.Seal()
	e.exactOn(sel, "sealed")

	var groups atomic.Int32
	e.s.writer.SetStatsCallback(func(_, _ uint32, _, _, _ int64, _ string) {
		groups.Add(1)
		listing() // the select pod lists the bucket between the groups
		e.exactOn(sel, fmt.Sprintf("mid-drain after group %d", groups.Load()))
	})
	if err := e.f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	e.s.writer.SetStatsCallback(nil)
	if groups.Load() < 2 {
		t.Fatalf("the drain wrote %d groups; want several", groups.Load())
	}
	listing()
	e.exactOn(sel, "committed, the pod still serves the segment")

	// Late rows, older than everything already in Parquet.
	e.ingest(base.Add(5*time.Minute), 7)
	e.exactOn(sel, "late rows in the active segment")

	if n := e.segs.Reap(time.Now().Add(time.Hour), time.Minute); n != 1 {
		t.Fatalf("reaped %d", n)
	}
	e.exactOn(sel, "first segment removed from the pod, its objects listed")
}

// The bridge answers a peer that sends no segment header (an older build)
// without dropping any object: every row stays exactly once only if nothing was
// flushed from the peer's buffer, which is the case this pins.
func TestBufferView_PeerWithoutNoncesExcludesNothing(t *testing.T) {
	v := &bufferView{bridged: true}
	files := []manifest.FileInfo{{Key: "logs/dt=2026-10-02/hour=06/65000000aaaabbbb-0.parquet"}}
	if got := v.exclude(files); len(got) != 1 {
		t.Errorf("a view without nonces dropped %d objects", len(files)-len(got))
	}
}

// The flusher writes the pmeta bundles a drain changed, and retries a bundle a
// failed PUT left dirty on a tick that drained nothing: the catalog's bloom
// facet cannot be rebuilt from the manifest, so it must reach the bucket.
func TestBufferFlusher_PersistsThePmetaBundles(t *testing.T) {
	e := newViewEnv(t)
	cs := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	e.s.catalog = cs
	e.s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	e.s.writer.catalogObserver = &catalogObserver{store: cs, pool: e.s.pool}
	bundles := func() int {
		e.srv.mu.RLock()
		defer e.srv.mu.RUnlock()
		n := 0
		for k := range e.srv.files {
			if strings.Contains(k, "_pmeta") {
				n++
			}
		}
		return n
	}

	e.ingest(time.Now().Add(-2*time.Hour), 10)
	e.segs.Seal()
	e.f.tick(context.Background(), time.Now())
	if len(e.segs.Pending()) != 0 {
		t.Fatal("the segment was not drained")
	}
	if bundles() == 0 {
		t.Fatal("no pmeta bundle reached the bucket after the drain")
	}
}
