package parquets3

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// The flusher's background loop, its back-off, the durable file writer, and
// the Storage plumbing that stops the flusher before the buffer closes.

func (e *segEnv) flusherAge(maxAge time.Duration) *BufferFlusher {
	e.t.Helper()
	f := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{
		TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: maxAge, Grace: time.Minute,
	})
	if err := f.load(time.Now()); err != nil {
		e.t.Fatalf("load flush state: %v", err)
	}
	return f
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second) // an upper bound only: slow CI runners under -race
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func uploadedTotal(e *segEnv) int {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	n := 0
	for _, c := range e.u.uploaded {
		n += c
	}
	return n
}

// Start seals by age and drains in the background; Stop ends it, is
// idempotent, and no tick runs afterwards.
func TestBufferFlusher_StartDrainsInTheBackgroundAndStopEndsIt(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Millisecond)
	e.ingest(segTenantA, hourAgo, 20)
	e.ingest(segTenantB, hourAgo, 10)
	f.Start(2 * time.Millisecond)

	waitFor(t, "the rows to reach Parquet", func() bool { return len(e.storedMsgs()) == 30 })
	e.mustHaveExactly(30, "background loop")

	stopped := make(chan struct{})
	go func() { f.Stop(); f.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return promptly (twice)")
	}

	before := uploadedTotal(e)
	e.ingest(segTenantA, hourAgo, 5)
	time.Sleep(40 * time.Millisecond) // many tick intervals
	if got := uploadedTotal(e); got != before {
		t.Errorf("%d uploads after Stop; want none", got-before)
	}
	if n := e.segs.Active().Rows(); n != 5 {
		t.Errorf("active segment has %d rows after Stop; want the 5 ingested after it", n)
	}
	e.storedOnce("after Stop")
}

// A non-positive interval defaults to one second; Stop before Start is fine.
func TestBufferFlusher_StopWithoutStartAndDefaultInterval(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	f.Stop()
	f.Stop()

	g := e.flusherAge(time.Hour)
	g.Start(0)
	done := make(chan struct{})
	go func() { g.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop after Start(0) hung")
	}
}

// A failed drain is counted, leaves the segment pending, waits for the
// back-off, and the retry sends the same bytes.
func TestBufferFlusher_TickFailureBacksOffAndRetriesIdentically(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	e.ingest(segTenantA, hourAgo, 12)
	e.failFn.Store(func(key string) bool { return !strings.Contains(key, manifest.SegmentMarkerDir) })

	errs0 := metrics.InsertFlushErrorsTotal.Get()
	up0 := metrics.BufferFlushErrors.Get("upload")
	now := time.Now().Add(2 * time.Hour) // the active segment is due
	f.tick(context.Background(), now)

	if got := metrics.InsertFlushErrorsTotal.Get() - errs0; got != 1 {
		t.Fatalf("flush error counter moved by %d; want 1", got)
	}
	if got := metrics.BufferFlushErrors.Get("upload") - up0; got != 1 {
		t.Errorf("upload stage counter moved by %d; want 1", got)
	}
	if n := len(e.segs.Pending()); n != 1 {
		t.Fatalf("%d segments pending after a failed drain; want 1", n)
	}
	if f.backoff != time.Second || !f.nextTry.Equal(now.Add(time.Second)) {
		t.Fatalf("backoff %s nextTry %s; want 1s after the failure", f.backoff, f.nextTry.Sub(now))
	}
	keys := make([]string, 0, 1)
	e.u.mu.Lock()
	for k := range e.u.attemptHashes {
		keys = append(keys, k)
	}
	e.u.mu.Unlock()
	if len(keys) != 1 {
		t.Fatalf("%d keys attempted; want 1", len(keys))
	}
	attempts := e.u.attempts(keys[0])

	// Inside the back-off nothing is retried and nothing is counted.
	f.tick(context.Background(), now.Add(500*time.Millisecond))
	if e.u.attempts(keys[0]) != attempts || metrics.InsertFlushErrorsTotal.Get()-errs0 != 1 {
		t.Error("the segment was retried inside the back-off")
	}

	// After it, the retry fails again and the back-off doubles.
	later := now.Add(2 * time.Second)
	f.tick(context.Background(), later)
	if e.u.attempts(keys[0]) != attempts+1 {
		t.Errorf("attempts %d; want one more after the back-off", e.u.attempts(keys[0]))
	}
	if f.backoff != 2*time.Second {
		t.Errorf("backoff %s after the second failure; want 2s", f.backoff)
	}

	// Recovery: stored once, byte-identical to every attempt (the uploader
	// reports any difference at cleanup), back-off reset.
	e.failFn.Store(func(string) bool { return false })
	f.tick(context.Background(), later.Add(10*time.Second))
	if f.backoff != 0 {
		t.Errorf("backoff %s after a clean drain; want reset", f.backoff)
	}
	if n := len(e.segs.Pending()); n != 0 {
		t.Errorf("%d segments pending after recovery", n)
	}
	e.mustHaveExactly(12, "after the retried tick")
	e.storedOnce("after the retried tick")
	e.u.mu.Lock()
	hs := e.u.attemptHashes[keys[0]]
	e.u.mu.Unlock()
	for _, h := range hs {
		if h != hs[0] {
			t.Fatalf("attempts of %s differ: %v", keys[0], hs)
		}
	}
}

func TestBufferFlusher_FailedBackoffDoublesAndCaps(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	e.ingest(segTenantA, hourAgo, 3)
	g := e.seal()
	now := time.Now()
	var got []time.Duration
	for i := 0; i < 8; i++ {
		f.failed(now, g, os.ErrInvalid)
		got = append(got, f.backoff)
		if !f.nextTry.Equal(now.Add(f.backoff)) {
			t.Fatalf("nextTry %s, want now+%s", f.nextTry.Sub(now), f.backoff)
		}
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 32 * time.Second, 32 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backoff sequence %v; want %v", got, want)
		}
	}
}

// A state directory that cannot be written stops the drain before any upload.
func TestBufferFlusher_UnwritableStateBlocksTheDrainBeforeAnyUpload(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	f.statePath = filepath.Join(e.dir, "no-such-dir", "buffer_flush_state.json")
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()

	intent0 := metrics.BufferFlushErrors.Get("intent")
	err := f.drain(context.Background(), g)
	if err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("drain error %v; want a failure to record the segment as draining", err)
	}
	if metrics.BufferFlushErrors.Get("intent")-intent0 != 1 {
		t.Error("intent error not counted")
	}
	if n := uploadedTotal(e); n != 0 {
		t.Errorf("%d objects uploaded without a durable draining record", n)
	}
	if len(e.segs.Pending()) != 1 {
		t.Error("the segment is no longer pending")
	}
}

func TestWriteFileDurable_ErrorsLeaveThePreviousFileReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state")
	if err := writeFileDurable(path, []byte("old")); err != nil {
		t.Fatal(err)
	}

	// The directory does not exist.
	if err := writeFileDurable(filepath.Join(dir, "missing", "state"), []byte("x")); err == nil {
		t.Error("no error for a missing directory")
	}

	// The target is a directory: the rename fails.
	if err := os.Mkdir(filepath.Join(dir, "isdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeFileDurable(filepath.Join(dir, "isdir"), []byte("x")); err == nil {
		t.Error("no error when the target is a directory")
	}

	// fsync of the file fails: the rename never happens.
	orig := fsyncFile
	t.Cleanup(func() { fsyncFile = orig })
	fsyncFile = func(fh *os.File) error {
		if st, err := fh.Stat(); err == nil && !st.IsDir() {
			return os.ErrDeadlineExceeded
		}
		return nil
	}
	if err := writeFileDurable(path, []byte("new")); err == nil {
		t.Error("no error when fsync fails")
	}
	if b, _ := os.ReadFile(path); string(b) != "old" {
		t.Errorf("previous content = %q after a failed write; want it intact", b)
	}

	// fsync of the directory fails: reported (the rename is not durable).
	fsyncFile = func(fh *os.File) error {
		if st, err := fh.Stat(); err == nil && st.IsDir() {
			return os.ErrDeadlineExceeded
		}
		return nil
	}
	if err := writeFileDurable(path, []byte("newer")); err == nil {
		t.Error("no error when the directory fsync fails")
	}
	fsyncFile = orig
	if err := writeFileDurable(path, []byte("final")); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); string(b) != "final" {
		t.Errorf("content = %q; want final", b)
	}
}

// A failed state write keeps the last committed state readable.
func TestBufferFlusher_FailedStateWriteKeepsTheLastState(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	if err := f.writeState(flushState{CommittedThroughSeq: 7}); err != nil {
		t.Fatal(err)
	}
	orig := fsyncFile
	t.Cleanup(func() { fsyncFile = orig })
	fsyncFile = func(*os.File) error { return os.ErrDeadlineExceeded }
	if err := f.writeState(flushState{CommittedThroughSeq: 9}); err == nil {
		t.Fatal("writeState succeeded with a failing fsync")
	}
	fsyncFile = orig
	st, err := readFlushState(f.statePath)
	if err != nil || st.CommittedThroughSeq != 7 {
		t.Fatalf("state = %+v, %v; want committed_through_seq 7 intact", st, err)
	}
}

// --- Storage.Close / SetBufferFlusher ---

type orderBuffer struct {
	flusher       *BufferFlusher
	closes        atomic.Int32
	flusherStoped bool
}

func (b *orderBuffer) Snapshot() *membuffer.Snapshot { return nil }
func (b *orderBuffer) Close() {
	b.closes.Add(1)
	select {
	case <-b.flusher.done:
		b.flusherStoped = true
	default:
	}
}

func TestStorageClose_StopsTheFlusherBeforeTheBuffer(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Millisecond)
	f.Start(time.Millisecond)

	s := testStorage()
	buf := &orderBuffer{flusher: f}
	s.SetLocalBuffer(buf)
	s.SetBufferFlusher(f)
	if s.bufferFlusher != f {
		t.Fatal("SetBufferFlusher did not record the flusher")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.closes.Load() != 1 {
		t.Fatalf("buffer closed %d times; want once", buf.closes.Load())
	}
	if !buf.flusherStoped {
		t.Error("the buffer was closed while the flusher was still running")
	}
	// Close is repeatable (Stop is idempotent).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageClose_WithoutFlusherOrBuffer(t *testing.T) {
	s := testStorage()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A buffer without a flusher (select-only or tests) is still closed.
	buf := &orderBuffer{flusher: &BufferFlusher{done: make(chan struct{})}}
	s.SetLocalBuffer(buf)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.closes.Load() != 1 {
		t.Errorf("buffer closed %d times; want 1", buf.closes.Load())
	}
}

// --- RefreshDiscovery ---

func stubDiscovery(peerService string, hosts func(host string) ([]string, error)) *discovery.Discovery {
	return discovery.New("", nil, "", peerService, "9428", 5*time.Second,
		discovery.WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no srv"}
		}),
		discovery.WithLookupHost(func(_ context.Context, host string) ([]string, error) { return hosts(host) }),
	)
}

func bridgeEndpoints(b *BufferBridge) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]string(nil), b.endpoints...)
}

// An insert service that resolves to nothing empties the bridge (the insert
// pods are gone); a later answer fills it again, each with the scheme added.
func TestRefreshDiscovery_InsertServiceEmptyThenBack(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.cfg.Select.InsertHeadlessService = "ins:9428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	var answer atomic.Value
	answer.Store([]string{"10.0.0.1"})
	s.discovery = stubDiscovery("", func(string) ([]string, error) { return answer.Load().([]string), nil })

	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := bridgeEndpoints(s.bufferBridge); len(got) != 1 || got[0] != "http://10.0.0.1:9428" {
		t.Fatalf("endpoints %v; want http://10.0.0.1:9428", got)
	}
	answer.Store([]string{})
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := bridgeEndpoints(s.bufferBridge); len(got) != 0 || s.bufferBridge.HasPeers() {
		t.Errorf("endpoints %v after an empty answer; want none", got)
	}
	answer.Store([]string{"10.0.0.3", "10.0.0.2"})
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(bridgeEndpoints(s.bufferBridge), ","); got != "http://10.0.0.2:9428,http://10.0.0.3:9428" {
		t.Errorf("endpoints %s; want both insert pods, sorted", got)
	}
}

// A peer ring that does not resolve is the refresh's error and leaves the
// bridge and cache as they were.
func TestRefreshDiscovery_PeerRingErrorKeepsTheBridge(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.bufferBridge.SetEndpoints([]string{"10.5.5.5:9428"})
	s.discovery = stubDiscovery("peers", func(string) ([]string, error) { return nil, &net.DNSError{Err: "gone"} })
	err := s.RefreshDiscovery(context.Background())
	if err == nil || !strings.Contains(err.Error(), "discover peers") {
		t.Fatalf("error %v; want a peer discovery error", err)
	}
	if got := bridgeEndpoints(s.bufferBridge); len(got) != 1 || got[0] != "http://10.5.5.5:9428" {
		t.Errorf("endpoints %v; want them unchanged", got)
	}
}

// With AZ awareness the peers' zones are fetched (with the peer auth key) and
// feed the cache's ring and the bridge; the bridge is left alone when an
// insert service is configured.
func TestRefreshDiscovery_AZAwarePeersFeedCacheAndBridge(t *testing.T) {
	var gotKey atomic.Value
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/cache/stats" {
			http.NotFound(w, r)
			return
		}
		gotKey.Store(r.Header.Get("X-Peer-Auth-Key"))
		_ = json.NewEncoder(w).Encode(map[string]string{"az": "az-a"})
	}))
	defer peer.Close()
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(peer.URL, "http://"))

	build := func(insertSvc string) *Storage {
		s := testStorage()
		s.cfg.Select.BufferQueryEnabled = true
		s.cfg.Select.InsertHeadlessService = insertSvc
		s.cfg.Peer = config.PeerConfig{AZAware: true, AZMode: "strict", AZMinPeersPerAZ: 2, AuthKey: "k3y"}
		s.selfAZ = "az-a"
		s.peerCache = peercache.New(host+":"+port, "k3y", time.Second, 2)
		s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
		s.discovery = stubDiscovery("peers:"+port, func(h string) ([]string, error) {
			if h == "peers" {
				return []string{host}, nil
			}
			return nil, &net.DNSError{Err: "unknown"}
		})
		return s
	}

	s := build("")
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotKey.Load() != "k3y" {
		t.Errorf("peer AZ request carried auth key %v; want k3y", gotKey.Load())
	}
	if st := s.peerCache.StatsAZ(); st.SameAZMembers != 1 || st.CrossAZMembers != 0 || st.SelfAZ != "az-a" {
		t.Errorf("peer cache AZ stats %+v; want one same-AZ member", st)
	}
	if got := bridgeEndpoints(s.bufferBridge); len(got) != 1 || got[0] != "http://"+host+":"+port {
		t.Errorf("bridge endpoints %v; want the peer with scheme", got)
	}
	s.bufferBridge.mu.RLock()
	same := len(s.bufferBridge.sameAZEndpoints)
	s.bufferBridge.mu.RUnlock()
	if same != 1 {
		t.Errorf("%d same-AZ bridge endpoints; want 1", same)
	}

	// With an insert service the peer ring does not touch the bridge.
	s = build("ins:9428")
	s.bufferBridge.SetEndpoints([]string{"10.7.7.7:9428"})
	s.discovery = stubDiscovery("peers:"+port, func(h string) ([]string, error) {
		if h == "peers" {
			return []string{host}, nil
		}
		return []string{"10.8.8.8"}, nil
	})
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := bridgeEndpoints(s.bufferBridge); len(got) != 1 || got[0] != "http://10.8.8.8:9428" {
		t.Errorf("bridge endpoints %v; want the insert service's pod only", got)
	}
}

// --- drain failure stages ---

func TestNewBufferFlusher_Defaults(t *testing.T) {
	f := newBufferFlusher(nil, nil, t.TempDir(), nil, BufferFlusherConfig{Grace: -time.Second})
	if f.maxRows != (128<<20)/estBytesPerTraceRow || f.maxAge != 5*time.Minute || f.sealBytes != 128<<20 || f.grace != 0 {
		t.Errorf("defaults: maxRows %d maxAge %s sealBytes %d grace %s", f.maxRows, f.maxAge, f.sealBytes, f.grace)
	}
	if g := newBufferFlusher(nil, nil, t.TempDir(), nil, BufferFlusherConfig{TargetBytes: 1}); g.maxRows != 1 {
		t.Errorf("maxRows %d for a 1-byte target; want 1", g.maxRows)
	}
}

func TestReadFlushState_RejectsOtherVersionsAndGarbage(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{"old": `{"version":1}`, "junk": `not json`} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readFlushState(p); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := readFlushState(filepath.Join(dir, "absent")); !os.IsNotExist(err) {
		t.Errorf("absent: %v; want not-exist", err)
	}
}

// A torn main state file falls back to the backup copy.
func TestBufferFlusher_LoadFallsBackToThePreviousState(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	if err := f.writeState(flushState{CommittedThroughSeq: 4}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.statePath, []byte(`{"torn`), 0o600); err != nil {
		t.Fatal(err)
	}
	g := e.flusherAge(time.Hour) // load() inside
	if g.state.CommittedThroughSeq != 4 {
		t.Errorf("committed_through_seq %d; want 4 from the backup", g.state.CommittedThroughSeq)
	}
}

// A mark that cannot be made durable is counted but the group still counts as
// stored: the segment commits and every row is written once.
func TestBufferFlusher_MarkFailureIsCountedNotFatal(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	e.ingest(segTenantA, hourAgo, 9)
	g := e.seal()
	orig := fsyncFile
	t.Cleanup(func() { fsyncFile = orig })
	fsyncFile = func(fh *os.File) error {
		if strings.HasSuffix(fh.Name(), ".stored") {
			return os.ErrDeadlineExceeded
		}
		return orig(fh)
	}
	m0 := metrics.BufferFlushErrors.Get("mark")
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if metrics.BufferFlushErrors.Get("mark")-m0 < 1 {
		t.Error("the failed mark was not counted")
	}
	e.mustHaveExactly(9, "after a failed mark")
	e.storedOnce("after a failed mark")
}

// The commit marker failing keeps the segment pending and uncommitted; the
// retry writes the marker and commits without re-uploading the objects.
func TestBufferFlusher_MarkerFailureHoldsTheCommit(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	e.ingest(segTenantA, hourAgo, 8)
	g := e.seal()
	e.marks.fail = func(string) error { return errPutFailed }
	m0 := metrics.BufferFlushErrors.Get("marker")
	if err := f.drain(context.Background(), g); err == nil {
		t.Fatal("drain succeeded without its marker")
	}
	if metrics.BufferFlushErrors.Get("marker")-m0 != 1 {
		t.Error("marker failure not counted")
	}
	if f.state.CommittedThroughSeq >= g.Seq() || len(e.segs.Pending()) != 1 {
		t.Fatalf("segment committed without a marker (state %+v)", f.state)
	}
	up := uploadedTotal(e)
	e.marks.fail = nil
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if uploadedTotal(e) != up {
		t.Error("objects uploaded again on the retry")
	}
	if !e.markerStored(g.Nonce()) || f.state.CommittedThroughSeq != g.Seq() {
		t.Error("marker or commit missing after the retry")
	}
	e.mustHaveExactly(8, "after the marker retry")
}

// A tick whose context is cancelled drains nothing and does not count an
// error: the segment resumes after the restart.
func TestBufferFlusher_CancelledTickDrainsNothingAndCountsNoError(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	e.ingest(segTenantA, hourAgo, 4)
	e.seal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	errs0 := metrics.InsertFlushErrorsTotal.Get()
	f.tick(ctx, time.Now())
	if metrics.InsertFlushErrorsTotal.Get() != errs0 {
		t.Error("a cancelled tick counted a flush error")
	}
	if uploadedTotal(e) != 0 || len(e.segs.Pending()) != 1 {
		t.Error("a cancelled tick drained")
	}
}

// Tenants of one account are drained in project order, each group stored once.
func TestBufferFlusher_SameAccountTenantsAreAllDrained(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	for _, tn := range []logstorage.TenantID{{AccountID: 5, ProjectID: 9}, {AccountID: 5, ProjectID: 1}, {AccountID: 4, ProjectID: 7}} {
		e.ingest(tn, hourAgo, 6)
	}
	g := e.seal()
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	e.mustHaveExactly(18, "three tenants")
	e.storedOnce("three tenants")
	for _, prefix := range []string{"t5-9/", "t5-1/", "t4-7/"} {
		found := false
		for _, k := range e.storedDataKeys() {
			found = found || strings.HasPrefix(k, prefix)
		}
		if !found {
			t.Errorf("no object under %s", prefix)
		}
	}
}

// Draining with a cancelled context stops with an error and commits nothing.
func TestBufferFlusher_DrainWithCancelledContextCommitsNothing(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	e.ingest(segTenantA, hourAgo, 4)
	g := e.seal()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.drain(ctx, g); err == nil {
		t.Fatal("drain with a cancelled context succeeded")
	}
	if f.state.CommittedThroughSeq >= g.Seq() || uploadedTotal(e) != 0 {
		t.Error("a cancelled drain committed or uploaded")
	}
}

func TestSyncDir_MissingDirectory(t *testing.T) {
	if err := syncDir(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Error("no error for a missing directory")
	}
}

// Stored marks of other segments, torn lines and unparsable numbers are
// ignored; valid marks of the named segment are returned.
func TestBufferFlusher_LoadMarksIgnoresForeignAndTornLines(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusherAge(time.Hour)
	body := strings.Join([]string{
		"nonceA\t1\t2\t2026100100\t0",
		"nonceB\t1\t2\t2026100100\t1", // another segment
		"nonceA\tx\t2\t2026100100\t2", // bad account
		"nonceA\t1\ty\t2026100100\t3", // bad project
		"nonceA\t1\t2\t2026100100\tz", // bad slice
		"nonceA\t1\t2\t\t4",           // empty partition
		"nonceA\t1\t2\t2026100101\t5",
		"nonceA\t1\t2\t20261001", // torn
	}, "\n")
	if err := os.WriteFile(f.storedPath(), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	got := f.loadMarks("nonceA")
	if len(got) != 2 {
		t.Fatalf("%d marks %v; want the 2 valid ones", len(got), got)
	}
	for _, ref := range []flushGroupRef{{1, 2, "2026100100", 0}, {1, 2, "2026100101", 5}} {
		if _, ok := got[ref]; !ok {
			t.Errorf("mark %+v missing", ref)
		}
	}
	if len(f.loadMarks("none")) != 0 {
		t.Error("marks returned for an unknown segment")
	}
}

// --- Storage accessors that must be safe with the optional parts absent ---

func TestStorage_OptionalPartsAbsentGiveZeroValues(t *testing.T) {
	s := testStorage()
	if s.DiskCacheBytes() != 0 || s.PmetaResidentBytes() != 0 || s.PmetaPersistedBytes() != 0 {
		t.Error("byte gauges non-zero without disk cache / pmeta")
	}
	if s.PmetaPersistedBytesByTenant() != nil || s.PmetaMetadataBytesByField() != nil {
		t.Error("per-tenant/per-field footprints non-nil without pmeta")
	}
	if s.PmetaCardinality("service.name") != 0 {
		t.Error("cardinality non-zero without pmeta")
	}
	if s.FooterCache() != nil {
		t.Error("footer cache present on a bare storage")
	}
	s.PrefetchFootersByKeys(context.Background(), []string{"a", "b"}, 2) // no pool: a no-op, no panic
	s.memCache.Put("k", []byte("v"))
	s.ClearCaches()
	if _, ok := s.memCache.Get("k"); ok {
		t.Error("ClearCaches left the memory cache populated")
	}
}

func TestCurrentSchemaFingerprint_StablePerModeAndDistinct(t *testing.T) {
	l1, l2 := CurrentSchemaFingerprint(config.ModeTraces), CurrentSchemaFingerprint(config.ModeTraces)
	tr := CurrentSchemaFingerprint(config.ModeLogs)
	if l1 == "" || l1 != l2 {
		t.Errorf("logs fingerprint %q / %q not stable", l1, l2)
	}
	if tr == l1 {
		t.Error("logs and traces share a schema fingerprint")
	}
}

func TestBatchWriter_SettledCallsTheHookOnlyWhenSet(t *testing.T) {
	e := newSegEnv(t)
	var got []string
	e.bw.settled(func(k string) { got = append(got, k) }, "obj")
	e.bw.settled(nil, "ignored")
	if len(got) != 1 || got[0] != "obj" {
		t.Errorf("settled hook calls %v; want [obj]", got)
	}
}

// --- the unflushed rows an insert pod serves to its peers ---

func TestBridgeSource_ServesTheTenantOrAllTenantsAndNoSpansInLogsMode(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 4)
	e.ingest(segTenantB, hourAgo, 3)
	e.segs.DebugFlush()
	src := BridgeSource{Segments: e.segs}
	start, end := hourAgo.Add(-time.Minute).UnixNano(), hourAgo.Add(time.Minute).UnixNano()

	one, err := src.ReadBuffer(context.Background(), buffer.Selection{AccountID: segTenantA.AccountID, ProjectID: segTenantA.ProjectID}, start, end, string(config.ModeTraces))
	if err != nil || len(one.Traces) != 4 {
		t.Fatalf("one tenant: %d rows, %v; want 4", len(one.Traces), err)
	}
	if len(one.Nonces) == 0 {
		t.Error("the answer names no segments")
	}
	all, err := src.ReadBuffer(context.Background(), buffer.Selection{All: true}, start, end, string(config.ModeTraces))
	if err != nil || len(all.Traces) != 7 {
		t.Fatalf("all tenants: %d rows, %v; want 7", len(all.Traces), err)
	}
	tr, err := src.ReadBuffer(context.Background(), buffer.Selection{All: true}, start, end, string(config.ModeLogs))
	if err != nil || len(tr.Traces) != 0 || len(tr.Nonces) == 0 {
		t.Errorf("logs mode: %d span rows, %d nonces, %v; want no rows but the segments named", len(tr.Traces), len(tr.Nonces), err)
	}
	none, err := src.ReadBuffer(context.Background(), buffer.Selection{AccountID: 99}, start, end, string(config.ModeTraces))
	if err != nil || len(none.Traces) != 0 {
		t.Errorf("unknown tenant: %d rows, %v; want none", len(none.Traces), err)
	}
}

func TestBufferTenantAccountIDs_ListsTheBufferedAccounts(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 2)
	e.ingest(segTenantB, hourAgo, 2)
	e.segs.DebugFlush()
	s := testStorage()
	seen := map[uint32]struct{}{9: {}}
	s.bufferTenantAccountIDs(seen) // no buffer: unchanged
	if len(seen) != 1 {
		t.Fatalf("seen %v without a buffer", seen)
	}
	s.SetLocalBuffer(e.segs)
	s.bufferTenantAccountIDs(seen)
	for _, a := range []uint32{1, 2, 9} {
		if _, ok := seen[a]; !ok {
			t.Errorf("account %d missing from %v", a, seen)
		}
	}
}

// A group whose key the manifest already has (adopted by a listing) or has
// retired (compacted, rewritten) is settled without a PUT: the flusher is told,
// so its record of what is done includes it, and retired rows are counted.
func TestBatchWriter_UploadGroupSettlesLiveAndRetiredKeysWithoutAPut(t *testing.T) {
	e := newSegEnv(t)
	logRows := make([]schema.LogRow, 3)
	traceRows := make([]schema.TraceRow, 2)

	var told []string
	hook := func(k string) { told = append(told, k) }

	live := &logGroupUpload{partition: "dt=2026-10-01/hour=00", batchID: "live", rows: logRows, onStored: hook}
	key := e.bw.assignLogKey(live)
	e.m.AddFile(live.partition, manifest.FileInfo{Key: key})
	if err := e.bw.uploadLogGroup(context.Background(), live); err != nil {
		t.Fatal(err)
	}

	retired := &logGroupUpload{partition: "dt=2026-10-01/hour=00", batchID: "gone", rows: logRows, onStored: hook}
	rkey := e.bw.assignLogKey(retired)
	e.m.Retire(rkey, "", false)
	before := metrics.InsertRowsSuperseded.Get()
	if err := e.bw.uploadLogGroup(context.Background(), retired); err != nil {
		t.Fatal(err)
	}
	if d := metrics.InsertRowsSuperseded.Get() - before; d != 3 {
		t.Errorf("superseded rows rose by %d; want 3", d)
	}

	tlive := &traceGroupUpload{partition: "dt=2026-10-01/hour=01", batchID: "tlive", rows: traceRows, onStored: hook}
	tkey := e.bw.assignTraceKey(tlive)
	e.m.AddFile(tlive.partition, manifest.FileInfo{Key: tkey})
	if err := e.bw.uploadTraceGroup(context.Background(), tlive); err != nil {
		t.Fatal(err)
	}
	tret := &traceGroupUpload{partition: "dt=2026-10-01/hour=01", batchID: "tgone", rows: traceRows, onStored: hook}
	trkey := e.bw.assignTraceKey(tret)
	e.m.Retire(trkey, "", false)
	before = metrics.InsertRowsSuperseded.Get()
	if err := e.bw.uploadTraceGroup(context.Background(), tret); err != nil {
		t.Fatal(err)
	}
	if d := metrics.InsertRowsSuperseded.Get() - before; d != 2 {
		t.Errorf("superseded span rows rose by %d; want 2", d)
	}

	if strings.Join(told, ",") != strings.Join([]string{key, rkey, tkey, trkey}, ",") {
		t.Errorf("onStored told %v; want all four keys in order", told)
	}
	if n := uploadedTotal(e); n != 0 {
		t.Errorf("%d PUTs for settled groups; want none", n)
	}
}
