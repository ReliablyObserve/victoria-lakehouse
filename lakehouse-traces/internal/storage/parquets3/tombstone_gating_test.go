package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// Per-file tombstone gating (#612), traces twin of the logs tests: a tombstone
// that provably cannot match a file leaves that file on the metadata fast
// paths, a file it may match is read and the deleted spans are excluded. Spans
// are deleted mostly by trace_id, which the pmeta bloom facet proves.

const gateMaxNs = int64(1<<63 - 1)

const gateSvc = `"resource_attr:service.name"`

type gateFx struct {
	t      *testing.T
	s      *Storage
	mock   *mockS3Server
	rows   [][]schema.TraceRow
	files  []manifest.FileInfo
	start  int64
	end    int64
	tombID int
	// warmFooters caches every footer before a run, so reads are ranged and
	// column-projected (a cold footer makes a small object a whole download).
	warmFooters bool
	gets        atomic.Int64 // object reads the bucket served
	bytes       atomic.Int64 // bytes those reads returned
}

// gateCountingWriter counts the body bytes a read returns.
type gateCountingWriter struct {
	http.ResponseWriter
	n *atomic.Int64
}

func (w *gateCountingWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	return w.ResponseWriter.Write(p)
}

func gateTraceID(g, trace int) string { return fmt.Sprintf("%032x", 0xabc000+g*100+trace) }

// gateGroups: four files, one per hour. File 0 holds alpha+beta, 1 gamma+delta,
// 2 alpha+gamma, 3 epsilon. Two spans per trace, ten traces per file.
func gateGroups(base time.Time) [][]schema.TraceRow {
	spec := [][]string{{"alpha", "beta"}, {"gamma", "delta"}, {"alpha", "gamma"}, {"epsilon"}}
	groups := make([][]schema.TraceRow, len(spec))
	for g, svcs := range spec {
		for i := 0; i < 20; i++ {
			svc := svcs[(i/2)%len(svcs)]
			groups[g] = append(groups[g], schema.TraceRow{
				TimestampUnixNano: base.Add(time.Duration(g)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
				TraceID:           gateTraceID(g, i/2),
				SpanID:            fmt.Sprintf("%016x", g*100+i),
				SpanName:          []string{"GET", "PUT"}[i%2],
				ServiceName:       svc,
				Stream:            fmt.Sprintf(`{resource_attr:service.name=%q}`, svc),
				StreamID:          fmt.Sprintf("%048x", len(svc)),
				SpanAttributes:    map[string]string{"user": fmt.Sprintf("u%d", (i+g)%4)},
			})
		}
	}
	return groups
}

func newGateFx(t *testing.T, catalog bool, groups [][]schema.TraceRow) *gateFx {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	if catalog {
		s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}
	fx := &gateFx{t: t, s: s, mock: mock, rows: groups, start: 1, end: gateMaxNs}
	inner := mock.srv.Config.Handler
	mock.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "" {
			fx.gets.Add(1)
			w = &gateCountingWriter{ResponseWriter: w, n: &fx.bytes}
		}
		inner.ServeHTTP(w, r)
	})
	for _, g := range groups {
		bw.stageTraceRows(g)
		bw.flushStagedNow()
	}
	fx.files = s.manifest.GetFilesForRange(0, gateMaxNs)
	sort.Slice(fx.files, func(i, j int) bool { return fx.files[i].MinTimeNs < fx.files[j].MinTimeNs })
	if len(fx.files) != len(groups) {
		t.Fatalf("fixture: want %d files, got %d", len(groups), len(fx.files))
	}
	t.Cleanup(func() { vlapp.SetExternalStorage(nil) })
	return fx
}

func (fx *gateFx) tombstone(defs ...delete.Tombstone) {
	fx.t.Helper()
	store := delete.NewTombstoneStore()
	for _, d := range defs {
		fx.tombID++
		d.Tenants = []delete.TenantRef{{}}
		d.Mode = "hide"
		if d.ID == "" {
			d.ID = fmt.Sprintf("t%d", fx.tombID)
		}
		store.Add(d)
	}
	fx.s.SetTombstoneStore(store)
}

func allTime(query string) delete.Tombstone {
	return delete.Tombstone{Query: query, StartNs: -1 << 63, EndNs: gateMaxNs}
}

type gateRun struct {
	sum   int64
	gets  int64
	bytes int64
	meta  uint64
	rows  string
}

func (fx *gateFx) run(query string, hint bool) gateRun {
	fx.t.Helper()
	internalvlstorage.SetStorage(fx.s, nil)
	fx.s.memCache = cache.NewLRU(64 * 1024 * 1024)
	fx.s.footerCache = NewFooterCache(0)
	if fx.warmFooters {
		prefetchFooters(context.Background(), fx.s.pool, fx.files, fx.s.footerCache, 4, fx.s.footerPrefetchBytes())
	}
	q := mustParseQueryWithTime(fx.t, query, fx.start, fx.end)
	ctx := context.Background()
	if hint {
		ctx = storage.WithTimestampOnlyHint(ctx)
	}
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, nil, q, false, nil)
	getsBefore, bytesBefore, metaBefore := fx.gets.Load(), fx.bytes.Load(), metrics.MetadataOnlyFiles.Get()
	var mu sync.Mutex
	var sum int64
	var lines []string
	if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		cols := db.GetColumns(false)
		for i := 0; i < db.RowsCount(); i++ {
			var parts []string
			isCount := false
			for _, c := range cols {
				parts = append(parts, c.Name+"="+c.Values[i])
				if c.Name == "n" {
					var v int64
					_, _ = fmt.Sscan(c.Values[i], &v)
					sum += v
					isCount = true
				}
			}
			if !isCount {
				sum++
			}
			sort.Strings(parts)
			lines = append(lines, strings.Join(parts, ","))
		}
	}); err != nil {
		fx.t.Fatalf("RunQuery(%q): %v", query, err)
	}
	sort.Strings(lines)
	return gateRun{sum: sum, gets: fx.gets.Load() - getsBefore, bytes: fx.bytes.Load() - bytesBefore, meta: metrics.MetadataOnlyFiles.Get() - metaBefore, rows: strings.Join(lines, ";")}
}

func gateRowsMatching(rows [][]schema.TraceRow, keep func(r *schema.TraceRow) bool) int64 {
	var n int64
	for _, g := range rows {
		for i := range g {
			if keep(&g[i]) {
				n++
			}
		}
	}
	return n
}

func gateBase() time.Time { return time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Hour) }

// A tombstone no file holds a value of keeps the counts on the metadata path.
func TestTombstoneProof_ValueNoFileHolds_KeepsMetadataPath(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(gateBase()))
	total := gateRowsMatching(fx.rows, func(*schema.TraceRow) bool { return true })
	if base := fx.run(`* | stats count() n`, true); base.sum != total || base.gets != 0 {
		t.Fatalf("fixture: no-tombstone count = %d with %d GETs, want %d with 0", base.sum, base.gets, total)
	}
	for _, c := range []struct {
		name, q string
	}{
		{"count", `* | stats count() n`},
		{"hits", `* | stats by (_time:1h) count() n`},
	} {
		fx.tombstone()
		want := fx.run(c.q, true)
		fx.tombstone(allTime(gateSvc + `:="gone"`))
		got := fx.run(c.q, true)
		if got.rows != want.rows || got.sum != total {
			t.Errorf("%s: answer %q (sum %d), want %q (sum %d)", c.name, got.rows, got.sum, want.rows, total)
		}
		if got.gets != 0 || got.meta == 0 {
			t.Errorf("%s: %d GETs and %d metadata-answered files with a tombstone that cannot match any file, want 0 GETs and >0 files from metadata", c.name, got.gets, got.meta)
		}
	}
}

func TestTombstoneProof_TaskOlderThanFile_KeepsMetadataPath(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(gateBase()))
	total := gateRowsMatching(fx.rows, func(*schema.TraceRow) bool { return true })
	fx.tombstone(delete.Tombstone{Query: `span_attr:user:="u1"`, StartNs: -1 << 63, EndNs: fx.files[0].MinTimeNs - 1})
	if got := fx.run(`* | stats count() n`, true); got.sum != total || got.gets != 0 {
		t.Fatalf("count with a tombstone older than every file = %d with %d GETs, want %d with 0", got.sum, got.gets, total)
	}
}

// A trace delete: with pmeta the bloom facet proves every file but the one
// holding the trace clean; that file is read and its spans are not counted.
func TestTombstoneProof_TraceIDDelete_OnlyTheFileHoldingItIsRead(t *testing.T) {
	fx := newGateFx(t, true, gateGroups(gateBase()))
	total := gateRowsMatching(fx.rows, func(*schema.TraceRow) bool { return true })
	victim := gateTraceID(2, 3)
	victimSpans := gateRowsMatching(fx.rows, func(r *schema.TraceRow) bool { return r.TraceID == victim })
	if victimSpans == 0 {
		t.Fatal("fixture: the victim trace has no spans")
	}
	fx.tombstone(allTime(`trace_id:="` + victim + `"`))
	got := fx.run(`* | stats count() n`, true)
	if got.sum != total-victimSpans {
		t.Fatalf("count = %d, want %d (the trace's %d spans are deleted)", got.sum, total-victimSpans, victimSpans)
	}
	if got.meta != 3 || got.gets != 1 {
		t.Errorf("metadata-answered files = %d, GETs = %d; want 3 and 1 (only the file holding the trace is read)", got.meta, got.gets)
	}

	// A trace id nobody holds: every file is proven clean.
	fx.tombstone(allTime(`trace_id:="` + gateTraceID(9, 9) + `"`))
	if got := fx.run(`* | stats count() n`, true); got.sum != total || got.gets != 0 {
		t.Errorf("count with a trace id no file holds = %d with %d GETs, want %d with 0", got.sum, got.gets, total)
	}

	// Without pmeta there is no bloom to prove with (trace_id is not a label):
	// every file is read, and the answer is the same.
	fx.s.catalog = nil
	fx.tombstone(allTime(`trace_id:="` + victim + `"`))
	got = fx.run(`* | stats count() n`, true)
	if got.sum != total-victimSpans || got.meta != 0 {
		t.Errorf("without pmeta: count = %d from %d metadata files, want %d from none", got.sum, got.meta, total-victimSpans)
	}
}

// A tombstone that holds spans in some files: those files are read, the rest
// stay on the metadata path, and every shape equals the all-files-read answer.
func TestTombstoneProof_MixedWindow_OnlyMatchingFilesAreRead(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(gateBase()))
	alpha := func(r *schema.TraceRow) bool { return r.ServiceName == "alpha" }
	total := gateRowsMatching(fx.rows, func(*schema.TraceRow) bool { return true })
	alphaRows := gateRowsMatching(fx.rows, alpha)
	fx.tombstone(allTime(gateSvc + `:="alpha"`))
	got := fx.run(`* | stats count() n`, true)
	if got.sum != total-alphaRows {
		t.Fatalf("count = %d, want %d (alpha spans are deleted)", got.sum, total-alphaRows)
	}
	if got.meta != 2 || got.gets != 2 {
		t.Errorf("metadata-answered files = %d, GETs = %d; want 2 and 2", got.meta, got.gets)
	}
	saved := maxProofPairs
	defer func() { maxProofPairs = saved }()
	for _, c := range []struct {
		q    string
		hint bool
	}{
		{`* | stats count() n`, true},
		{`* | stats by (_time:1h) count() n`, true},
		{`name:="GET" | stats count() n`, false},
		{`span_attr:user:="u1" | stats count() n`, false},
		{`* | field_names`, false},
		{`* | fields _time, name | sort by (_time, name)`, false},
	} {
		maxProofPairs = saved
		gated := fx.run(c.q, c.hint)
		maxProofPairs = 0
		scan := fx.run(c.q, c.hint)
		if gated.rows != scan.rows {
			t.Errorf("%s:\n gated %s\n scan  %s", c.q, gated.rows, scan.rows)
		}
		if gated.gets > scan.gets {
			t.Errorf("%s: %d GETs with the proof, %d without", c.q, gated.gets, scan.gets)
		}
	}
}

// A field-value enumeration keeps the per-value aggregate for the clean files.
func TestTombstoneProof_FieldValues_AggregateKeptForCleanFiles(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(gateBase()))
	values := func() (map[string]uint64, int64) {
		fx.s.memCache = cache.NewLRU(64 * 1024 * 1024)
		fx.s.footerCache = NewFooterCache(0)
		before := fx.gets.Load()
		q := mustParseQueryWithTime(t, "*", fx.start, fx.end)
		vs, err := fx.s.GetFieldValues(context.Background(), nil, q, "resource_attr:service.name", 100)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]uint64{}
		for _, v := range vs {
			out[v.Value] = v.Hits
		}
		return out, fx.gets.Load() - before
	}
	fx.tombstone(allTime(gateSvc + `:="gone"`))
	got, gets := values()
	if len(got) != 5 || gets != 0 {
		t.Errorf("tombstone no file holds: values %v with %d GETs, want 5 services with 0 GETs", got, gets)
	}
	fx.tombstone(allTime(gateSvc + `:="alpha"`))
	got, gets = values()
	if _, ok := got["alpha"]; ok || len(got) != 4 || gets != 2 {
		t.Errorf("tombstone on alpha: values %v with %d GETs, want alpha gone, the other four, 2 GETs", got, gets)
	}
}

// A tombstone the proof clears is invisible to the reads: the same S3 reads and
// the same bytes as with no tombstone at all, for the shapes that scan files.
func TestTombstoneProof_CleanTombstoneIsInvisibleToReads(t *testing.T) {
	// Objects over 64 KiB, so a projected read is ranged: with a whole-object
	// download a column a tombstone adds to the projection would cost nothing.
	groups := gateGroups(gateBase())
	rng := rand.New(rand.NewSource(612))
	for g := range groups {
		for i, n := 0, len(groups[g]); i < n; i++ {
			for j := 0; j < 200; j++ {
				r := groups[g][i]
				r.TimestampUnixNano += int64(j+1) * int64(time.Millisecond)
				r.Body = fmt.Sprintf("%016x%016x%016x%016x", rng.Int63(), rng.Int63(), rng.Int63(), rng.Int63())
				r.SpanID = fmt.Sprintf("%016x", rng.Int63())
				groups[g] = append(groups[g], r)
			}
		}
	}
	fx := newGateFx(t, true, groups)
	fx.s.cfg.S3.WholeFileThresholdBytes = 1
	fx.warmFooters = true
	shapes := []struct {
		q    string
		hint bool
	}{
		{`name:="GET" | stats count() n`, false},
		{`* | stats by ("span_attr:user") count() n`, false},
		{`* | stats by (name) count() n`, false},
		{`* | stats by (_time:1h) count() n`, true},
		{`* | field_names`, false},
		{`name:="GET" | fields _time, name`, false},
	}
	clean := []delete.Tombstone{
		allTime(gateSvc + `:="gone"`),
		allTime(`trace_id:="` + gateTraceID(9, 9) + `"`),
		{Query: `span_attr:user:="u1"`, StartNs: -1 << 63, EndNs: fx.files[0].MinTimeNs - 1},
	}
	fx.tombstone()
	want := make([]gateRun, len(shapes))
	for i, sh := range shapes {
		fx.run(sh.q, sh.hint) // the first run learns what later runs reuse (not only the data caches)
		want[i] = fx.run(sh.q, sh.hint)
	}
	for _, ts := range clean {
		fx.tombstone(ts)
		for i, sh := range shapes {
			fx.run(sh.q, sh.hint)
			got := fx.run(sh.q, sh.hint)
			if got.rows != want[i].rows || got.gets != want[i].gets || got.bytes != want[i].bytes {
				t.Errorf("tombstone %q, %s: rows equal=%v, GETs %d (want %d), bytes %d (want %d)",
					ts.Query, sh.q, got.rows == want[i].rows, got.gets, want[i].gets, got.bytes, want[i].bytes)
			}
		}
	}
}

// A compaction output's label set is the union of its inputs' and is partial
// when an input had none, so it must not clear the file: a delete of a value
// the set lacks (but the rows hold) is still applied.
func TestTombstoneProof_CompactedLabelSetIsNotTrusted(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(gateBase()))
	m := manifest.New("test-bucket", "logs/")
	for i, fi := range fx.files {
		if i == 0 { // holds alpha and beta; the label set says alpha only
			fi.CompactionLevel = 1
			fi.Labels = map[string][]string{"service.name": {"alpha"}}
		}
		m.AddFile(manifest.ExtractTenantPartition(fi.Key), fi)
	}
	fx.s.manifest = m
	total := gateRowsMatching(fx.rows, func(*schema.TraceRow) bool { return true })
	beta := gateRowsMatching(fx.rows, func(r *schema.TraceRow) bool { return r.ServiceName == "beta" })
	fx.tombstone(allTime(gateSvc + `:="beta"`))
	if got := fx.run(`* | stats count() n`, true); got.sum != total-beta {
		t.Errorf("count = %d, want %d: the beta spans of the compacted file must stay deleted", got.sum, total-beta)
	}
}

// compactedReader plays the pods that did not compact: the writer pod compacts
// file 0 (alpha, beta; span names GET, PUT) into a compacted-L1 object whose
// label sets are the partial union of its inputs' (what an unlabelled input
// lost), folds it into its pmeta and persists the bundles. The returned reader
// pod has its own storage, its own pmeta warmed from the persisted bundles (or,
// with sharedCatalog, the writer's), and a manifest built the way a LIST builds
// it (key and size only) and enriched from pmeta. That view carries no
// compaction level, so the key is all that says "compaction output".
func compactedReader(t *testing.T, fx *gateFx, sharedCatalog bool) *gateFx {
	t.Helper()
	old := fx.files[0]
	fx.mock.mu.RLock()
	data := fx.mock.files[old.Key]
	fx.mock.mu.RUnlock()
	newKey := path.Dir(old.Key) + "/compacted-L1-readerview.parquet"
	fx.mock.putFile(newKey, data)
	out := old
	out.Key, out.CompactionLevel = newKey, 1
	out.Labels = map[string][]string{"service.name": {"alpha"}, "span.name": {"GET"}}
	fx.s.PmetaOnCompacted([]manifest.FileInfo{out}, []string{old.Key}, nil)

	reader := testStorageWithS3(t, fx.mock.url())
	reader.cfg.Mode = config.ModeTraces
	reader.catalog = fx.s.catalog
	if !sharedCatalog {
		ctx := context.Background()
		if _, err := fx.s.catalog.PersistDirty(ctx, poolObjectStore{fx.s.pool}); err != nil {
			t.Fatal(err)
		}
		reader.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		var parts []string
		for _, f := range append([]manifest.FileInfo{out}, fx.files[1:]...) {
			parts = append(parts, manifest.ExtractTenantPartition(f.Key))
		}
		if res := reader.catalog.WarmPartitions(ctx, poolObjectStore{reader.pool}, parts, 4); res.Loaded == 0 {
			t.Fatalf("no bundle loaded for the reader pod (NeedsRebuild=%v)", res.NeedsRebuild)
		}
	}
	m := manifest.New("test-bucket", "logs/")
	for _, f := range append([]manifest.FileInfo{out}, fx.files[1:]...) {
		m.AddFile(manifest.ExtractTenantPartition(f.Key), manifest.FileInfo{Key: f.Key, Size: f.Size})
	}
	if n, _ := m.EnrichFromProvider(catalogFileMetaProvider{reader.catalog}); n != 4 {
		t.Fatalf("the reader's manifest enriched %d files from pmeta, want 4", n)
	}
	reader.manifest = m
	for _, f := range m.GetFilesForRange(0, gateMaxNs) {
		if f.Key == newKey && f.CompactionLevel != 0 {
			t.Fatalf("fixture: the reader's view is expected to carry no compaction level, got %d", f.CompactionLevel)
		}
	}
	return &gateFx{t: t, s: reader, mock: fx.mock, rows: fx.rows, files: fx.files, start: fx.start, end: fx.end}
}

// A pod that did not compact never learns the compaction level of a file: its
// manifest comes from a LIST and pmeta, which carry the (partial) union label
// set but not the level. The file is still known to be a compaction output by
// its key, so a delete of a value the union lacks is applied exactly as a scan
// applies it (#621).
func TestTombstoneProof_ReaderPodOfACompactedFile(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "two_pods"
		if shared {
			name = "shared_catalog"
		}
		t.Run(name, func(t *testing.T) {
			fx := newGateFx(t, true, gateGroups(gateBase()))
			total := gateRowsMatching(fx.rows, func(*schema.TraceRow) bool { return true })
			beta := gateRowsMatching(fx.rows, func(r *schema.TraceRow) bool { return r.ServiceName == "beta" })
			put := gateRowsMatching(fx.rows, func(r *schema.TraceRow) bool { return r.SpanName == "PUT" })
			trace := gateRowsMatching(fx.rows, func(r *schema.TraceRow) bool { return r.TraceID == gateTraceID(0, 1) })
			reader := compactedReader(t, fx, shared)
			saved := maxProofPairs
			defer func() { maxProofPairs = saved }()
			for _, c := range []struct {
				tomb string
				gone int64
			}{{gateSvc + `:="beta"`, beta}, {`name:="PUT"`, put}, {`trace_id:="` + gateTraceID(0, 1) + `"`, trace}} {
				reader.tombstone(allTime(c.tomb))
				for _, sh := range []struct {
					q    string
					hint bool
				}{
					{`* | stats count() n`, true},
					{`* | stats by (name) count() n`, false},
					{`* | stats by (_time:1h) count() n`, true},
					{`* | field_names`, false},
					{`* | fields _time, name, trace_id | sort by (_time, name, trace_id)`, false},
				} {
					maxProofPairs = saved
					gated := reader.run(sh.q, sh.hint)
					maxProofPairs = 0
					scan := reader.run(sh.q, sh.hint)
					maxProofPairs = saved
					if gated.rows != scan.rows {
						t.Errorf("%s, %s:\n gated %s\n scan  %s", c.tomb, sh.q, gated.rows, scan.rows)
					}
					if sh.q == `* | stats count() n` && gated.sum != total-c.gone {
						t.Errorf("%s: count = %d, want %d", c.tomb, gated.sum, total-c.gone)
					}
				}
			}
		})
	}
}
