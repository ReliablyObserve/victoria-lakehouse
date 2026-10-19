package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"sort"
	"strings"
	"sync"
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
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// Per-file tombstone gating (#612): a tombstone that provably cannot match a
// file leaves that file on the metadata fast paths; a file it may match is read
// and its deleted rows are excluded. These tests use only behaviour (answers
// and the S3 reads the mock sees), so they also compile against main, where the
// "stays on the fast path" tests fail.

const gateMaxNs = int64(1<<63 - 1)

type gateFx struct {
	t      *testing.T
	s      *Storage
	mock   *mockS3Server
	rows   [][]schema.LogRow // rows[i] is the file flushed from group i
	files  []manifest.FileInfo
	start  int64
	end    int64
	tombID int
	// warmFooters caches every footer before a run, so reads are ranged and
	// column-projected (a cold footer makes a small object a whole download).
	warmFooters bool
}

// gateGroups: four files, one per hour. File 0 holds alpha+beta, 1 gamma+delta,
// 2 alpha+gamma, 3 epsilon. Every row carries a map attribute user=u<n>.
func gateGroups(base time.Time) [][]schema.LogRow {
	spec := [][]string{{"alpha", "beta"}, {"gamma", "delta"}, {"alpha", "gamma"}, {"epsilon"}}
	groups := make([][]schema.LogRow, len(spec))
	for g, svcs := range spec {
		for i := 0; i < 20; i++ {
			svc := svcs[i%len(svcs)]
			groups[g] = append(groups[g], schema.LogRow{
				TimestampUnixNano: base.Add(time.Duration(g)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
				Body:              fmt.Sprintf("msg %d g%d", i, g),
				SeverityText:      []string{"INFO", "ERROR"}[i%2],
				ServiceName:       svc,
				Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
				StreamID:          fmt.Sprintf("%048x", len(svc)),
				LogAttributes:     map[string]string{"user": fmt.Sprintf("u%d", (i+g)%4)},
			})
		}
	}
	return groups
}

// newGateFx flushes one file per group through the production writer, with the
// pmeta catalog (and so the bloom facet) when catalog is set.
func newGateFx(t *testing.T, catalog bool, groups [][]schema.LogRow) *gateFx {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	if catalog {
		s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}
	fx := &gateFx{t: t, s: s, mock: mock, rows: groups, start: 1, end: gateMaxNs}
	for _, g := range groups {
		bw.stageLogRows(g)
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

// tombstone installs exactly the given tombstones for tenant 0:0.
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

// allTime is a tombstone as an upstream delete task is: no lower bound.
func allTime(query string) delete.Tombstone {
	return delete.Tombstone{Query: query, StartNs: -1 << 63, EndNs: gateMaxNs}
}

type gateRun struct {
	sum   int64 // the sum of column n, or the number of rows when there is none
	gets  int64 // S3 object reads
	bytes int64 // bytes those reads returned
	meta  uint64
	rows  string // canonical fingerprint
}

func (fx *gateFx) run(query string, hint bool) gateRun {
	fx.t.Helper()
	internalvlstorage.SetStorage(fx.s, nil)
	// Cold caches, so the GETs of one run do not hide the GETs of the next.
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
	getsBefore, bytesBefore, metaBefore := fx.mock.gets.Load(), fx.mock.bytesServed.Load(), metrics.MetadataOnlyFiles.Get()
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
	return gateRun{sum: sum, gets: fx.mock.gets.Load() - getsBefore, bytes: fx.mock.bytesServed.Load() - bytesBefore, meta: metrics.MetadataOnlyFiles.Get() - metaBefore, rows: strings.Join(lines, ";")}
}

func gateRowsMatching(rows [][]schema.LogRow, keep func(r *schema.LogRow) bool) int64 {
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

// A tombstone no file holds a value of keeps every count on the metadata path:
// no S3 read, the answer equals the unfiltered total.
func TestTombstoneProof_ValueNoFileHolds_KeepsMetadataPath(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
	total := gateRowsMatching(fx.rows, func(*schema.LogRow) bool { return true })

	base := fx.run(`* | stats count() n`, true)
	if base.sum != total || base.gets != 0 {
		t.Fatalf("fixture: no-tombstone count = %d with %d GETs, want %d with 0", base.sum, base.gets, total)
	}
	fx.tombstone(allTime(`service.name:="gone"`))
	for _, c := range []struct {
		name, q string
		hint    bool
	}{
		{"count", `* | stats count() n`, true},
		{"hits", `* | stats by (_time:1h) count() n`, true},
		{"count by service", `* | stats by (service.name) count() n`, false},
	} {
		got := fx.run(c.q, c.hint)
		fx.tombstone() // the same query with no tombstone is the reference
		want := fx.run(c.q, c.hint)
		fx.tombstone(allTime(`service.name:="gone"`))
		if got.rows != want.rows || got.sum != total {
			t.Errorf("%s: with a tombstone no file holds, answer %q (sum %d), want %q (sum %d)", c.name, got.rows, got.sum, want.rows, total)
		}
		if got.gets != 0 || got.meta == 0 {
			t.Errorf("%s: %d GETs and %d metadata-answered files with a tombstone that cannot match any file, want 0 GETs and >0 files from metadata", c.name, got.gets, got.meta)
		}
	}
}

// A tombstone whose range ends before the oldest file's rows cannot match any
// of them, so the time proof alone keeps the fast path.
func TestTombstoneProof_TaskOlderThanFile_KeepsMetadataPath(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
	total := gateRowsMatching(fx.rows, func(*schema.LogRow) bool { return true })
	older := delete.Tombstone{Query: `user:="u1"`, StartNs: -1 << 63, EndNs: fx.files[0].MinTimeNs - 1}
	fx.tombstone(older)
	if got := fx.run(`* | stats count() n`, true); got.sum != total || got.gets != 0 {
		t.Fatalf("count with a tombstone older than every file = %d with %d GETs, want %d with 0", got.sum, got.gets, total)
	}
}

// A tombstone that holds rows in some files: those files are read and the
// deleted rows are excluded; the other files stay on the metadata path.
func TestTombstoneProof_MixedWindow_OnlyMatchingFilesAreRead(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
	alpha := func(r *schema.LogRow) bool { return r.ServiceName == "alpha" }
	total := gateRowsMatching(fx.rows, func(*schema.LogRow) bool { return true })
	alphaRows := gateRowsMatching(fx.rows, alpha)
	fx.tombstone(allTime(`service.name:="alpha"`))

	got := fx.run(`* | stats count() n`, true)
	if got.sum != total-alphaRows {
		t.Fatalf("count = %d, want %d (alpha rows are deleted)", got.sum, total-alphaRows)
	}
	// Files 1 and 3 hold no alpha: they are answered from metadata. Files 0 and
	// 2 are read (one object read each), and their alpha rows are not counted.
	if got.meta != 2 || got.gets != 2 {
		t.Errorf("metadata-answered files = %d, GETs = %d; want 2 and 2 (only the files holding alpha are read)", got.meta, got.gets)
	}
	// Every shape equals the answer with every file read with its tombstones.
	saved := maxProofPairs
	defer func() { maxProofPairs = saved }()
	for _, c := range []struct {
		q    string
		hint bool
	}{
		{`* | stats count() n`, true},
		{`* | stats by (_time:1h) count() n`, true},
		{`* | stats by (service.name) count() n`, false},
		{`service.name:="alpha" | stats count() n`, false},
		{`service.name:="gamma" | stats count() n`, false},
		{`user:="u1" | stats count() n`, false},
		{`* | field_names`, false},
		{`* | fields _time, service.name | sort by (_time, service.name)`, false},
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

// field_names with a tombstone no file holds equals the no-tombstone answer.
func TestTombstoneProof_FieldNamesAndFiltered_EqualScan(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
	queries := []string{`service.name:="gamma" | stats count() n`, `level:="ERROR" | stats count() n`, `* | field_names`}
	want := map[string]gateRun{}
	for _, q := range queries {
		want[q] = fx.run(q, false)
	}
	fx.tombstone(allTime(`service.name:="gone"`))
	for _, q := range queries {
		if got := fx.run(q, false); got.rows != want[q].rows {
			t.Errorf("%s: answer %q, want %q", q, got.rows, want[q].rows)
		}
	}
}

// A field-value enumeration answers a file from its per-value aggregate
// (no S3 read) unless a tombstone may match the file; the counts it returns
// exclude the deleted rows either way.
func TestTombstoneProof_FieldValues_AggregateKeptForCleanFiles(t *testing.T) {
	fx := newGateFx(t, false, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
	values := func() (map[string]uint64, int64) {
		fx.s.memCache = cache.NewLRU(64 * 1024 * 1024)
		fx.s.footerCache = NewFooterCache(0)
		before := fx.mock.gets.Load()
		q := mustParseQueryWithTime(t, "*", fx.start, fx.end)
		vs, err := fx.s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]uint64{}
		for _, v := range vs {
			out[v.Value] = v.Hits
		}
		return out, fx.mock.gets.Load() - before
	}
	alphaRows := uint64(gateRowsMatching(fx.rows, func(r *schema.LogRow) bool { return r.ServiceName == "alpha" }))
	allAlpha := alphaRows

	fx.tombstone(allTime(`service.name:="gone"`))
	got, gets := values()
	if got["alpha"] != allAlpha || len(got) != 5 || gets != 0 {
		t.Errorf("tombstone no file holds: values %v with %d GETs, want all 5 services untouched with 0 GETs", got, gets)
	}
	fx.tombstone(allTime(`service.name:="alpha"`))
	got, gets = values()
	if _, ok := got["alpha"]; ok || len(got) != 4 || got["beta"] == 0 {
		t.Errorf("tombstone on alpha: values %v, want alpha gone and the other four untouched", got)
	}
	if gets != 2 {
		t.Errorf("tombstone on alpha: %d GETs, want 2 (only the two files holding alpha are scanned)", gets)
	}
}

// A tombstone the proof clears is invisible to the reads: the same S3 reads and
// the same bytes as with no tombstone at all, for the shapes that scan files
// (a tombstone's own columns are not projected, no row filter is added).
func TestTombstoneProof_CleanTombstoneIsInvisibleToReads(t *testing.T) {
	// Objects over 64 KiB, so a projected read is ranged: with a whole-object
	// download a column a tombstone adds to the projection would cost nothing.
	groups := gateGroups(time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Hour))
	rng := rand.New(rand.NewSource(612))
	for g := range groups {
		for i, n := 0, len(groups[g]); i < n; i++ {
			for j := 0; j < 200; j++ {
				r := groups[g][i]
				r.TimestampUnixNano += int64(j+1) * int64(time.Millisecond)
				r.Body = fmt.Sprintf("%016x%016x%016x%016x", rng.Int63(), rng.Int63(), rng.Int63(), rng.Int63())
				groups[g] = append(groups[g], r)
			}
		}
	}
	fx := newGateFx(t, false, groups)
	fx.s.cfg.S3.WholeFileThresholdBytes = 1
	fx.warmFooters = true
	shapes := []struct {
		q    string
		hint bool
	}{
		{`service.name:="gamma" | stats count() n`, false},
		{`* | stats by (user) count() n`, false},
		{`* | stats by (_time:1h) count() n`, true},
		{`* | field_names`, false},
		{`service.name:="gamma" | fields _time, service.name`, false},
	}
	clean := []delete.Tombstone{
		allTime(`service.name:="gone"`),
		allTime(`user:="u9" level:="WARN"`),
		{Query: `user:="u1"`, StartNs: -1 << 63, EndNs: fx.files[0].MinTimeNs - 1},
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
	fx := newGateFx(t, false, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
	m := manifest.New("test-bucket", "logs/")
	for i, fi := range fx.files {
		if i == 0 { // holds alpha and beta; the label set says alpha only
			fi.CompactionLevel = 1
			fi.Labels = map[string][]string{"service.name": {"alpha"}}
		}
		m.AddFile(partitionFromKey(fi.Key), fi)
	}
	fx.s.manifest = m
	total := gateRowsMatching(fx.rows, func(*schema.LogRow) bool { return true })
	beta := gateRowsMatching(fx.rows, func(r *schema.LogRow) bool { return r.ServiceName == "beta" })
	fx.tombstone(allTime(`service.name:="beta"`))
	for _, c := range []struct {
		q    string
		hint bool
	}{{`* | stats count() n`, true}, {`* | stats by (service.name) count() n`, false}} {
		if got := fx.run(c.q, c.hint); got.sum != total-beta {
			t.Errorf("%s = %d, want %d: the beta rows of the compacted file must stay deleted", c.q, got.sum, total-beta)
		}
	}
}

// compactedReader plays the pods that did not compact: the writer pod compacts
// file 0 (alpha, beta) into a compacted-L1 object whose label set is the
// partial union of its inputs' (alpha only; the part of the union an unlabelled
// input lost), folds it into its pmeta, and persists the bundles. The returned
// reader pod has its own storage, its own pmeta warmed from the persisted
// bundles (or, with sharedCatalog, the writer's), and a manifest built the way
// a LIST builds it (key and size only) and enriched from pmeta. That view
// carries no compaction level, so the key is all that says "compaction output".
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
	out.Labels = map[string][]string{"service.name": {"alpha"}, "severity_text": {"INFO"}}
	fx.s.PmetaOnCompacted([]manifest.FileInfo{out}, []string{old.Key}, nil)

	reader := testStorageWithS3(t, fx.mock.url())
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
		m.AddFile(partitionFromKey(f.Key), manifest.FileInfo{Key: f.Key, Size: f.Size})
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
			fx := newGateFx(t, true, gateGroups(time.Now().UTC().Add(-6*time.Hour).Truncate(time.Hour)))
			total := gateRowsMatching(fx.rows, func(*schema.LogRow) bool { return true })
			beta := gateRowsMatching(fx.rows, func(r *schema.LogRow) bool { return r.ServiceName == "beta" })
			errs := gateRowsMatching(fx.rows, func(r *schema.LogRow) bool { return r.SeverityText == "ERROR" })
			reader := compactedReader(t, fx, shared)
			saved := maxProofPairs
			defer func() { maxProofPairs = saved }()
			for _, c := range []struct {
				tomb string
				gone int64
			}{{`service.name:="beta"`, beta}, {`level:="ERROR"`, errs}} {
				reader.tombstone(allTime(c.tomb))
				for _, sh := range []struct {
					q    string
					hint bool
				}{
					{`* | stats count() n`, true},
					{`* | stats by (service.name) count() n`, false},
					{`* | stats by (_time:1h) count() n`, true},
					{`* | field_names`, false},
					{`* | fields _time, service.name, level | sort by (_time, service.name, level)`, false},
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
