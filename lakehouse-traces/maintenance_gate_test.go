package main

// Twin of cmd/lakehouse-logs/maintenance_gate_test.go (spans instead of log rows).
//
// A select pod only reads. Every background job that writes to S3 or mutates
// state shared between pods (compaction and its orphan sweep, delete rewrites,
// retention, the stats snapshots and sidecar, the pmeta cleanup) runs on the
// roles all and insert only (#311: select replicas compacted the same
// partitions as the insert pod and left every row 2-4 times in the bucket).

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"
)

// bucketWriteOps are the requests that change the bucket.
var bucketWriteOps = map[string]bool{
	"PUT": true, "COPY": true, "DELETE": true, "DELETE_OBJECTS": true,
	"MPU_CREATE": true, "MPU_PART": true, "MPU_COMPLETE": true,
}

// bucketWrites returns the requests of a log that change the bucket.
func bucketWrites(reqs []fakeReq) []fakeReq {
	var out []fakeReq
	for _, r := range reqs {
		if bucketWriteOps[r.Op] {
			out = append(out, r)
		}
	}
	return out
}

// podConfig is the config of one pod over the fake bucket with every
// maintenance job switched on and ticking fast.
func podConfig(t *testing.T, role config.Role, endpoint string) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	cfg.Role = role
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = endpoint
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = t.TempDir()
	cfg.Insert.BufferDir = t.TempDir()
	cfg.Delete.CacheDir = t.TempDir()

	cfg.Compaction.Enabled = true
	cfg.Compaction.Interval = 50 * time.Millisecond
	cfg.Compaction.MinFilesL0 = 2
	cfg.Compaction.MinFilesL1 = 2
	cfg.Compaction.MinAge = 0

	cfg.Delete.Enabled = true

	cfg.Retention.Enabled = true
	cfg.Retention.Default = "720h" // 30 days: the 60-day-old seed expires, the 2-day-old one does not
	cfg.Retention.CheckInterval = "50ms"

	cfg.Stats.Enabled = true
	cfg.Stats.SnapshotPrefix = "stats"
	cfg.Stats.SnapshotInterval = 50 * time.Millisecond
	cfg.Stats.PushInterval = time.Hour
	cfg.Pmeta.Enabled = true
	cfg.Tenant.AliasSyncInterval = 50 * time.Millisecond
	return cfg
}

func newPod(t *testing.T, cfg *config.Config) *parquets3.Storage {
	t.Helper()
	store, err := parquets3.New(cfg)
	if err != nil {
		t.Fatalf("parquets3.New(role=%s): %v", cfg.Role, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// seedGroup is a number of small objects flushed into the hour partition `age` ago.
type seedGroup struct {
	files int
	age   time.Duration
}

// seedFlushes makes the insert pod flush `files` small objects of `rowsEach`
// rows into one old hour partition, one object per batch, and returns the
// number of rows written.
func seedFlushes(t *testing.T, store *parquets3.Storage, cfg *config.Config, rowsEach int, groups ...seedGroup) int {
	t.Helper()
	return seedFlushesAfter(t, store, cfg, rowsEach, 0, groups...)
}

// seedFlushesAfter is seedFlushes on a pod that already holds `existing` objects.
func seedFlushesAfter(t *testing.T, store *parquets3.Storage, cfg *config.Config, rowsEach, existing int, groups ...seedGroup) int {
	t.Helper()
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: cfg.Insert.BufferDir})
	if err != nil {
		t.Fatal(err)
	}
	store.SetLocalBuffer(segs)
	flusher := parquets3.NewBufferFlusher(store.Writer(), segs, cfg.Insert.BufferDir, nil, parquets3.BufferFlusherConfig{
		TargetBytes: 1 << 20, MaxAge: time.Millisecond, Grace: time.Minute})
	flusher.Start(10 * time.Millisecond)
	store.SetBufferFlusher(flusher)

	var ages []time.Duration
	for _, g := range groups {
		for i := 0; i < g.files; i++ {
			ages = append(ages, g.age)
		}
	}
	files := len(ages)
	rows := 0
	for f := 0; f < files; f++ {
		base := time.Now().Add(-ages[f]).Truncate(time.Hour).Add(10 * time.Minute)
		lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
		for r := 0; r < rowsEach; r++ {
			at := base.Add(time.Duration(f*rowsEach+r) * time.Second).UnixNano()
			lr.MustAdd(logstorage.TenantID{}, at, []logstorage.Field{
				{Name: "resource_attr:service.name", Value: "svc"},
				{Name: "trace_id", Value: fmt.Sprintf("t%d-%d", f, r)},
				{Name: "span_id", Value: fmt.Sprintf("s%d-%d", f, r)},
				{Name: "name", Value: "op"},
				// A span has a start and an end: the trace-by-ID index keeps them,
				// and Jaeger's lookup searches spans inside that range.
				{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", at)},
				{Name: "end_time_unix_nano", Value: fmt.Sprintf("%d", at+int64(time.Millisecond))},
			}, 1)
			rows++
		}
		segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
		deadline := time.Now().Add(20 * time.Second)
		for store.Manifest().TotalFiles() < existing+f+1 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if got := store.Manifest().TotalFiles(); got != existing+f+1 {
			t.Fatalf("after batch %d the insert pod holds %d objects, want %d", f, got, existing+f+1)
		}
	}
	return rows
}

// queryAll runs a query over every row of the seeded window on the pod.
func queryAll(t *testing.T, store *parquets3.Storage) int {
	t.Helper()
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	q.AddTimeFilter(time.Now().Add(-96*time.Hour).UnixNano(), time.Now().UnixNano())
	var mu sync.Mutex
	rows := 0
	if err := store.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		rows += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	return rows
}

// bucketRows counts the rows of every Parquet object in the bucket, and the
// objects.
func bucketRows(t *testing.T, s3 *fakeS3) (rows int64, objects int) {
	t.Helper()
	s3.mu.Lock()
	defer s3.mu.Unlock()
	for k, data := range s3.objs {
		if len(k) < 8 || k[len(k)-8:] != ".parquet" {
			continue
		}
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("open %s: %v", k, err)
		}
		rows += f.NumRows()
		objects++
	}
	return rows, objects
}

// startAllJobs starts every maintenance job of main.go on one pod, the way run()
// wires them, and returns what each returned plus a stop function.
type startedJobs struct {
	compaction  bool // the scheduler and the orphan sweep
	retention   bool
	stop        func()
	statsCancel chan struct{}
}

func startAllJobs(t *testing.T, cfg *config.Config, store *parquets3.Storage) *startedJobs {
	t.Helper()
	tombstones := delete.NewTombstoneStore()
	ret := &tombstoneRetention{}
	if err := attachTombstones(cfg, store.Pool(), tombstones, ret, fastPoll); err != nil {
		t.Fatal(err)
	}
	store.SetTombstoneStore(tombstones)
	detector := newStorageClassDetector(cfg)
	var holder *tenant.PolicyRegistry
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	registry := stats.NewTenantRegistry("test-node")
	policy, err := tenant.NewPolicyRegistry(cfg.Tenant.Overrides, resolver)
	if err != nil {
		t.Fatal(err)
	}
	holder = policy

	j := &startedJobs{statsCancel: make(chan struct{})}
	sched, sweep, stopCompaction := setupCompaction(cfg, store, nil, ":9428", &holder, detector, selfaddr.Env{})
	j.compaction = sched != nil || sweep != nil || stopCompaction != nil
	stopRetention := startRetention(cfg, store, policy)
	j.retention = stopRetention != nil
	startStatsLoops(cfg, store, registry, resolver, ":9428", j.statsCancel)
	// The alias registry's periodic sync (conditional PUTs, only for an alias
	// that was registered on this pod) and the tombstone sync, as run() starts
	// them.
	aliasReg, err := tenant.NewRegistry(resolver, tenant.RegistryConfig{
		Pool:        store.Pool(),
		Key:         cfg.AutoPrefix() + "_meta/tenant-aliases.json",
		Range:       tenant.AutoRange{Min: cfg.Tenant.AutoRegisterMinID, Max: cfg.Tenant.AutoRegisterMaxID},
		DataTenants: store.DataTenantAccountIDs,
	})
	if err != nil {
		t.Fatal(err)
	}
	startTenantAliasPersist(cfg, aliasReg, j.statsCancel)
	startTombstoneSync(tombstones, j.statsCancel)

	var once sync.Once
	j.stop = func() {
		once.Do(func() {
			close(j.statsCancel)
			if stopRetention != nil {
				stopRetention()
			}
			if stopCompaction != nil {
				stopCompaction()
			}
		})
	}
	t.Cleanup(j.stop)
	return j
}

// The jobs main.go starts, per role: a select pod starts none of them even
// though they are all enabled in its config; all and insert start all of them.
func TestMaintenanceJobs_StartedByRole(t *testing.T) {
	for _, tc := range []struct {
		role config.Role
		want bool
	}{{config.RoleAll, true}, {config.RoleInsert, true}, {config.RoleSelect, false}} {
		t.Run(string(tc.role), func(t *testing.T) {
			s3 := newFakeS3()
			defer s3.Close()
			cfg := podConfig(t, tc.role, s3.URL())
			store := newPod(t, cfg)
			j := startAllJobs(t, cfg, store)
			for name, got := range map[string]bool{
				"compaction scheduler and orphan sweep": j.compaction,
				"retention manager":                     j.retention,
			} {
				if got != tc.want {
					t.Errorf("role=%s: %s started = %v, want %v", tc.role, name, got, tc.want)
				}
			}
		})
	}
}

// writesOf formats a request log's bucket writes for a failure message.
func writesOf(w []fakeReq) []string {
	var ops []string
	for _, r := range w {
		ops = append(ops, r.Op+" "+r.Key)
	}
	return ops
}

// unsupportedOps are the requests the fake bucket did not understand: a write
// the harness cannot see would make a "zero writes" result worthless.
func unsupportedOps(reqs []fakeReq) []fakeReq {
	var out []fakeReq
	for _, r := range reqs {
		if strings.HasPrefix(r.Op, "UNSUPPORTED_") {
			out = append(out, r)
		}
	}
	return out
}

// The wiring of the real main-package functions for role=select with every job
// enabled: nothing is started and the bucket is never written, across two starts
// (a restart). The bucket holds work for every job: small objects in a two-day-old
// hour (compaction) and an object in a 60-day-old hour (retention, 30 days). A
// positive control on role=insert shows the same scenario compacts and expires,
// so the silence is the gate and not an idle bucket.
func TestSelectRole_WritesNothingToTheBucket(t *testing.T) {
	seedBucket := func() *fakeS3 {
		s3 := newFakeS3()
		t.Cleanup(s3.Close)
		seedCfg := podConfig(t, config.RoleInsert, s3.URL())
		seeder := newPod(t, seedCfg)
		seedFlushes(t, seeder, seedCfg, 5, seedGroup{files: 4, age: 48 * time.Hour}, seedGroup{files: 1, age: 60 * 24 * time.Hour})
		s3.Age(time.Hour)
		return s3
	}

	// Two starts of the same pod (a restart), each with the start-up warm-ups of
	// runStartup, the jobs of run(), a query and a close; every request of both is
	// counted on the pod's own endpoint (the seeding pod's flusher ticks are not).
	run := func(s3 *fakeS3, role config.Role) []fakeReq {
		ep := newFakeS3On(s3)
		defer ep.Close()
		cfg := podConfig(t, role, ep.URL())
		ep.Reset()
		ctx := context.Background()
		for start := 0; start < 2; start++ {
			store := newPod(t, cfg)
			if err := store.RefreshManifest(ctx); err != nil {
				t.Fatalf("refresh: %v", err)
			}
			if store.Manifest().TotalFiles() == 0 {
				t.Fatal("the pod sees no objects to maintain")
			}
			store.WarmLabelIndex(ctx)
			store.WarmCatalogFromS3(ctx)
			store.WarmMetadata(ctx)
			store.WarmCatalog(ctx)
			j := startAllJobs(t, cfg, store)
			if got := queryAll(t, store); got == 0 {
				t.Fatal("the query returned no rows: the check on the pod's requests proves nothing")
			}
			// The warm-up writes of the stats aggregate and the pmeta cleanup,
			// as runStartup() makes them.
			agg := stats.NewStatsAggregate()
			agg.Recompute(store.Manifest().AllFiles())
			saveStatsAggregate(ctx, cfg, store, agg)
			cleanupLegacyPmeta(ctx, cfg, store)
			time.Sleep(500 * time.Millisecond)
			j.stop()
			if err := store.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		}
		if bad := unsupportedOps(ep.Requests()); len(bad) != 0 {
			t.Fatalf("the fake bucket did not understand %d requests (first: %s %s): the write count is not trustworthy", len(bad), bad[0].Op, bad[0].Key)
		}
		return bucketWrites(ep.Requests())
	}

	if w := run(seedBucket(), config.RoleSelect); len(w) != 0 {
		t.Fatalf("role=select wrote to the bucket %d times: %v", len(w), writesOf(w))
	}

	// Control: the same scenario on an insert pod, on a fresh bucket (the select
	// run above must not have consumed the work).
	w := run(seedBucket(), config.RoleInsert)
	oldHour := time.Now().Add(-60 * 24 * time.Hour).UTC().Format("dt=2006-01-02")
	var compacted, expired, snapshot bool
	for _, r := range w {
		switch {
		case r.Op == "PUT" && strings.Contains(r.Key, "compacted-"):
			compacted = true
		case r.Op == "DELETE" && strings.HasSuffix(r.Key, ".parquet") && strings.Contains(r.Key, oldHour):
			expired = true
		case r.Op == "PUT" && strings.HasSuffix(r.Key, "stats/snapshot.json"):
			snapshot = true
		}
	}
	if !compacted || !expired || !snapshot {
		t.Errorf("control: the insert pod compacted=%v deleted the expired object=%v wrote the stats snapshot=%v; the select check above proves nothing (writes: %v)", compacted, expired, snapshot, writesOf(w))
	}
}

// One insert pod and two select pods over one bucket, compaction enabled on
// every one of them: exactly one pod writes, and no row is duplicated. The pods
// are in-process Storage instances over one shared fake bucket, each with its
// own endpoint so every pod's requests are counted on their own. The bucket
// holds a delete of the compaction inputs back for a moment, so that every pod
// that compacts has uploaded its output before any input is gone (the real
// window of #311 on a loaded bucket).
func TestMultiInstance_OneInsertTwoSelect_OnlyTheWriterCompacts(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	bucket.HoldDeletes(400 * time.Millisecond)
	selA, selB := newFakeS3On(bucket), newFakeS3On(bucket)
	defer selA.Close()
	defer selB.Close()

	insCfg := podConfig(t, config.RoleInsert, bucket.URL())
	insCfg.Retention.Enabled = false
	ins := newPod(t, insCfg)
	wantRows := seedFlushes(t, ins, insCfg, 5, seedGroup{files: 6, age: 48 * time.Hour})
	_, before := bucketRows(t, bucket)
	if before != 6 {
		t.Fatalf("seed: %d objects, want 6", before)
	}
	// The insert buffer's segment guard keeps compaction off objects younger
	// than twice the buffer grace; the seed is as old as a settled bucket.
	bucket.Age(time.Hour)

	type pod struct {
		name  string
		cfg   *config.Config
		store *parquets3.Storage
		s3    *fakeS3
	}
	pods := []pod{{"insert", insCfg, ins, bucket}}
	for i, ep := range []*fakeS3{selA, selB} {
		cfg := podConfig(t, config.RoleSelect, ep.URL())
		cfg.Retention.Enabled = false
		s := newPod(t, cfg)
		if err := s.RefreshManifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := s.Manifest().TotalFiles(); got != 6 {
			t.Fatalf("select pod %d sees %d objects, want 6", i, got)
		}
		pods = append(pods, pod{fmt.Sprintf("select-%d", i), cfg, s, ep})
	}
	for _, p := range pods {
		p.s3.Reset()
	}

	// The insert pod's discovery resolves the writer pods alone (select.insert_headless_service),
	// which is the ring its compaction ownership hashes over.
	ins.SetWriterPeers([]string{"insert:9428"})

	// Every pod runs main.go's compaction wiring with compaction enabled.
	var stops []func()
	for _, p := range pods {
		var holder *tenant.PolicyRegistry
		_, _, stop := setupCompaction(p.cfg, p.store, nil, p.name+":9428", &holder, newStorageClassDetector(p.cfg), selfaddr.Env{})
		if stop != nil {
			stops = append(stops, stop)
		}
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	// Wait until the writer's compaction has settled: one object left.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, n := bucketRows(t, bucket); n == 1 {
			break
		}
		// The flusher commits a segment (writes its marker) a moment after the
		// object lands, so the last marker can appear after the aging above.
		bucket.AgePrefix("_segments/", time.Hour)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(1200 * time.Millisecond) // any late publish of another pod would land here

	for _, p := range pods {
		if bad := unsupportedOps(p.s3.Requests()); len(bad) != 0 {
			t.Fatalf("%s: the fake bucket did not understand %d requests (first: %s %s): the write counts are not trustworthy", p.name, len(bad), bad[0].Op, bad[0].Key)
		}
	}
	for _, p := range pods[1:] {
		if w := bucketWrites(p.s3.Requests()); len(w) != 0 {
			t.Errorf("%s wrote to the bucket %d times (first: %s %s); a select pod never compacts", p.name, len(w), w[0].Op, w[0].Key)
		}
	}
	if w := bucketWrites(pods[0].s3.Requests()); len(w) == 0 {
		t.Error("the insert pod did not compact; the check above proves nothing")
	}
	// The readers see every row once: a refresh, then a query on each select pod.
	for _, p := range pods[1:] {
		if err := p.store.RefreshManifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := queryAll(t, p.store); got != wantRows {
			t.Errorf("a query on %s returns %d rows, want %d (every row once)", p.name, got, wantRows)
		}
	}
	rows, objects := bucketRows(t, bucket)
	if rows != int64(wantRows) {
		t.Errorf("the bucket holds %d rows in %d objects, want %d rows once (compaction must not duplicate rows)", rows, objects, wantRows)
	}
	if objects != 1 {
		bucket.mu.Lock()
		var keys []string
		for k := range bucket.objs {
			if strings.HasSuffix(k, ".parquet") {
				keys = append(keys, k)
			}
		}
		bucket.mu.Unlock()
		t.Errorf("the bucket holds %d objects after compaction, want 1: %v", objects, keys)
	}
}

// ownedBy counts the partitions the resolver says this pod owns.
func ownedBy(res *compaction.OwnershipResolver, partitions []string) int {
	n := 0
	for _, p := range partitions {
		if res.OwnsPartition(p) {
			n++
		}
	}
	return n
}

// Select pods sit in the same peer ring as the insert pods (the ring comes from one
// headless service the chart gives both roles). Hashing over that ring would hand
// a share of the partitions to select pods, which never compact them. The ring
// compaction hashes over is the writer pods alone.
func TestCompactionOwnership_RingIsTheWritersOnly(t *testing.T) {
	var partitions []string
	for d := 1; d <= 20; d++ {
		for h := 0; h < 10; h++ {
			partitions = append(partitions, fmt.Sprintf("0/0/%s/dt=2026-10-%02d/hour=%02d", "traces", d, h))
		}
	}
	settled := func(pc *peercache.PeerCache) { time.Sleep(15 * time.Millisecond); _ = pc }
	newRing := func(members ...string) *peercache.PeerCache {
		pc := peercache.NewWithStabilize(":9428", "k", time.Second, 1, time.Millisecond)
		pc.UpdatePeers(members)
		settled(pc)
		return pc
	}

	pc := newRing("sel0:9428", "sel1:9428", "ins0:9428")
	writers := func() []string { return []string{"ins0:9428"} }
	own := newCompactionOwnership("ins0:9428", pc, writers, selfaddr.Env{}, true)
	if got := ownedBy(own, partitions); got != len(partitions) {
		t.Fatalf("the only writer owns %d of %d partitions with two select pods in the ring, want all", got, len(partitions))
	}
	// A select pod joins and another leaves: who compacts does not change.
	pc.UpdatePeers([]string{"sel1:9428", "sel2:9428", "sel3:9428", "ins0:9428"})
	settled(pc)
	if got := ownedBy(own, partitions); got != len(partitions) {
		t.Fatalf("after the select pods changed the only writer owns %d of %d partitions, want all", got, len(partitions))
	}

	// Without a writer list the ring is the whole peer ring and a share of the
	// partitions belongs to select pods: this is the gap the writer list closes.
	whole := newCompactionOwnership("ins0:9428", newRing("sel0:9428", "sel1:9428", "ins0:9428"), nil, selfaddr.Env{}, true)
	if got := ownedBy(whole, partitions); got == 0 || got == len(partitions) {
		t.Fatalf("with the whole ring the insert pod owns %d of %d partitions, want a share (the premise of the test)", got, len(partitions))
	}

	// Two writers and selects in the ring: every partition has exactly one owner
	// among the writers.
	pc2 := newRing("sel0:9428", "sel1:9428", "ins0:9428", "ins1:9428")
	two := func() []string { return []string{"ins0:9428", "ins1:9428"} }
	a := ownedBy(newCompactionOwnership("ins0:9428", pc2, two, selfaddr.Env{}, true), partitions)
	b := ownedBy(newCompactionOwnership("ins1:9428", pc2, two, selfaddr.Env{}, true), partitions)
	if a == 0 || b == 0 || a+b != len(partitions) {
		t.Fatalf("two writers own %d + %d of %d partitions, want a split that covers every partition once", a, b, len(partitions))
	}
}

// The shutdown of a select pod writes nothing either: no stats snapshot, no
// tombstone object that is not owed, no pmeta bundle.
func TestSelectRole_ShutdownWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		role  config.Role
		wants bool
	}{{config.RoleSelect, false}, {config.RoleInsert, true}} {
		t.Run(string(tc.role), func(t *testing.T) {
			s3 := newFakeS3()
			defer s3.Close()
			cfg := podConfig(t, tc.role, s3.URL())
			cfg.Shutdown.PersistTimeout = 5 * time.Second
			store := newPod(t, cfg)
			registry := stats.NewTenantRegistry("test-node")
			registry.RecordWrite("0:0", 10, 100, 1, "STANDARD")
			// runShutdown stops the HTTP server first; give it one to stop.
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := l.Addr().String()
			_ = l.Close()
			httpserver.Serve([]string{addr}, func(w http.ResponseWriter, r *http.Request) bool { return false }, httpserver.ServeOptions{})
			for i := 0; i < 200; i++ { // Serve starts the server in a goroutine: wait until it answers
				if resp, err := http.Get("http://" + addr + "/health"); err == nil {
					_ = resp.Body.Close()
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			// What run() initialises for the role.
			if cfg.InsertEnabled() {
				vtinsert.Init()
			}
			if cfg.SelectEnabled() {
				internalselect.Init()
			}
			s3.Reset()
			runShutdown(cfg, addr, store, registry, nil)
			w := bucketWrites(s3.Requests())
			snapshot := false
			for _, r := range w {
				if strings.HasSuffix(r.Key, "stats/snapshot.json") {
					snapshot = true
				}
			}
			if tc.wants != snapshot || (!tc.wants && len(w) != 0) {
				t.Fatalf("role=%s: shutdown wrote the stats snapshot=%v, writes=%v; want snapshot=%v and, for select, no write at all", tc.role, snapshot, writesOf(w), tc.wants)
			}
		})
	}
}

// run() starts the tombstone sync on every node, as a top-level statement: a
// condition around it would leave a class of nodes (a select pod) enforcing a
// view of the deletes that never refreshes.
func TestRun_StartsTheTombstoneSyncOnEveryNode(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "run" {
			continue
		}
		for _, st := range fd.Body.List {
			if es, ok := st.(*ast.ExprStmt); ok {
				if call, ok := es.X.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "startTombstoneSync" {
						found = true
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("run() has no top-level call to startTombstoneSync")
	}
}
