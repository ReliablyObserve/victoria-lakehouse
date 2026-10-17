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
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
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
	cfg.Delete.PersistPath = t.TempDir()

	cfg.Compaction.Enabled = true
	cfg.Compaction.Interval = 50 * time.Millisecond
	cfg.Compaction.MinFilesL0 = 2
	cfg.Compaction.MinFilesL1 = 2
	cfg.Compaction.MinAge = 0

	cfg.Delete.Enabled = true
	cfg.Delete.VerifyInterval = 50 * time.Millisecond

	cfg.Retention.Enabled = true
	cfg.Retention.Default = "1h" // the seeded data is two days old
	cfg.Retention.CheckInterval = "50ms"

	cfg.Stats.Enabled = true
	cfg.Stats.SnapshotPrefix = "stats"
	cfg.Stats.SnapshotInterval = 50 * time.Millisecond
	cfg.Stats.PushInterval = time.Hour
	cfg.Pmeta.Enabled = true
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

// seedFlushes makes the insert pod flush `files` small objects of `rowsEach`
// rows into one old hour partition, one object per batch, and returns the
// number of rows written.
func seedFlushes(t *testing.T, store *parquets3.Storage, cfg *config.Config, files, rowsEach int) int {
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

	base := time.Now().Add(-48 * time.Hour).Truncate(time.Hour).Add(10 * time.Minute)
	rows := 0
	for f := 0; f < files; f++ {
		lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
		for r := 0; r < rowsEach; r++ {
			lr.MustAdd(logstorage.TenantID{}, base.Add(time.Duration(f*rowsEach+r)*time.Second).UnixNano(), []logstorage.Field{
				{Name: "resource_attr:service.name", Value: "svc"},
				{Name: "trace_id", Value: fmt.Sprintf("t%d-%d", f, r)},
				{Name: "span_id", Value: fmt.Sprintf("s%d-%d", f, r)},
				{Name: "name", Value: "op"},
			}, 1)
			rows++
		}
		segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
		deadline := time.Now().Add(20 * time.Second)
		for store.Manifest().TotalFiles() < f+1 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if got := store.Manifest().TotalFiles(); got != f+1 {
			t.Fatalf("after batch %d the insert pod holds %d objects, want %d", f, got, f+1)
		}
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
	deleteRew   bool
	retention   bool
	stop        func()
	statsCancel chan struct{}
}

func startAllJobs(t *testing.T, cfg *config.Config, store *parquets3.Storage) *startedJobs {
	t.Helper()
	tombstones := delete.NewTombstoneStore()
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
	sched, sweep, stopCompaction := setupCompaction(cfg, store, nil, ":9428", &holder, tombstones, detector)
	j.compaction = sched != nil || sweep != nil || stopCompaction != nil
	rewrite := startDeleteRewrite(cfg, store, nil, tombstones, detector)
	j.deleteRew = rewrite != nil
	stopRetention := startRetention(cfg, store, policy)
	j.retention = stopRetention != nil
	startStatsLoops(cfg, store, registry, resolver, ":9428", j.statsCancel)

	var once sync.Once
	j.stop = func() {
		once.Do(func() {
			close(j.statsCancel)
			if stopRetention != nil {
				stopRetention()
			}
			if rewrite != nil {
				rewrite.Stop()
			}
			if stopCompaction != nil {
				stopCompaction()
			}
		})
	}
	t.Cleanup(j.stop)
	return j
}

// The predicate itself, for every role.
func TestRunsMaintenance_ByRole(t *testing.T) {
	for _, tc := range []struct {
		role config.Role
		want bool
	}{{config.RoleAll, true}, {config.RoleInsert, true}, {config.RoleSelect, false}} {
		cfg := config.Default()
		cfg.Role = tc.role
		if got := cfg.RunsMaintenance(); got != tc.want {
			t.Errorf("role=%s RunsMaintenance() = %v, want %v", tc.role, got, tc.want)
		}
	}
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
				"delete rewrite scheduler":              j.deleteRew,
				"retention manager":                     j.retention,
			} {
				if got != tc.want {
					t.Errorf("role=%s: %s started = %v, want %v", tc.role, name, got, tc.want)
				}
			}
		})
	}
}

// The wiring of the real main-package functions for role=select with every job
// enabled: nothing is started and the bucket is never written. A positive
// control on role=insert shows the same scenario does write (compaction,
// retention deletes, stats snapshots), so the silence is the gate and not an
// idle bucket.
func TestSelectRole_WritesNothingToTheBucket(t *testing.T) {
	s3 := newFakeS3()
	defer s3.Close()

	// A bucket with work for every job: small objects to compact, old
	// objects retention would drop, and no stats snapshot yet.
	seedCfg := podConfig(t, config.RoleInsert, s3.URL())
	seeder := newPod(t, seedCfg)
	seedFlushes(t, seeder, seedCfg, 4, 5)

	run := func(role config.Role) []fakeReq {
		cfg := podConfig(t, role, s3.URL())
		store := newPod(t, cfg)
		if err := store.RefreshManifest(context.Background()); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		if store.Manifest().TotalFiles() == 0 {
			t.Fatal("the pod sees no objects to maintain")
		}
		s3.Reset()
		j := startAllJobs(t, cfg, store)
		// The shutdown and warm-up writes of the stats aggregate and the pmeta
		// cleanup, as run()/runStartup() make them.
		agg := stats.NewStatsAggregate()
		agg.Recompute(store.Manifest().AllFiles())
		saveStatsAggregate(context.Background(), cfg, store, agg)
		cleanupLegacyPmeta(context.Background(), cfg, store)
		time.Sleep(800 * time.Millisecond)
		j.stop()
		return bucketWrites(s3.Requests())
	}

	if w := run(config.RoleSelect); len(w) != 0 {
		var ops []string
		for _, r := range w {
			ops = append(ops, r.Op+" "+r.Key)
		}
		t.Fatalf("role=select wrote to the bucket %d times: %v", len(w), ops)
	}

	// Control: the same scenario on an insert pod. A fresh bucket again, because
	// the select run above must not have consumed the work.
	s3b := newFakeS3()
	defer s3b.Close()
	seedCfgB := podConfig(t, config.RoleInsert, s3b.URL())
	seederB := newPod(t, seedCfgB)
	seedFlushes(t, seederB, seedCfgB, 4, 5)
	cfg := podConfig(t, config.RoleInsert, s3b.URL())
	store := newPod(t, cfg)
	if err := store.RefreshManifest(context.Background()); err != nil {
		t.Fatal(err)
	}
	s3b.Reset()
	j := startAllJobs(t, cfg, store)
	agg := stats.NewStatsAggregate()
	agg.Recompute(store.Manifest().AllFiles())
	saveStatsAggregate(context.Background(), cfg, store, agg)
	time.Sleep(800 * time.Millisecond)
	j.stop()
	got := map[string]bool{}
	for _, r := range bucketWrites(s3b.Requests()) {
		got[classify(r.Key)] = true
	}
	for _, want := range []string{"data", "sidecar"} {
		if !got[want] {
			t.Errorf("control: the insert pod did not write a %q object (writes by class: %v); the select check above proves nothing", want, got)
		}
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
	wantRows := seedFlushes(t, ins, insCfg, 6, 5)
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

	// Every pod runs main.go's compaction wiring with compaction enabled.
	var stops []func()
	for _, p := range pods {
		var holder *tenant.PolicyRegistry
		_, _, stop := setupCompaction(p.cfg, p.store, nil, p.name+":9428", &holder, delete.NewTombstoneStore(), newStorageClassDetector(p.cfg))
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
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(1200 * time.Millisecond) // any late publish of another pod would land here

	for _, p := range pods[1:] {
		if w := bucketWrites(p.s3.Requests()); len(w) != 0 {
			t.Errorf("%s wrote to the bucket %d times (first: %s %s); a select pod never compacts", p.name, len(w), w[0].Op, w[0].Key)
		}
	}
	if w := bucketWrites(pods[0].s3.Requests()); len(w) == 0 {
		t.Error("the insert pod did not compact; the check above proves nothing")
	}
	rows, objects := bucketRows(t, bucket)
	if rows != int64(wantRows) {
		t.Errorf("the bucket holds %d rows in %d objects, want %d rows once (compaction must not duplicate rows)", rows, objects, wantRows)
	}
	if objects != 1 {
		t.Errorf("the bucket holds %d objects after compaction, want 1", objects)
	}
}
