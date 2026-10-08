package compaction

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Issue #343: compaction counted files across ALL tenants of a partition while
// the compactor merges per tenant. These tests run the scheduler with the
// shipped defaults (MinFiles 10/10, MinAge 1h, DailyRollupAge 24h,
// MaxConcurrent 1, fair-share 1) in both modes.

const planFP = "fp-current"

// planWorld is a manifest + pool seeded with real Parquet objects laid out the
// way the writer lays them out: <acct>/<proj>/<mode>/dt=…/hour=…/<file>.
type planWorld struct {
	t    *testing.T
	mode config.Mode
	pool *mockPool
	m    *manifest.Manifest
	seq  int
}

func newPlanWorld(t *testing.T, mode config.Mode) *planWorld {
	t.Helper()
	return &planWorld{t: t, mode: mode, pool: newMockPool(), m: manifest.New("test-bucket", string(mode)+"/")}
}

// partitionAt returns the hour partition key of a time.
func partitionAt(ts time.Time) string {
	ts = ts.UTC()
	return fmt.Sprintf("dt=%s/hour=%02d", ts.Format("2006-01-02"), ts.Hour())
}

// add seeds n objects for tenant (acct/proj) in partition at level, rows rows
// each, and returns their keys.
func (w *planWorld) add(tenant, partition string, level, n, rows int, mutate func(*manifest.FileInfo)) []string {
	w.t.Helper()
	pt, err := manifest.ParsePartitionTime(partition)
	if err != nil {
		w.t.Fatal(err)
	}
	var keys []string
	for i := 0; i < n; i++ {
		w.seq++
		base := pt.UnixNano() + int64(w.seq)*1000
		var data []byte
		switch w.mode {
		case config.ModeTraces:
			tr := make([]schema.TraceRow, rows)
			for r := range tr {
				ts := base + int64(r)
				tr[r] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts), TraceID: fmt.Sprintf("t-%s-%d-%d", tenant, w.seq, r), SpanID: fmt.Sprintf("s-%d-%d", w.seq, r), SpanName: "op", ServiceName: "svc", DurationNs: schema.Int64Ptr(10)}
			}
			data = makeTestTraceParquet(w.t, tr)
		default:
			lr := make([]schema.LogRow, rows)
			for r := range lr {
				lr[r] = schema.LogRow{TimestampUnixNano: base + int64(r), Body: fmt.Sprintf("%s-%d-%d", tenant, w.seq, r), ServiceName: "svc"}
			}
			data = makeTestParquet(w.t, lr)
		}
		key := fmt.Sprintf("%s/%s/%s/batch-L%d-%05d.parquet", tenant, w.mode, partition, level, w.seq)
		w.pool.put(key, data)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(rows),
			MinTimeNs: base, MaxTimeNs: base + int64(rows-1),
			SchemaFingerprint: planFP, CompactionLevel: level,
		}
		if mutate != nil {
			mutate(&fi)
		}
		w.m.AddFile(partition, fi)
		keys = append(keys, key)
	}
	return keys
}

// shippedPolicy is the default config's level policy.
func shippedPolicy() *LevelPolicy {
	d := config.Default().Compaction
	p := NewLevelPolicy(d.MinFilesL0, d.MinFilesL1, d.MinAge)
	p.DailyRollupAge = d.DailyRollupAge
	return p
}

// shippedScheduler wires the scheduler the way setupCompaction does, with the
// default config's knobs.
func (w *planWorld) shippedScheduler() *Scheduler {
	d := config.Default().Compaction
	return NewScheduler(SchedulerConfig{
		Manifest: w.m, Pool: w.pool, Ownership: soleOwnerResolver(),
		FairShare: NewFairShareScheduler(1), Policy: shippedPolicy(),
		Prefix: string(w.mode) + "/", Mode: w.mode, Interval: d.Interval,
		MaxConcurrent: d.MaxConcurrent, RowGroupSize: 1000, CompressionLevel: 3,
		CurrentSchemaFingerprint: planFP, CompactionConfig: d,
	})
}

func (w *planWorld) keys(partition string) []string {
	var out []string
	for _, f := range w.m.FilesForPartition(partition) {
		out = append(out, f.Key)
	}
	sort.Strings(out)
	return out
}

func (w *planWorld) rows() int64 {
	var n int64
	for _, files := range w.m.AllFiles() {
		for _, f := range files {
			n += f.RowCount
		}
	}
	return n
}

func bothModes(t *testing.T, fn func(t *testing.T, mode config.Mode)) {
	for _, mode := range []config.Mode{config.ModeLogs, config.ModeTraces} {
		t.Run(string(mode), func(t *testing.T) { fn(t, mode) })
	}
}

// TestScan_NoChurn_MultiTenantSingleFiles is the #343 reproduction: a
// partition older than DailyRollupAge where each of two tenants already holds a
// single compacted file. Nothing can be merged, so every scan must do zero
// compactions and leave the objects untouched. On the old planner the
// partition-wide L1 count (2) kept the daily rollup eligible and each tenant's
// lone file was rewritten 1 → 1 on every scan, climbing L2, L3, … forever.
func TestScan_NoChurn_MultiTenantSingleFiles(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		old := partitionAt(time.Now().Add(-72 * time.Hour))
		w.add("1001/0", old, 1, 1, 5, nil)
		w.add("1002/0", old, 1, 1, 5, nil)
		before := w.keys(old)
		sched := w.shippedScheduler()
		for scan := 1; scan <= 3; scan++ {
			n, err := sched.Scan(context.Background())
			if err != nil {
				t.Fatalf("scan %d: %v", scan, err)
			}
			if n != 0 {
				t.Fatalf("scan %d: compactions=%d, want 0 (single file per tenant must never be rewritten); keys now %v", scan, n, w.keys(old))
			}
		}
		if after := w.keys(old); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("objects rewritten: before %v after %v", before, after)
		}
	})
}

// TestScan_SecondScanDoesNothing is the storage-health cell: after a scan has
// done all the work there is, a second scan over the same manifest does zero
// compactions — whatever the tenant and partition mix.
func TestScan_SecondScanDoesNothing(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		now := time.Now()
		for i, tenant := range []string{"1001/0", "1002/0", "1003/7"} {
			w.add(tenant, partitionAt(now.Add(-72*time.Hour)), 1, 2, 3, nil)   // closed: rollup 2 → 1
			w.add(tenant, partitionAt(now.Add(-50*time.Hour)), 0, 3+i, 3, nil) // closed: L0 leftovers
			w.add(tenant, partitionAt(now.Add(-3*time.Hour)), 0, 10, 3, nil)   // open: 10 L0
			w.add(tenant, partitionAt(now.Add(-2*time.Hour)), 1, 1, 3, nil)    // single file: never
		}
		sched := w.shippedScheduler()
		rows := w.rows()
		for scan := 0; scan < 50; scan++ {
			n, err := sched.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
		}
		if got := w.rows(); got != rows {
			t.Fatalf("rows not conserved: %d → %d", rows, got)
		}
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("second scan after convergence: compactions=%d, want 0", n)
		}
	})
}

// TestScan_NoStarvation_NewerPartitionCompacted: a newer partition holding
// ≥ MinFilesL0 L0 files must be compacted on the first scan even when an older
// multi-tenant partition exists. On the old planner the older partition was
// "eligible" forever (cross-tenant count) and, sorted oldest first under one
// fair-share bucket, won every scan with MaxConcurrent 1.
func TestScan_NoStarvation_NewerPartitionCompacted(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		now := time.Now()
		old := partitionAt(now.Add(-72 * time.Hour))
		w.add("1001/0", old, 1, 1, 5, nil)
		w.add("1002/0", old, 1, 1, 5, nil)
		fresh := partitionAt(now.Add(-2 * time.Hour))
		w.add("1001/0", fresh, 0, 12, 2, nil)
		sched := w.shippedScheduler()
		if _, err := sched.Scan(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := len(w.m.FilesForPartition(fresh)); got != 1 {
			t.Fatalf("newer partition starved: %d files after one scan, want 1", got)
		}
	})
}

// TestScan_FairShareByRealTenant: tenant 1001 has three eligible partitions
// older than tenant 1002's one. With MaxConcurrent 1 and fair-share 1, tenant
// 1002 must get a slot within two scans. On the old planner every candidate of
// the production partition keys "dt=…/hour=…" fell into one "default"
// fair-share bucket, so the oldest-first order served tenant 1001 three times
// first.
func TestScan_FairShareByRealTenant(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		now := time.Now()
		for h := 0; h < 3; h++ {
			w.add("1001/0", partitionAt(now.Add(-time.Duration(10+h)*time.Hour)), 0, 10, 2, nil)
		}
		late := partitionAt(now.Add(-2 * time.Hour))
		w.add("1002/0", late, 0, 10, 2, nil)
		sched := w.shippedScheduler()
		for scan := 0; scan < 2; scan++ {
			if _, err := sched.Scan(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if got := len(w.m.FilesForPartition(late)); got != 1 {
			t.Fatalf("tenant 1002 starved behind tenant 1001: %d files in its partition after 2 scans, want 1", got)
		}
	})
}

// TestScan_NeverRewritesTieredObjects: objects whose storage class is not
// STANDARD (or INTELLIGENT_TIERING) are never selected: rewriting them costs a
// retrieval fee plus early deletion, and fails outright for Glacier Flexible
// and Deep Archive.
func TestScan_NeverRewritesTieredObjects(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for _, class := range []string{"STANDARD_IA", "ONEZONE_IA", "GLACIER_IR", "GLACIER", "DEEP_ARCHIVE"} {
			t.Run(class, func(t *testing.T) {
				w := newPlanWorld(t, mode)
				p := partitionAt(time.Now().Add(-72 * time.Hour))
				tiered := w.add("1001/0", p, 1, 2, 3, func(fi *manifest.FileInfo) { fi.StorageClass = class })
				w.add("1001/0", p, 1, 1, 3, nil)
				sched := w.shippedScheduler()
				if _, err := sched.Scan(context.Background()); err != nil {
					t.Fatal(err)
				}
				live := map[string]bool{}
				for _, k := range w.keys(p) {
					live[k] = true
				}
				for _, k := range tiered {
					if !live[k] {
						t.Fatalf("%s object %s was rewritten", class, k)
					}
				}
			})
		}
	})
}

// TestScan_StalePlanRunsOnLiveFiles: plans are made from one manifest snapshot
// per scan. When a planned file leaves the manifest before its merge runs (here
// a concurrent removal while another tenant's merge uploads), the merge must
// run on the files still registered instead of carrying the stale file into a
// merge whose publish would be refused. Without the re-check this scan does one
// merge instead of two and leaves the other tenant's hour unmerged.
func TestScan_StalePlanRunsOnLiveFiles(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-72 * time.Hour))
		keys := map[string][]string{
			"1001/0": w.add("1001/0", p, 0, 3, 2, nil),
			"1002/0": w.add("1002/0", p, 0, 3, 2, nil),
		}
		fp := &faultPool{mockPool: w.pool}
		var once sync.Once
		var removed string
		fp.set(func() {
			fp.uploadErr = func(key string) error {
				once.Do(func() {
					other := "1002/0"
					if strings.HasPrefix(key, "1002/0/") {
						other = "1001/0"
					}
					removed = keys[other][0]
					w.m.RemoveFile(p, removed)
					_ = w.pool.Delete(context.Background(), removed)
				})
				return nil
			}
		})
		d := config.Default().Compaction
		sched := NewScheduler(SchedulerConfig{
			Manifest: w.m, Pool: fp, Ownership: soleOwnerResolver(),
			FairShare: NewFairShareScheduler(1), Policy: shippedPolicy(),
			Prefix: string(mode) + "/", Mode: mode, MaxConcurrent: d.MaxConcurrent,
			RowGroupSize: 1000, CompressionLevel: 3, CurrentSchemaFingerprint: planFP, CompactionConfig: d,
		})
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("compactions=%d, want 2 (the tenant whose file vanished still merges its two live files)", n)
		}
		if got := len(w.m.FilesForPartition(p)); got != 2 {
			t.Fatalf("partition holds %d files, want one per tenant", got)
		}
		if got, want := w.rows(), int64(5*2); got != want {
			t.Fatalf("rows %d, want %d (6 files × 2 rows minus the removed file)", got, want)
		}
		_ = removed
	})
}
