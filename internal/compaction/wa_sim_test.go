package compaction

// Write-amplification and S3-request simulation of the shipped compaction
// defaults (issue #343). Skipped unless LH_COMPACTION_SIM=1.
//
// It drives the real Scheduler with config.Default().Compaction and
// FairShare(1) over a counting object pool. The planner's clock advances 5
// minutes per scan (288 scans per simulated day) and every tenant flushes one
// small L0 file per scan. The numbers it reports are MEASURED on the in-memory
// pool: they count requests and bytes the compaction code issues, not S3
// latency or cost.
//
// The file uses only APIs that exist on origin/main (plus the planClock hook),
// so the same file produces the "before" numbers there.
//
//	LH_COMPACTION_SIM=1 LH_SIM_LABEL=after GOWORK=off \
//	  go test ./internal/compaction -run TestSimWriteAmplification -count=1 -timeout=60m -v
//
// Output: $LH_SIM_OUT (default /tmp/fix343/sim-<label>.md) plus a .json twin.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type simCountingPool struct {
	mu                       sync.Mutex
	objs                     map[string][]byte
	puts, gets, dels         int
	bytesPut, bytesGot       int64
	flushedBytes, flushedObj int64
}

func (p *simCountingPool) Upload(_ context.Context, key string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objs[key] = append([]byte(nil), data...)
	p.puts++
	p.bytesPut += int64(len(data))
	return nil
}

func (p *simCountingPool) Download(_ context.Context, key string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d, ok := p.objs[key]
	p.gets++
	if !ok {
		return nil, nil
	}
	p.bytesGot += int64(len(d))
	return append([]byte(nil), d...), nil
}

func (p *simCountingPool) Delete(_ context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.objs, key)
	p.dels++
	return nil
}

// flush stores an object the way the writer does: not counted as compaction I/O.
func (p *simCountingPool) flush(key string, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objs[key] = data
	p.flushedBytes += int64(len(data))
	p.flushedObj++
}

func (p *simCountingPool) snapshot() (puts, gets, dels int, bytesPut int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.puts, p.gets, p.dels, p.bytesPut
}

type simDay struct {
	Day                  int     `json:"day"`
	Merges               int     `json:"merges"`
	ObjectsWritten       int     `json:"objects_written_by_compaction"`
	PutPerScanMean       float64 `json:"put_per_scan_mean"`
	PutPerScanMax        int     `json:"put_per_scan_max"`
	GetPerScanMean       float64 `json:"get_per_scan_mean"`
	GetPerScanMax        int     `json:"get_per_scan_max"`
	DelPerScanMean       float64 `json:"delete_per_scan_mean"`
	DelPerScanMax        int     `json:"delete_per_scan_max"`
	BytesFlushed         int64   `json:"bytes_flushed"`
	BytesWrittenByComp   int64   `json:"bytes_written_by_compaction"`
	WA                   float64 `json:"write_amplification"`
	RowsIngested         int64   `json:"rows_ingested"`
	RowsRewritten        int64   `json:"rows_rewritten"`
	RowsRewrittenRatio   float64 `json:"rows_rewritten_over_ingested"`
	L0Backlog            int     `json:"l0_backlog_files_past_min_age"`
	L0OldestAgeHours     float64 `json:"l0_oldest_age_hours"`
	ClosedGroupFilesP50  int     `json:"files_per_closed_tenant_hour_p50"`
	ClosedGroupFilesP90  int     `json:"files_per_closed_tenant_hour_p90"`
	ClosedGroupFilesMax  int     `json:"files_per_closed_tenant_hour_max"`
	TotalObjects         int     `json:"total_objects_end_of_day"`
	MaxLevelSeen         int     `json:"max_level_seen"`
	scans                int
	putSum, getSum, dSum int
}

type simResult struct {
	Label             string   `json:"label"`
	Mode              string   `json:"mode"`
	Tenants           int      `json:"tenants"`
	Days              []simDay `json:"days"`
	IdleScanMerges    []int    `json:"idle_scans_merges_after_ingest_stops"`
	IdleScanObjWrites int      `json:"idle_scans_objects_written"`
	FinalObjects      int      `json:"final_objects"`
}

func simPercentile(v []int, q float64) int {
	if len(v) == 0 {
		return 0
	}
	s := append([]int(nil), v...)
	sort.Ints(s)
	i := int(q * float64(len(s)-1))
	return s[i]
}

func simGroupOf(key string) string {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) < 3 {
		return key
	}
	return parts[0] + "/" + parts[1]
}

func runSimScenario(t *testing.T, label string, mode config.Mode, tenantCount, days int) simResult {
	const (
		scansPerDay = 288
		rowsPerFile = 20
		fp          = "sim-fp"
	)
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	clock := start
	oldClock := planClock
	planClock = func() time.Time { return clock }
	defer func() { planClock = oldClock }()

	pool := &simCountingPool{objs: map[string][]byte{}}
	m := manifest.New("sim-bucket", string(mode)+"/")
	d := config.Default().Compaction
	policy := NewLevelPolicy(d.MinFilesL0, d.MinFilesL1, d.MinAge)
	policy.DailyRollupAge = d.DailyRollupAge
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool,
		Ownership: NewOwnershipResolver("self", func() []string { return []string{"self"} }),
		FairShare: NewFairShareScheduler(1), Policy: policy,
		Prefix: string(mode) + "/", Mode: mode, Interval: d.Interval,
		MaxConcurrent: d.MaxConcurrent, RowGroupSize: 1000, CompressionLevel: 3,
		CurrentSchemaFingerprint: fp, CompactionConfig: d,
	})

	tenantName := func(i int) string { return fmt.Sprintf("%d/0", 1001+i) }
	seq := 0
	flush := func() {
		part := fmt.Sprintf("dt=%s/hour=%02d", clock.Format("2006-01-02"), clock.Hour())
		for i := 0; i < tenantCount; i++ {
			seq++
			base := clock.UnixNano() + int64(seq)*1000
			var data []byte
			if mode == config.ModeTraces {
				rows := make([]schema.TraceRow, rowsPerFile)
				for r := range rows {
					ts := base + int64(r)
					rows[r] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: ts, TraceID: fmt.Sprintf("t%d-%d", seq, r), SpanID: fmt.Sprintf("s%d-%d", seq, r), SpanName: "op", ServiceName: "svc", DurationNs: 10}
				}
				data = makeTestTraceParquet(t, rows)
			} else {
				rows := make([]schema.LogRow, rowsPerFile)
				for r := range rows {
					rows[r] = schema.LogRow{TimestampUnixNano: base + int64(r), Body: fmt.Sprintf("line %d %d some message text", seq, r), ServiceName: "svc"}
				}
				data = makeTestParquet(t, rows)
			}
			key := fmt.Sprintf("%s/%s/%s/batch-L0-%07d.parquet", tenantName(i), mode, part, seq)
			pool.flush(key, data)
			m.AddFile(part, manifest.FileInfo{
				Key: key, Size: int64(len(data)), RowCount: rowsPerFile,
				MinTimeNs: base, MaxTimeNs: base + rowsPerFile - 1,
				SchemaFingerprint: fp, CompactionLevel: 0,
			})
		}
	}

	res := simResult{Label: label, Mode: string(mode), Tenants: tenantCount}
	seenOutputs := map[string]bool{}
	for day := 0; day < days; day++ {
		sd := simDay{Day: day + 1}
		flushedBytes0 := pool.flushedBytes
		for i := 0; i < scansPerDay; i++ {
			clock = clock.Add(5 * time.Minute)
			flush()
			sd.RowsIngested += int64(tenantCount * rowsPerFile)
			p0, g0, d0, b0 := pool.snapshot()
			n, err := sched.Scan(context.Background())
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			p1, g1, d1, b1 := pool.snapshot()
			sd.Merges += n
			sd.ObjectsWritten += p1 - p0
			sd.BytesWrittenByComp += b1 - b0
			sd.putSum += p1 - p0
			sd.getSum += g1 - g0
			sd.dSum += d1 - d0
			if p1-p0 > sd.PutPerScanMax {
				sd.PutPerScanMax = p1 - p0
			}
			if g1-g0 > sd.GetPerScanMax {
				sd.GetPerScanMax = g1 - g0
			}
			if d1-d0 > sd.DelPerScanMax {
				sd.DelPerScanMax = d1 - d0
			}
			sd.scans++
			// Rows rewritten: rows in each compaction output the first time it appears.
			for _, files := range m.AllFiles() {
				for _, f := range files {
					if strings.Contains(f.Key, "compacted-") && !seenOutputs[f.Key] {
						seenOutputs[f.Key] = true
						sd.RowsRewritten += f.RowCount
					}
				}
			}
		}
		sd.BytesFlushed = pool.flushedBytes - flushedBytes0
		if sd.BytesFlushed > 0 {
			sd.WA = float64(sd.BytesWrittenByComp) / float64(sd.BytesFlushed)
		}
		if sd.RowsIngested > 0 {
			sd.RowsRewrittenRatio = float64(sd.RowsRewritten) / float64(sd.RowsIngested)
		}
		sd.PutPerScanMean = float64(sd.putSum) / float64(sd.scans)
		sd.GetPerScanMean = float64(sd.getSum) / float64(sd.scans)
		sd.DelPerScanMean = float64(sd.dSum) / float64(sd.scans)

		// End-of-day state.
		groups := map[string]int{}
		var closed []int
		for part, files := range m.AllFiles() {
			pt, err := manifest.ParsePartitionTime(part)
			if err != nil {
				continue
			}
			age := clock.Sub(pt)
			perGroup := map[string]int{}
			for _, f := range files {
				sd.TotalObjects++
				perGroup[simGroupOf(f.Key)]++
				if f.CompactionLevel > sd.MaxLevelSeen {
					sd.MaxLevelSeen = f.CompactionLevel
				}
				if f.CompactionLevel == 0 && age >= d.MinAge {
					sd.L0Backlog++
					if h := age.Hours(); h > sd.L0OldestAgeHours {
						sd.L0OldestAgeHours = h
					}
				}
			}
			for g, n := range perGroup {
				groups[part+"|"+g] = n
				if age >= d.DailyRollupAge+time.Hour {
					closed = append(closed, n)
				}
			}
		}
		sd.ClosedGroupFilesP50 = simPercentile(closed, 0.5)
		sd.ClosedGroupFilesP90 = simPercentile(closed, 0.9)
		sd.ClosedGroupFilesMax = simPercentile(closed, 1)
		res.Days = append(res.Days, sd)
	}

	// Ingest stops; the second scan over a settled manifest must do nothing.
	p0, _, _, _ := pool.snapshot()
	for i := 0; i < 5; i++ {
		clock = clock.Add(5 * time.Minute)
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		res.IdleScanMerges = append(res.IdleScanMerges, n)
	}
	p1, _, _, _ := pool.snapshot()
	res.IdleScanObjWrites = p1 - p0
	n := 0
	for _, files := range m.AllFiles() {
		n += len(files)
	}
	res.FinalObjects = n
	return res
}

func simMarkdown(rs []simResult) string {
	var b strings.Builder
	for _, r := range rs {
		fmt.Fprintf(&b, "\n### %s (%s, %d tenants)\n\n", r.Label, r.Mode, r.Tenants)
		b.WriteString("| day | merges | objs written | PUT/scan mean (max) | GET/scan mean (max) | DEL/scan mean (max) | WA bytes | rows rewritten / ingested | L0 backlog (oldest h) | files per closed tenant-hour p50/p90/max | total objects | max level |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
		for _, d := range r.Days {
			fmt.Fprintf(&b, "| %d | %d | %d | %.2f (%d) | %.2f (%d) | %.2f (%d) | %.2f | %.2f | %d (%.1f) | %d/%d/%d | %d | %d |\n",
				d.Day, d.Merges, d.ObjectsWritten, d.PutPerScanMean, d.PutPerScanMax, d.GetPerScanMean, d.GetPerScanMax,
				d.DelPerScanMean, d.DelPerScanMax, d.WA, d.RowsRewrittenRatio, d.L0Backlog, d.L0OldestAgeHours,
				d.ClosedGroupFilesP50, d.ClosedGroupFilesP90, d.ClosedGroupFilesMax, d.TotalObjects, d.MaxLevelSeen)
		}
		fmt.Fprintf(&b, "\nAfter ingest stops, 5 idle scans: merges %v, objects written %d, final objects %d.\n", r.IdleScanMerges, r.IdleScanObjWrites, r.FinalObjects)
	}
	return b.String()
}

func TestSimWriteAmplification(t *testing.T) {
	if os.Getenv("LH_COMPACTION_SIM") != "1" {
		t.Skip("set LH_COMPACTION_SIM=1 to run the compaction write-amplification simulation")
	}
	label := os.Getenv("LH_SIM_LABEL")
	if label == "" {
		label = "run"
	}
	scenarios := []struct {
		name    string
		mode    config.Mode
		tenants int
	}{
		{"logs-4-tenants", config.ModeLogs, 4},
		{"logs-20-tenants", config.ModeLogs, 20},
		{"traces-4-tenants", config.ModeTraces, 4},
	}
	var results []simResult
	for _, sc := range scenarios {
		t.Logf("scenario %s", sc.name)
		results = append(results, runSimScenario(t, sc.name, sc.mode, sc.tenants, 3))
	}
	out := os.Getenv("LH_SIM_OUT")
	if out == "" {
		out = fmt.Sprintf("/tmp/fix343/sim-%s.md", label)
	}
	if err := os.MkdirAll("/tmp/fix343", 0o755); err != nil {
		t.Fatal(err)
	}
	md := fmt.Sprintf("## Compaction simulation: %s\n\nMeasured on an in-memory counting pool, shipped defaults (min_files 10/10, min_age 1h, daily_rollup_age 24h, max_concurrent 1, fair share 1), planner clock +5 min per scan (288 scans/day), one 20-row L0 file per tenant per scan.\n%s", label, simMarkdown(results))
	if err := os.WriteFile(out, []byte(md), 0o644); err != nil {
		t.Fatal(err)
	}
	js, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(strings.TrimSuffix(out, ".md")+".json", js, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + md)
}
