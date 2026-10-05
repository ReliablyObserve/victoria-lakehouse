package compaction

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// faultPool wraps the in-memory pool with per-operation fault hooks.
type faultPool struct {
	*mockPool
	mu          sync.Mutex
	downloadErr func(key string) error
	uploadErr   func(key string) error
	deleteErr   func(key string) error
	onDownload  func(key string)
}

func (p *faultPool) hooks() (d, u, del func(string) error, on func(string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.downloadErr, p.uploadErr, p.deleteErr, p.onDownload
}

func (p *faultPool) set(f func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f()
}

func (p *faultPool) Download(ctx context.Context, key string) ([]byte, error) {
	d, _, _, on := p.hooks()
	if on != nil {
		on(key)
	}
	if d != nil {
		if err := d(key); err != nil {
			return nil, err
		}
	}
	return p.mockPool.Download(ctx, key)
}

func (p *faultPool) Upload(ctx context.Context, key string, data []byte) error {
	_, u, _, _ := p.hooks()
	if u != nil {
		if err := u(key); err != nil {
			return err
		}
	}
	return p.mockPool.Upload(ctx, key, data)
}

func (p *faultPool) Delete(ctx context.Context, key string) error {
	_, _, del, _ := p.hooks()
	if del != nil {
		if err := del(key); err != nil {
			return err
		}
	}
	return p.mockPool.Delete(ctx, key)
}

// objectBytes builds a real Parquet object whose rows carry exactly ids, in
// the signal's own schema (logs: Body, traces: TraceID).
func objectBytes(t *testing.T, mode config.Mode, ids []string, base int64) []byte {
	switch mode {
	case config.ModeTraces:
		rows := make([]schema.TraceRow, len(ids))
		for i, id := range ids {
			ts := base + int64(i)
			rows[i] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: ts, TraceID: id, SpanID: fmt.Sprintf("s%d", i), SpanName: "op", ServiceName: "svc", DurationNs: 10}
		}
		return makeTestTraceParquet(t, rows)
	default:
		rows := make([]schema.LogRow, len(ids))
		for i, id := range ids {
			rows[i] = schema.LogRow{TimestampUnixNano: base + int64(i), Body: id, ServiceName: "svc"}
		}
		return makeTestParquet(t, rows)
	}
}

// rowIDs reads the row ids back out of an object.
func rowIDs(mode config.Mode, data []byte) ([]string, error) {
	if mode == config.ModeTraces {
		rows, err := readTraceRows(data)
		if err != nil {
			return nil, err
		}
		ids := make([]string, len(rows))
		for i := range rows {
			ids[i] = rows[i].TraceID
		}
		return ids, nil
	}
	rows, err := readLogRows(data)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(rows))
	for i := range rows {
		ids[i] = rows[i].Body
	}
	return ids, nil
}

// idTag is the owner of a row: "<tenant>@<bucket>", the id's text before '#'.
func idTag(id string) string {
	if i := strings.IndexByte(id, '#'); i >= 0 {
		return id[:i]
	}
	return id
}

// ownerTag derives the expected owner of an object from where it sits: its key
// prefix and its bucket. Legacy keys without a numeric tenant prefix are the
// "legacy" tenant.
func ownerTag(key, bucket string) string {
	prefix := manifest.CompactionGroupPrefix(key)
	tenant := "legacy"
	if prefix != "" {
		parts := strings.SplitN(prefix, "/", 3)
		tenant = parts[0] + "/" + parts[1]
	}
	return tenant + "@" + bucket
}

// ledger wraps a planWorld with an oracle of every row ever ingested, and
// checks the whole cold tier against it.
type ledger struct {
	t     *testing.T
	w     *planWorld
	pool  interface{ Keys() []string }
	want  map[string]bool
	cache map[string][]string // object key -> row ids (objects are immutable)
	seq   int
}

func newLedger(w *planWorld) *ledger {
	return &ledger{t: w.t, w: w, pool: w.pool, want: map[string]bool{}, cache: map[string][]string{}}
}

// put ingests an object of n fresh rows owned by tag at key, registers it in
// the partition at level, and returns its entry. size > 0 overrides the
// manifest size (a mature file the tiny test object stands in for).
func (l *ledger) put(key, tag, bucket, partition string, level, n int, size int64) manifest.FileInfo {
	l.t.Helper()
	pt, err := manifest.ParsePartitionTime(partition)
	if err != nil {
		l.t.Fatal(err)
	}
	l.seq++
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s#%d#%d", tag, l.seq, i)
		l.want[ids[i]] = true
	}
	return l.putIDs(key, bucket, partition, level, ids, pt.UnixNano()+int64(l.seq)*1000, size)
}

// putIDs registers an object holding exactly ids (no oracle change).
func (l *ledger) putIDs(key, bucket, partition string, level int, ids []string, base, size int64) manifest.FileInfo {
	data := objectBytes(l.t, l.w.mode, ids, base)
	l.w.pool.put(key, data)
	if size <= 0 {
		size = int64(len(data))
	}
	fi := manifest.FileInfo{
		Key: key, Bucket: bucket, Size: size, RowCount: int64(len(ids)),
		MinTimeNs: base, MaxTimeNs: base + int64(len(ids)) - 1,
		SchemaFingerprint: planFP, CompactionLevel: level,
	}
	l.w.m.AddFile(partition, fi)
	l.cache[key] = ids
	return fi
}

// tenantL0 ingests n single-row-pair L0 files for a numeric tenant.
func (l *ledger) tenantL0(tenant, partition string, n int) []manifest.FileInfo {
	var out []manifest.FileInfo
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s/%s/%s/batch-L0-%s-%05d.parquet", tenant, l.w.mode, partition, tenant[:4], l.seq+1)
		out = append(out, l.put(key, tenant+"@", "", partition, 0, 2, 0))
	}
	return out
}

// check asserts, against the oracle: the storage invariants; every manifest
// object readable; the multiset of served rows == everything ingested (none
// lost, none served twice); and every row sits under its own tenant's key
// prefix and bucket. awaiting allows objects the manifest still owes a delete.
func (l *ledger) check(stage string, awaiting bool) {
	l.t.Helper()
	st := storageinvariants.State{Manifest: l.w.m, Bucket: l.pool}
	if awaiting {
		st.AwaitingDeletion = storageinvariants.AwaitingDeletionIn(l.w.m)
	}
	storageinvariants.Assert(l.t, stage, st)

	seen := make(map[string]int, len(l.want))
	var served, claimed int64
	for _, files := range l.w.m.AllFiles() {
		for _, f := range files {
			ids, ok := l.cache[f.Key]
			if !ok {
				var err error
				ids, err = rowIDs(l.w.mode, l.w.pool.get(f.Key))
				if err != nil {
					l.t.Fatalf("%s: read %s: %v", stage, f.Key, err)
				}
				want := ownerTag(f.Key, f.Bucket)
				for _, id := range ids {
					if got := idTag(id); got != want {
						l.t.Fatalf("%s: cross-tenant mixing: row %q (owner %q) sits in %s (bucket %q, owner %q)", stage, id, got, f.Key, f.Bucket, want)
					}
				}
				l.cache[f.Key] = ids
			}
			claimed += f.RowCount
			for _, id := range ids {
				served++
				seen[id]++
				if !l.want[id] {
					l.t.Fatalf("%s: row %q was never ingested (found in %s)", stage, id, f.Key)
				}
			}
		}
	}
	for id, n := range seen {
		if n > 1 {
			l.t.Fatalf("%s: row %q is served %d times", stage, id, n)
		}
	}
	if len(seen) != len(l.want) {
		var missing []string
		for id := range l.want {
			if seen[id] == 0 {
				missing = append(missing, id)
			}
		}
		sort.Strings(missing)
		if len(missing) > 5 {
			missing = missing[:5]
		}
		l.t.Fatalf("%s: %d of %d ingested rows are not served, e.g. %v", stage, len(l.want)-len(seen), len(l.want), missing)
	}
	if claimed != served {
		l.t.Fatalf("%s: manifest claims %d rows, objects hold %d", stage, claimed, served)
	}
}

// schedulerOn builds the shipped-defaults scheduler over pool.
func (w *planWorld) schedulerOn(pool CompactorPool) *Scheduler {
	d := config.Default().Compaction
	return NewScheduler(SchedulerConfig{
		Manifest: w.m, Pool: pool, Ownership: soleOwnerResolver(),
		FairShare: NewFairShareScheduler(1), Policy: shippedPolicy(),
		Prefix: string(w.mode) + "/", Mode: w.mode, Interval: d.Interval,
		MaxConcurrent: d.MaxConcurrent, RowGroupSize: 1000, CompressionLevel: 3,
		CurrentSchemaFingerprint: planFP, CompactionConfig: d,
	})
}
