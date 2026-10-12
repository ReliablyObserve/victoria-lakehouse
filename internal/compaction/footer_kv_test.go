package compaction

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tokenbloom"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// footerKV reads every footer key-value of an object.
func footerKV(t *testing.T, data []byte) map[string]string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	kv := map[string]string{}
	for _, e := range f.Metadata().KeyValueMetadata {
		kv[e.Key] = e.Value
	}
	return kv
}

// fakeComputers describe each row as one stream-day listing "_msg" and the
// service, so the test can tell which rows a facet was computed from.
func fakeComputers(t *testing.T) {
	t.Helper()
	fieldpresence.SetComputers(fieldpresence.Computers{
		Logs: func(rows []schema.LogRow, _ schema.SlotMapping) *fieldpresence.Builder {
			b := fieldpresence.NewBuilder()
			for _, r := range rows {
				b.Add(1, "", "s", 1, []string{"_msg", "svc:" + r.ServiceName})
			}
			return b
		},
		Traces: func(rows []schema.TraceRow, _ schema.SlotMapping) *fieldpresence.Builder {
			b := fieldpresence.NewBuilder()
			for _, r := range rows {
				b.Add(1, "", "s", 1, []string{"_msg", "svc:" + r.ServiceName})
			}
			return b
		},
	})
	t.Cleanup(func() { fieldpresence.SetComputers(fieldpresence.Computers{}) })
}

func facetNames(t *testing.T, kv map[string]string) (names []string, rows uint64) {
	t.Helper()
	v, ok := kv[fieldpresence.KVKey]
	if !ok {
		t.Fatalf("compacted object has no %s footer key; keys: %v", fieldpresence.KVKey, kvKeys(kv))
	}
	f, err := fieldpresence.Decode(v)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range f.Entries {
		rows += e.Rows
		names = append(names, f.NamesOf(e)...)
	}
	sort.Strings(names)
	return names, rows
}

func kvKeys(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// A compacted logs object carries every footer key-value a flush writes: the
// field-presence facet, recomputed from the rows of the OUTPUT (never merged from
// the inputs'), the body token bloom of each output row group, and the slot
// binding. Before, compaction wrote neither the facet nor the token blooms (#344).
func TestCompactor_LogsOutputCarriesEveryFooterKV(t *testing.T) {
	fakeComputers(t)
	prev := activeSlotResolver
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant_id"}}))
	t.Cleanup(func() { SetSlotResolver(prev) })

	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	const partition = "dt=2026-05-04/hour=10"
	var all []schema.LogRow
	var files []manifest.FileInfo
	for f := 0; f < 3; f++ {
		var rows []schema.LogRow
		for i := 0; i < 40; i++ {
			n := f*40 + i
			rows = append(rows, schema.LogRow{TimestampUnixNano: int64(1000 + n), Body: fmt.Sprintf("request %d from checkout-%d failed", n, n%7), ServiceName: fmt.Sprintf("svc-%d", n%3)})
		}
		all = append(all, rows...)
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/dt=2026-05-04/hour=10/src-%d.parquet", f)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 40, MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[39].TimestampUnixNano, SchemaFingerprint: "fp"}
		m.AddFile(partition, fi)
		files = append(files, fi)
	}
	c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "logs/", Mode: config.ModeLogs, RowGroupSize: 50, CompressionLevel: 3})
	res, err := c.Compact(context.Background(), partition, files, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := pool.get(res.OutputFile)
	kv := footerKV(t, out)

	names, rows := facetNames(t, kv)
	if rows != 120 || strings.Join(names, ",") != "_msg,svc:svc-0,svc:svc-1,svc:svc-2" {
		t.Errorf("facet rows=%d names=%v: want the 120 output rows listing _msg and the three services", rows, names)
	}

	// 120 rows in row groups of 50: three row groups, each with its own bloom.
	pf, _ := parquet.OpenFile(bytes.NewReader(out), int64(len(out)))
	if got := len(pf.RowGroups()); got != 3 {
		t.Fatalf("output has %d row groups, want 3", got)
	}
	sorted := append([]schema.LogRow(nil), all...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TimestampUnixNano < sorted[j].TimestampUnixNano })
	for rg := 0; rg < 3; rg++ {
		v, ok := kv[fmt.Sprintf(tokenbloom.KeyFormat, rg)]
		if !ok {
			t.Fatalf("compacted object has no token bloom for row group %d; keys: %v", rg, kvKeys(kv))
		}
		var bloom tokenbloom.TokenBloom
		if err := bloom.UnmarshalBinary([]byte(v)); err != nil {
			t.Fatal(err)
		}
		// Soundness: row group rg's bloom holds every token of ITS rows (the input
		// files' row-group indexes mean nothing here).
		end := (rg + 1) * 50
		if end > len(sorted) {
			end = len(sorted)
		}
		for _, r := range sorted[rg*50 : end] {
			for _, tok := range tokenbloom.Tokenize(r.Body) {
				if !bloom.Test(tok) {
					t.Fatalf("row group %d bloom misses %q of its own row %q: a body search would skip rows that match", rg, tok, r.Body)
				}
			}
		}
	}
	if kv[schema.DedicatedSlotsMetaKey] == "" {
		t.Error("slot binding missing")
	}
	if kv[schema.TraceIDHexMetaKey] == "" {
		t.Error("lh.trace_id_hex attestation missing")
	}
}

func TestCompactor_TracesOutputCarriesEveryFooterKV(t *testing.T) {
	fakeComputers(t)
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")
	const partition = "dt=2026-05-04/hour=10"
	var files []manifest.FileInfo
	for f := 0; f < 2; f++ {
		var rows []schema.TraceRow
		for i := 0; i < 30; i++ {
			n := f*30 + i
			rows = append(rows, schema.TraceRow{TimestampUnixNano: int64(1000 + n), StartTimeUnixNano: int64(1000 + n), TraceID: fmt.Sprintf("trace-%02d", n/2), SpanID: fmt.Sprintf("s%d", n), SpanName: "op", ServiceName: fmt.Sprintf("svc-%d", n%2), Body: fmt.Sprintf("span %d did work", n)})
		}
		data := makeTestTraceParquet(t, rows)
		key := fmt.Sprintf("traces/dt=2026-05-04/hour=10/src-%d.parquet", f)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 30, MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[29].TimestampUnixNano, SchemaFingerprint: "fp"}
		m.AddFile(partition, fi)
		files = append(files, fi)
	}
	c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "traces/", Mode: config.ModeTraces, RowGroupSize: 25, CompressionLevel: 3})
	res, err := c.Compact(context.Background(), partition, files, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := pool.get(res.OutputFile)
	kv := footerKV(t, out)
	if _, rows := facetNames(t, kv); rows != 60 {
		t.Errorf("facet rows = %d, want 60", rows)
	}
	if kv[traceindex.MetadataKey] == "" {
		t.Error("_trace_idx missing")
	}
	for rg := 0; rg < 3; rg++ {
		if kv[fmt.Sprintf(tokenbloom.KeyFormat, rg)] == "" {
			t.Errorf("no body token bloom for row group %d", rg)
		}
	}
}

// With no computer installed (the shared writers' own unit tests) nothing is
// written and nothing fails.
func TestCompactor_NoFacetWithoutAComputer(t *testing.T) {
	fieldpresence.SetComputers(fieldpresence.Computers{})
	data, err := WriteLogs([]schema.LogRow{{TimestampUnixNano: 1, Body: "x y"}}, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := footerKV(t, data)[fieldpresence.KVKey]; ok {
		t.Error("a facet was written without a computer")
	}
}

// An object whose facet passes the cap gets the marker and no facet.
func TestWriteLogs_OverCapObjectCarriesTheMarker(t *testing.T) {
	fieldpresence.SetComputers(fieldpresence.Computers{
		Logs: func(rows []schema.LogRow, _ schema.SlotMapping) *fieldpresence.Builder {
			b := fieldpresence.NewBuilder()
			for i := 0; i < 60000; i++ {
				b.Add(1, fmt.Sprintf("%016x%016x%016x", splitmix(uint64(i)*3), splitmix(uint64(i)*3+1), splitmix(uint64(i)*3+2)), "", 1, []string{"a"})
			}
			return b
		},
	})
	t.Cleanup(func() { fieldpresence.SetComputers(fieldpresence.Computers{}) })
	data, err := WriteLogs([]schema.LogRow{{TimestampUnixNano: 1}}, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	kv := footerKV(t, data)
	if _, ok := kv[fieldpresence.KVKey]; ok {
		t.Error("an over-cap object carries a facet")
	}
	if kv[fieldpresence.OverCapKey] != "1" {
		t.Errorf("over-cap marker = %q, want 1", kv[fieldpresence.OverCapKey])
	}
}

// splitmix is a stateless 64-bit mixer: distinct inputs give incompressible ids.
func splitmix(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}
