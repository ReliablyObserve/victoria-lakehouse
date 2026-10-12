package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	lhdelete "github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// installPresenceComputers installs this package's row-to-facet functions as the
// binaries do at startup.
func installPresenceComputers(t *testing.T) {
	t.Helper()
	fieldpresence.SetComputers(FieldPresenceComputers())
	t.Cleanup(func() { fieldpresence.SetComputers(fieldpresence.Computers{}) })
}

func footerKVOf(tb testing.TB, data []byte) map[string]string {
	tb.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	kv := map[string]string{}
	for _, e := range f.Metadata().KeyValueMetadata {
		kv[e.Key] = e.Value
	}
	return kv
}

func sortedTraceRows(rows []schema.TraceRow) []schema.TraceRow {
	out := append([]schema.TraceRow(nil), rows...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].TimestampUnixNano != out[j].TimestampUnixNano {
			return out[i].TimestampUnixNano < out[j].TimestampUnixNano
		}
		return out[i].ServiceName < out[j].ServiceName
	})
	return out
}

// cellsEqual compares two folds of facets.
func cellsEqual(a, b map[streamDayKey]*presenceCell) bool {
	if len(a) != len(b) {
		return false
	}
	for k, ca := range a {
		cb, ok := b[k]
		if !ok || ca.rows != cb.rows || len(ca.names) != len(cb.names) {
			return false
		}
		for n := range ca.names {
			if _, ok := cb.names[n]; !ok {
				return false
			}
		}
	}
	return true
}

// A flush and a compaction of the same rows write the same footer key-values:
// the field-presence facet, the body token blooms, the slot binding, the trace-id
// attestation. A key the flush writes and compaction does not is a file that
// prunes worse (or lists fewer fields) the older it gets (#344).
func TestFooterKV_FlushAndCompactionWriteTheSameKeys(t *testing.T) {
	installPresenceComputers(t)
	prev := activeSlotResolver
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant_id"}}))
	compaction.SetSlotResolver(activeSlotResolver)
	t.Cleanup(func() { SetSlotResolver(prev); compaction.SetSlotResolver(prev) })

	rows := sortedTraceRows(genPresenceTraceRows(rand.New(rand.NewSource(4)), 230))
	flushed, err := writeTracesParquet(rows, 50, 3)
	if err != nil {
		t.Fatal(err)
	}
	compacted, err := compaction.WriteTraces(rows, 50, 3)
	if err != nil {
		t.Fatal(err)
	}
	fk, ck := footerKVOf(t, flushed.Data), footerKVOf(t, compacted)
	for k, v := range fk {
		cv, ok := ck[k]
		if !ok {
			t.Errorf("footer key %q is written by a flush and not by compaction", k)
		} else if cv != v {
			t.Errorf("footer key %q differs between a flush and a compaction of the same rows", k)
		}
	}
	for k := range ck {
		if _, ok := fk[k]; !ok {
			t.Errorf("footer key %q is written by compaction and not by a flush", k)
		}
	}
	for _, want := range []string{fieldpresence.KVKey, "_bloom_body_rg_0", "_bloom_body_rg_4", schema.DedicatedSlotsMetaKey} {
		if _, ok := fk[want]; !ok {
			t.Errorf("flushed object lacks %q; keys %v", want, fk)
		}
	}
}

// The facet of a compacted object is the facet of the union of its sources' rows:
// per stream-day the union of the names, the sum of the rows. It is recomputed
// from the merged rows, never merged from the inputs' facets, so it cannot drift.
func TestFieldPresence_CompactedFacetEqualsTheUnionOfTheSources(t *testing.T) {
	installPresenceComputers(t)
	for seed := int64(1); seed <= 8; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rows := genPresenceTraceRows(rng, 60+rng.Intn(150))
		parts := make([][]schema.TraceRow, 3)
		for _, r := range rows {
			i := rng.Intn(3)
			parts[i] = append(parts[i], r)
		}
		var sources []*fieldpresence.Facet
		var merged []schema.TraceRow
		for _, p := range parts {
			if len(p) == 0 {
				continue
			}
			res, err := writeTracesParquet(p, 40, 3)
			if err != nil {
				t.Fatal(err)
			}
			f, _ := facetOfParquet(t, res.Data)
			sources = append(sources, f)
			merged = append(merged, p...)
		}
		out, err := compaction.WriteTraces(sortedTraceRows(merged), 40, 3)
		if err != nil {
			t.Fatal(err)
		}
		cf, _ := facetOfParquet(t, out)
		if !cellsEqual(presenceFold(sources), presenceFold([]*fieldpresence.Facet{cf})) {
			t.Fatalf("seed %d: the compacted facet differs from the union of the sources' facets", seed)
		}
	}
}

type presenceMemPool struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (p *presenceMemPool) Upload(_ context.Context, key string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects[key] = append([]byte(nil), data...)
	return nil
}

func (p *presenceMemPool) Download(_ context.Context, key string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d, ok := p.objects[key]
	if !ok {
		return nil, context.DeadlineExceeded
	}
	return d, nil
}

func (p *presenceMemPool) Delete(_ context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.objects, key)
	return nil
}

// A delete rewrite recomputes the facet from the rows that survive: a field only
// the deleted rows carried is no longer listed (hot VictoriaLogs does not list a
// column no remaining row has), and the rows are those left.
func TestFieldPresence_DeleteRewriteRecomputesFromTheSurvivors(t *testing.T) {
	installPresenceComputers(t)
	var rows []schema.TraceRow
	for i := 0; i < 90; i++ {
		r := schema.TraceRow{
			TimestampUnixNano: int64(1_000_000 + i), StartTimeUnixNano: schema.Int64Ptr(int64(1_000_000 + i)), TraceID: fmt.Sprintf("trace-%02d", i), SpanID: fmt.Sprintf("s%d", i),
			Body: fmt.Sprintf("event %d", i), SpanName: "op", ServiceName: "keeper",
			Stream: `{resource_attr:service.name="keeper"}`, StreamID: fmt.Sprintf("%048x", 1),
		}
		if i%3 == 0 {
			r.ServiceName, r.Stream, r.StreamID = "leaky", `{resource_attr:service.name="leaky"}`, fmt.Sprintf("%048x", 2)
			r.SpanAttributes = map[string]string{"only.deleted": "secret"}
			r.ExceptionType = "Boom"
		}
		rows = append(rows, r)
	}
	src, err := writeTracesParquet(rows, 30, 3)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := facetOfParquet(t, src.Data)
	for _, name := range []string{"span_attr:only.deleted", "span_attr:exception.type"} {
		if !cellsHave(presenceFold([]*fieldpresence.Facet{before}), name) {
			t.Fatalf("the source facet must list %q, which only the doomed rows carry", name)
		}
	}
	pool := &presenceMemPool{objects: map[string][]byte{}}
	key := "traces/dt=2026-09-01/hour=00/src.parquet"
	pool.objects[key] = src.Data
	rw := lhdelete.NewRewriter(pool, "traces/", 30, "traces", lhdelete.WithParquetWriters(lhdelete.ParquetWriters{
		Logs: compaction.WriteLogs, Traces: compaction.WriteTraces, CompressionLevel: 3,
	}))
	res, err := rw.RewriteFile(context.Background(), key, []lhdelete.Tombstone{{ID: "t", Query: `service.name:="leaky"`, StartNs: 0, EndNs: 9_000_000}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsKept != 60 || res.RowsRemoved != 30 {
		t.Fatalf("kept %d removed %d, want 60/30", res.RowsKept, res.RowsRemoved)
	}
	after, overCap := facetOfParquet(t, pool.objects[res.NewKey])
	if after == nil || overCap {
		t.Fatal("the rewritten object has no facet")
	}
	cells := presenceFold([]*fieldpresence.Facet{after})
	for _, gone := range []string{"span_attr:only.deleted", "span_attr:exception.type"} {
		if cellsHave(cells, gone) {
			t.Errorf("%q is listed after the only rows carrying it were deleted", gone)
		}
	}
	var total uint64
	for k, c := range cells {
		total += c.rows
		if k.kind == fieldpresence.KindID24 && fmt.Sprintf("%x", k.key) == fmt.Sprintf("%048x", 2) {
			t.Errorf("the deleted stream is still listed: %+v", c)
		}
	}
	if total != 60 {
		t.Errorf("facet rows = %d, want the 60 survivors", total)
	}
	if want := presenceFold([]*fieldpresence.Facet{mustFacet(t, presenceOfTraceRows(survivors(rows), nil))}); !cellsEqual(cells, want) {
		t.Error("the rewritten facet differs from the facet of the surviving rows")
	}
}

func survivors(rows []schema.TraceRow) []schema.TraceRow {
	var out []schema.TraceRow
	for _, r := range rows {
		if r.ServiceName != "leaky" {
			out = append(out, r)
		}
	}
	return out
}

func mustFacet(t *testing.T, b *fieldpresence.Builder) *fieldpresence.Facet {
	t.Helper()
	f, err := fieldpresence.Decode(b.Finish().Value)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func cellsHave(cells map[streamDayKey]*presenceCell, name string) bool {
	for _, c := range cells {
		if _, ok := c.names[name]; ok {
			return true
		}
	}
	return false
}

// Random tombstones, then a delete rewrite of every object: the rewritten
// objects' facets are the facets a flush of the surviving rows writes, and those
// are what the read path lists for them (the drift guard's oracle).
func TestFieldPresence_RewrittenObjectsMatchTheReadPath(t *testing.T) {
	installPresenceComputers(t)
	queries := []string{`service.name:="svc-a"`, `service.name:="svc-b"`, `service.name:="svc-a" OR service.name:="svc-b"`}
	removed := int64(0)
	for seed := int64(1); seed <= 8; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rows := genPresenceTraceRows(rng, 60+rng.Intn(120))
		byHour := map[string][]schema.TraceRow{}
		for _, r := range rows {
			p := partitionFromNano(r.TimestampUnixNano)
			byHour[p] = append(byHour[p], r)
		}
		pool := &presenceMemPool{objects: map[string][]byte{}}
		rw := lhdelete.NewRewriter(pool, "traces/", 40, "traces", lhdelete.WithParquetWriters(lhdelete.ParquetWriters{
			Logs: compaction.WriteLogs, Traces: compaction.WriteTraces, CompressionLevel: 3,
		}))
		tomb := lhdelete.Tombstone{ID: "t", Query: queries[rng.Intn(len(queries))], StartNs: 0, EndNs: 1 << 62}
		var facets []*fieldpresence.Facet
		var survivorsBatches [][]schema.TraceRow
		n := 0
		for _, b := range byHour {
			res, err := writeTracesParquet(b, 40, 3)
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("traces/dt=2026-07-01/hour=23/src%d.parquet", n)
			n++
			pool.objects[key] = res.Data
			out, err := rw.RewriteFile(context.Background(), key, []lhdelete.Tombstone{tomb})
			if err != nil {
				t.Fatal(err)
			}
			removed += out.RowsRemoved
			data := res.Data
			if out.NewKey != "" {
				data = pool.objects[out.NewKey]
			} else if out.RowsKept == 0 && out.RowsRemoved > 0 {
				continue // everything was deleted: no object remains
			}
			f, overCap := facetOfParquet(t, data)
			if f == nil || overCap {
				t.Fatalf("seed %d: object has no facet", seed)
			}
			facets = append(facets, f)
			surv := readRowsForTest[schema.TraceRow](t, data)
			survivorsBatches = append(survivorsBatches, surv)
		}
		if len(survivorsBatches) == 0 {
			continue
		}
		s := fvcStorage(t, survivorsBatches...)
		lo := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixNano()
		got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "*", lo, lo+int64(72*time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
		if d := diffHits(namesToHits(got), presenceExpected(facets)); d != "" {
			t.Errorf("seed %d (%s): a rewritten object's facet and field_names differ: %s", seed, tomb.Query, d)
		}
	}
	if removed == 0 {
		t.Fatal("no tombstone removed a row: the test proves nothing")
	}
}

// readRowsForTest reads every row of a Parquet object.
func readRowsForTest[T any](t *testing.T, data []byte) []T {
	t.Helper()
	r := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	rows := make([]T, r.NumRows())
	n, err := r.Read(rows)
	if err != nil && n != len(rows) {
		t.Fatal(err)
	}
	return rows[:n]
}
