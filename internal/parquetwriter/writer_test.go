package parquetwriter

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter/structcheck"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	testRG   = 100
	testBase = int64(1_760_000_000_000_000_000)
)

func testLogRows(n int) []schema.LogRow {
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: testBase + int64(i)*1_000_000,
			Body:              fmt.Sprintf("processed request %d for tenant-%d", i, i%5),
			SeverityText:      "INFO",
			ServiceName:       fmt.Sprintf("svc-%02d", i%7),
			TraceID:           fmt.Sprintf("%032x", i),
			SpanID:            fmt.Sprintf("%016x", i),
		}
		if i%9 == 0 {
			rows[i].Body = "" // a row without a body adds no token
		}
	}
	return rows
}

func testSpanRows(n int) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: testBase + int64(i)*1_000_000,
			StartTimeUnixNano: schema.Int64Ptr(testBase + int64(i)*1_000_000 - 500),
			DurationNs:        schema.Int64Ptr(500),
			TraceID:           fmt.Sprintf("%032x", i/4),
			SpanID:            fmt.Sprintf("%016x", i),
			SpanName:          fmt.Sprintf("op-%d", i%6),
			ServiceName:       fmt.Sprintf("svc-%02d", i%3),
		}
		if i%3 == 0 {
			rows[i].Body = fmt.Sprintf("span event %d fired", i)
		}
	}
	return rows
}

func slots(attrs ...schema.SlotAttr) *schema.SlotResolver { return schema.NewSlotResolver(attrs) }

func mustLogs(t testing.TB, rows []schema.LogRow, o Options) []byte {
	t.Helper()
	res, err := WriteLogs(rows, o)
	if err != nil {
		t.Fatalf("WriteLogs: %v", err)
	}
	return res.Data
}

func mustTraces(t testing.TB, rows []schema.TraceRow, o Options) Result {
	t.Helper()
	res, err := WriteTraces(rows, o)
	if err != nil {
		t.Fatalf("WriteTraces: %v", err)
	}
	return res
}

func describe(t testing.TB, data []byte) *structcheck.Structure {
	t.Helper()
	s, err := structcheck.Describe(data)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	return s
}

// The writer is deterministic: the same rows and Options give the same bytes,
// whichever instance, goroutine or moment writes them. Two instances that encode
// the same rows (a retried flush, a re-run merge) therefore produce the same object.
func TestWriteIsByteReproducible(t *testing.T) {
	o := Options{RowGroupSize: testRG, CompressionLevel: 3, Slots: slots(schema.SlotAttr{Name: "tenant.tier", Bloom: true}), TraceIDHex: true}
	logs, spans := testLogRows(350), testSpanRows(350)
	if !bytes.Equal(mustLogs(t, logs, o), mustLogs(t, logs, o)) {
		t.Error("logs: two writes of the same rows differ")
	}
	a, b := mustTraces(t, spans, o), mustTraces(t, spans, o)
	if !bytes.Equal(a.Data, b.Data) {
		t.Error("traces: two writes of the same rows differ")
	}
}

// Two writers with DIFFERENT settings running at once do not influence each
// other: there is no shared state to leak a resolver, a level or an attestation
// through. Each result equals the same write made alone. Run with -race.
func TestConcurrentWritersWithDifferentOptionsDoNotInteract(t *testing.T) {
	logs, spans := testLogRows(450), testSpanRows(450)
	optsA := Options{RowGroupSize: 100, CompressionLevel: 1, Slots: slots(schema.SlotAttr{Name: "a.one", Bloom: true}), TraceIDHex: true}
	optsB := Options{RowGroupSize: 150, CompressionLevel: 7, Slots: slots(schema.SlotAttr{Name: "b.one"}, schema.SlotAttr{Name: "b.two", Bloom: true}), TraceIDHex: false}
	wantLogsA, wantLogsB := mustLogs(t, logs, optsA), mustLogs(t, logs, optsB)
	wantSpansA, wantSpansB := mustTraces(t, spans, optsA).Data, mustTraces(t, spans, optsB).Data
	if bytes.Equal(wantLogsA, wantLogsB) || bytes.Equal(wantSpansA, wantSpansB) {
		t.Fatal("fixture error: the two option sets must give different bytes")
	}

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for i := 0; i < 8; i++ {
		o, wl, ws, name := optsA, wantLogsA, wantSpansA, "A"
		if i%2 == 1 {
			o, wl, ws, name = optsB, wantLogsB, wantSpansB, "B"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if res, err := WriteLogs(logs, o); err != nil || !bytes.Equal(res.Data, wl) {
					errs <- fmt.Sprintf("logs writer %s: result differs from the same write made alone (err=%v)", name, err)
					return
				}
				if res, err := WriteTraces(spans, o); err != nil || !bytes.Equal(res.Data, ws) {
					errs <- fmt.Sprintf("traces writer %s: result differs from the same write made alone (err=%v)", name, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// Every field of Options reaches the file, and only through Options.
func TestOptionsDecideTheStructure(t *testing.T) {
	rows := testLogRows(250)
	base := Options{RowGroupSize: testRG, CompressionLevel: 3}

	t.Run("slots add the mapping and the bloom column", func(t *testing.T) {
		res := slots(schema.SlotAttr{Name: "tenant.tier", Bloom: true})
		slotted := testLogRows(250)
		for i := range slotted {
			schema.SetLogSlot(&slotted[i], "ded_s01", fmt.Sprintf("tier-%d", i%4))
		}
		with := describe(t, mustLogs(t, slotted, Options{RowGroupSize: testRG, CompressionLevel: 3, Slots: res}))
		without := describe(t, mustLogs(t, slotted, base))
		if _, ok := with.KeyValues[schema.DedicatedSlotsMetaKey]; !ok {
			t.Errorf("slot mapping missing from the footer: %v", with.KeyNames())
		}
		if _, ok := without.KeyValues[schema.DedicatedSlotsMetaKey]; ok {
			t.Errorf("a nil resolver must not write a slot mapping: %v", without.KeyNames())
		}
		if len(with.BloomColumns()) != len(without.BloomColumns())+1 {
			t.Errorf("a bloomed slot adds one split-block bloom column: %v vs %v", with.BloomColumns(), without.BloomColumns())
		}
	})

	t.Run("trace id hex attestation", func(t *testing.T) {
		a := describe(t, mustLogs(t, rows, Options{RowGroupSize: testRG, CompressionLevel: 3, TraceIDHex: true}))
		b := describe(t, mustLogs(t, rows, base))
		if a.KeyValues[schema.TraceIDHexMetaKey] == b.KeyValues[schema.TraceIDHexMetaKey] {
			t.Errorf("the lh.trace_id_hex footer value must follow Options.TraceIDHex: %v / %v", a.KeyValues, b.KeyValues)
		}
	})

	t.Run("row group size cuts row groups", func(t *testing.T) {
		s := describe(t, mustLogs(t, rows, Options{RowGroupSize: 60, CompressionLevel: 3}))
		if len(s.RowGroups) != 5 {
			t.Errorf("250 rows at 60 per row group: %d row groups, want 5", len(s.RowGroups))
		}
	})

	t.Run("compression level reaches the bytes", func(t *testing.T) {
		if bytes.Equal(mustLogs(t, rows, base), mustLogs(t, rows, Options{RowGroupSize: testRG, CompressionLevel: 11})) {
			t.Error("level 3 and level 11 gave the same bytes")
		}
	})

	t.Run("a non-positive row group size is refused", func(t *testing.T) {
		for _, rg := range []int{0, -1} {
			if _, err := WriteLogs(rows, Options{RowGroupSize: rg}); err == nil {
				t.Errorf("WriteLogs accepted RowGroupSize %d", rg)
			}
			if _, err := WriteTraces(testSpanRows(5), Options{RowGroupSize: rg}); err == nil {
				t.Errorf("WriteTraces accepted RowGroupSize %d", rg)
			}
		}
	})
}

// A file carries one body token bloom per row group that has a body; the traces
// object also carries the trace index and reports it for the caller's metrics.
func TestFooterEntriesPerSignal(t *testing.T) {
	o := Options{RowGroupSize: testRG, CompressionLevel: 3}

	logs := describe(t, mustLogs(t, testLogRows(350), o))
	if n := countPrefix(logs, TokenBloomKeyPrefix); n != 4 {
		t.Errorf("logs: %d token blooms for 4 row groups, want 4; keys %v", n, logs.KeyNames())
	}
	if _, ok := logs.KeyValues["_trace_idx"]; ok {
		t.Error("logs object must not carry a trace index")
	}

	res := mustTraces(t, testSpanRows(350), o)
	spans := describe(t, res.Data)
	if _, ok := spans.KeyValues["_trace_idx"]; !ok {
		t.Errorf("traces: no _trace_idx; keys %v", spans.KeyNames())
	}
	if n := countPrefix(spans, TokenBloomKeyPrefix); n != 4 {
		t.Errorf("traces with span bodies: %d token blooms, want 4; keys %v", n, spans.KeyNames())
	}
	if res.TraceIndex == nil || res.TraceIndex.Listed != 88 { // 350 spans / 4 per trace = 88 traces
		t.Errorf("TraceIndex = %+v, want 88 listed entries", res.TraceIndex)
	}

	empty := mustTraces(t, nil, o)
	if empty.TraceIndex != nil && empty.TraceIndex.Listed != 0 {
		t.Errorf("no spans, no listed index entries: %+v", empty.TraceIndex)
	}
	if _, err := structcheck.Describe(empty.Data); err != nil {
		t.Errorf("an empty object must still be a valid Parquet file: %v", err)
	}
}

// A row group of rows without any body has no token bloom (nothing to index),
// and the others are still keyed by their own row-group index.
func TestTokenBloomKeysFollowRowGroupIndex(t *testing.T) {
	rows := testLogRows(300)
	for i := 100; i < 200; i++ {
		rows[i].Body = ""
	}
	s := describe(t, mustLogs(t, rows, Options{RowGroupSize: testRG, CompressionLevel: 3}))
	for rg, want := range []bool{true, false, true} {
		if _, got := s.KeyValues[TokenBloomKey(rg)]; got != want {
			t.Errorf("row group %d: token bloom present = %v, want %v", rg, got, want)
		}
	}
}

// The package holds no mutable state: no package-level variable in any of its
// non-test files. A resolver or any other setting a caller wants to change goes
// through Options, so a second caller can never be affected by the first.
func TestPackageHasNoGlobalState(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				for _, n := range spec.(*ast.ValueSpec).Names {
					t.Errorf("%s: package-level var %q: pass it through Options", name, n.Name)
				}
			}
		}
	}
	if checked < 2 {
		t.Fatalf("checked %d source files, want the package's files", checked)
	}
}

func countPrefix(s *structcheck.Structure, prefix string) int {
	n := 0
	for k := range s.KeyValues {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

func BenchmarkWriteLogs(b *testing.B) {
	rows := testLogRows(10000)
	o := Options{RowGroupSize: 10000, CompressionLevel: 3}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mustLogs(b, rows, o)
	}
}
