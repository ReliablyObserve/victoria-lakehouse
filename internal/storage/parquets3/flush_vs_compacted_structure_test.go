package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter/structcheck"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// MAIN RULE 4: a compacted file carries exactly the pruning structures a flushed
// file does (#344). These tests run the same rows through the real flush writer
// and through the real compactor (inputs flushed, merged by Compactor.Compact,
// uploaded to an in-memory object store) and compare what the footers say:
// footer key-value entries, split-block bloom columns, statistics, page index,
// sort order. They compare the decoded structure, not bytes.

// memObjects is the object store the compactor merges in.
type memObjects struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func (m *memObjects) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = append([]byte(nil), data...)
	return nil
}

func (m *memObjects) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.objs[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), d...), nil
}

func (m *memObjects) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

// compactObjects merges the flushed objects through the real Compactor and
// returns the single output object. outputRowGroupSize is the compactor's
// row-group size for the output level (its static configuration); inputs are the
// level-0 objects.
func compactObjects(t *testing.T, mode config.Mode, inputs [][]byte, outputRowGroupSize int) []byte {
	t.Helper()
	const partition = "dt=2026-05-04/hour=10"
	pool := &memObjects{objs: map[string][]byte{}}
	m := manifest.New("test-bucket", "data/")
	var files []manifest.FileInfo
	for i, data := range inputs {
		key := fmt.Sprintf("data/%s/batch-%03d.parquet", partition, i)
		pool.objs[key] = data
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, SchemaFingerprint: "fp", CompactionLevel: 0}
		m.AddFile(partition, fi)
		files = append(files, fi)
	}
	c := compaction.NewCompactor(compaction.CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "data/",
		Mode:             mode,
		RowGroupSize:     outputRowGroupSize,
		CompressionLevel: 3,
	})
	res, err := c.Compact(context.Background(), partition, files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if len(res.OutputFiles) != 1 {
		t.Fatalf("compaction produced %d outputs, want 1", len(res.OutputFiles))
	}
	return pool.objs[res.OutputFile]
}

// splitInterleaved returns two row sets holding the even and the odd rows, so
// the merge has to interleave them back (as it does for real time-overlapping
// inputs).
func splitInterleaved[T any](rows []T) (a, b []T) {
	for i := range rows {
		if i%2 == 0 {
			a = append(a, rows[i])
		} else {
			b = append(b, rows[i])
		}
	}
	return a, b
}

func structureOf(t *testing.T, data []byte) *structcheck.Structure {
	t.Helper()
	s, err := structcheck.Describe(data)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	return s
}

func requireSameStructure(t *testing.T, flushed, compacted []byte) {
	t.Helper()
	want, got := structureOf(t, flushed), structureOf(t, compacted)
	if diff := structcheck.Diff(want, got); len(diff) > 0 {
		t.Fatalf("a compacted file must carry the structures a flushed file does (MAIN RULE 4, #344); %d difference(s), flushed -> compacted:\n  %s",
			len(diff), strings.Join(diff, "\n  "))
	}
}

func goldenCompactionLogRows(n int) []schema.LogRow {
	rows := goldenLogRows(n)
	for i := range rows {
		if i%7 == 0 {
			rows[i].Body = ""
		}
	}
	return rows
}

func goldenCompactionSpanRows(n int) []schema.TraceRow {
	rows := goldenTraceRows(n)
	for i := range rows {
		if i%3 == 0 {
			rows[i].Body = fmt.Sprintf("span event %d fired", i)
		}
	}
	return rows
}

func TestFlushedAndCompactedLogsHaveTheSameStructure(t *testing.T) {
	const rg = 1000
	for _, withSlots := range []bool{false, true} {
		t.Run(fmt.Sprintf("slots=%v", withSlots), func(t *testing.T) {
			saved := activeSlotResolver
			t.Cleanup(func() { activeSlotResolver = saved; compaction.SetSlotResolver(nil) })
			var resolver *schema.SlotResolver
			if withSlots {
				resolver = flushGoldenSlots()
			}
			activeSlotResolver = resolver
			compaction.SetSlotResolver(resolver)

			rows := goldenCompactionLogRows(2*rg + 321)
			flushed, err := writeLogsParquet(rows, rg, 3)
			if err != nil {
				t.Fatal(err)
			}
			a, b := splitInterleaved(rows)
			fa, err := writeLogsParquet(a, rg, 3)
			if err != nil {
				t.Fatal(err)
			}
			fb, err := writeLogsParquet(b, rg, 3)
			if err != nil {
				t.Fatal(err)
			}
			compacted := compactObjects(t, config.ModeLogs, [][]byte{fa.Data, fb.Data}, rg)

			s := structureOf(t, compacted)
			if got := countKeys(s, parquetwriter.BodyBloomEntryPrefix); got != 3 {
				t.Fatalf("compacted logs object has %d body token blooms, want one per row group (3); footer keys %v", got, s.KeyNames())
			}
			requireSameStructure(t, flushed.Data, compacted)
		})
	}
}

func TestFlushedAndCompactedTracesHaveTheSameStructure(t *testing.T) {
	const rg = 1000
	for _, withSlots := range []bool{false, true} {
		t.Run(fmt.Sprintf("slots=%v", withSlots), func(t *testing.T) {
			saved := activeSlotResolver
			t.Cleanup(func() { activeSlotResolver = saved; compaction.SetSlotResolver(nil) })
			var resolver *schema.SlotResolver
			if withSlots {
				resolver = flushGoldenSlots()
			}
			activeSlotResolver = resolver
			compaction.SetSlotResolver(resolver)

			rows := goldenCompactionSpanRows(2*rg + 321)
			flushed, err := writeTracesParquet(rows, rg, 3)
			if err != nil {
				t.Fatal(err)
			}
			a, b := splitInterleaved(rows)
			fa, err := writeTracesParquet(a, rg, 3)
			if err != nil {
				t.Fatal(err)
			}
			fb, err := writeTracesParquet(b, rg, 3)
			if err != nil {
				t.Fatal(err)
			}
			compacted := compactObjects(t, config.ModeTraces, [][]byte{fa.Data, fb.Data}, rg)

			s := structureOf(t, compacted)
			if _, ok := s.KeyValues["_trace_idx"]; !ok {
				t.Fatalf("compacted traces object lost _trace_idx; footer keys %v", s.KeyNames())
			}
			requireSameStructure(t, flushed.Data, compacted)
		})
	}
}

func countKeys(s *structcheck.Structure, prefix string) int {
	n := 0
	for k := range s.KeyValues {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

// footerMetadata is the footer key-value map the read path hands to
// tokenBloomSkip.
func footerMetadata(t *testing.T, data []byte) (map[string]string, int) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{}
	for _, kv := range f.Metadata().KeyValueMetadata {
		meta[kv.Key] = kv.Value
	}
	return meta, len(f.RowGroups())
}

// survivingRowGroups is the set of row groups a body-token search still reads.
func survivingRowGroups(meta map[string]string, rowGroups int, tokens []string) []int {
	var out []int
	for rg := 0; rg < rowGroups; rg++ {
		if !tokenBloomSkip(meta, rg, tokens) {
			out = append(out, rg)
		}
	}
	return out
}

// A body-token query reads the same row groups of a compacted file as of a
// flushed one: the footer token bloom prunes both, and a word that is in one row
// group only leaves one row group to read (the 63.6x extra body bytes of #344).
func TestBodyTokenSearchPrunesCompactedLikeFlushed(t *testing.T) {
	const rg = 1000
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved; compaction.SetSlotResolver(nil) })
	compaction.SetSlotResolver(nil)

	rows := goldenCompactionLogRows(4*rg + 500)
	flushed, err := writeLogsParquet(rows, rg, 3)
	if err != nil {
		t.Fatal(err)
	}
	a, b := splitInterleaved(rows)
	fa, _ := writeLogsParquet(a, rg, 3)
	fb, _ := writeLogsParquet(b, rg, 3)
	compacted := compactObjects(t, config.ModeLogs, [][]byte{fa.Data, fb.Data}, rg)

	fm, fn := footerMetadata(t, flushed.Data)
	cm, cn := footerMetadata(t, compacted)
	if fn != cn || fn != 5 {
		t.Fatalf("row groups: flushed %d, compacted %d, want 5", fn, cn)
	}
	for _, c := range []struct {
		name   string
		tokens []string
		// minPruned: the search must skip at least this many of the 5 row groups.
		minPruned int
	}{
		{"rare: a number that is in one row group only", []string{"2346"}, 4},
		{"absent: a word in no row group", []string{"zzzabsent"}, 5},
		{"common: a word in every row group", []string{"processed"}, 0},
	} {
		want := survivingRowGroups(fm, fn, c.tokens)
		got := survivingRowGroups(cm, cn, c.tokens)
		if fmt.Sprint(want) != fmt.Sprint(got) {
			t.Errorf("%s: the search reads row groups %v of the flushed file and %v of the compacted file", c.name, want, got)
		}
		if pruned := fn - len(got); pruned < c.minPruned {
			t.Errorf("%s: the compacted file prunes %d row groups, want at least %d (read %v)", c.name, pruned, c.minPruned, got)
		}
	}
}

// The row-group schedule of a compaction output is the one the token blooms are
// keyed by: the footer holds exactly one bloom per output row group, every body
// token of a row group passes its own bloom (a false negative would hide rows),
// and the row groups are cut by row count.
func TestCompactedTokenBloomsMatchTheirRowGroups(t *testing.T) {
	const inRG, outRG = 1000, 1700
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved; compaction.SetSlotResolver(nil) })
	compaction.SetSlotResolver(nil)

	rows := goldenCompactionLogRows(3*inRG + 250)
	a, b := splitInterleaved(rows)
	fa, _ := writeLogsParquet(a, inRG, 3)
	fb, _ := writeLogsParquet(b, inRG, 3)
	compacted := compactObjects(t, config.ModeLogs, [][]byte{fa.Data, fb.Data}, outRG)

	meta, rgs := footerMetadata(t, compacted)
	if want := (len(rows) + outRG - 1) / outRG; rgs != want {
		t.Fatalf("compacted object has %d row groups, want ceil(%d/%d) = %d", rgs, len(rows), outRG, want)
	}
	for rg := 0; rg < rgs; rg++ {
		if _, ok := meta[parquetwriter.TokenBloomKey(rg)]; !ok {
			t.Fatalf("row group %d of the compacted object has no body token bloom; footer keys %v", rg, structureOf(t, compacted).KeyNames())
		}
	}
	for rg := 0; rg < rgs; rg++ {
		start, end := rg*outRG, min((rg+1)*outRG, len(rows))
		for i := start; i < end; i++ {
			if rows[i].Body == "" {
				continue
			}
			if tokenBloomSkip(meta, rg, parquetwriter.Tokenize(rows[i].Body)) {
				t.Fatalf("row %d (row group %d): body %q is pruned by its own row group's token bloom", i, rg, rows[i].Body)
			}
		}
	}
}
