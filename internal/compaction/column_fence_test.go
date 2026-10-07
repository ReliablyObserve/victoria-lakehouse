package compaction

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The forward fence: an object with a column the running code does not model
// is never merged, because the merge would drop that column for good.

type futureLog struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano,delta"`
	Body              string `parquet:"body"`
	ServiceName       string `parquet:"service.name,dict"`
	Future            string `parquet:"future.column,optional"`
}

type futureTrace struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano,delta"`
	TraceID           string `parquet:"trace_id"`
	SpanID            string `parquet:"span_id"`
	ServiceName       string `parquet:"service.name,dict"`
	Future            string `parquet:"future.column,optional"`
}

func (s *compactSetup) addFutureFile(t *testing.T, signal string, idx int) manifest.FileInfo {
	t.Helper()
	var data []byte
	prefix := signal + "/"
	if signal == "logs" {
		var rows []futureLog
		for i := 0; i < 3; i++ {
			rows = append(rows, futureLog{int64(5000 + idx*10 + i), fmt.Sprintf("future-%d-%d", idx, i), "svc", "kept"})
		}
		data = writeParquetT(t, rows)
	} else {
		var rows []futureTrace
		for i := 0; i < 3; i++ {
			rows = append(rows, futureTrace{int64(5000 + idx*10 + i), fmt.Sprintf("ft-%d-%d", idx, i), fmt.Sprintf("fs%d%d", idx, i), "svc", "kept"})
		}
		data = writeParquetT(t, rows)
	}
	key := fmt.Sprintf("%s%s/future-%03d.parquet", prefix, s.partition, idx)
	if err := s.pool.Upload(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	fp := "fp-logs-v1"
	if signal == "traces" {
		fp = "fp-traces-v1"
	}
	fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 3, MinTimeNs: int64(5000 + idx*10), MaxTimeNs: int64(5002 + idx*10), SchemaFingerprint: fp}
	s.manifest.AddFile(s.partition, fi)
	return fi
}

func writeParquetT[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func fencedSetup(t *testing.T, signal string) (*compactSetup, func(int, int) manifest.FileInfo) {
	t.Helper()
	if signal == "logs" {
		s := setupLogCompactor(t)
		return s, func(idx, n int) manifest.FileInfo {
			var rows []schema.LogRow
			for i := 0; i < n; i++ {
				rows = append(rows, schema.LogRow{TimestampUnixNano: int64(1000 + idx*10 + i), Body: fmt.Sprintf("known-%d-%d", idx, i), ServiceName: "svc"})
			}
			return s.addLogFile(t, idx, rows)
		}
	}
	s := setupTraceCompactor(t)
	return s, func(idx, n int) manifest.FileInfo {
		var rows []schema.TraceRow
		for i := 0; i < n; i++ {
			rows = append(rows, schema.TraceRow{TimestampUnixNano: int64(1000 + idx*10 + i), TraceID: fmt.Sprintf("k-%d-%d", idx, i), SpanID: fmt.Sprintf("ks%d%d", idx, i), ServiceName: "svc"})
		}
		return s.addTraceFile(t, idx, rows)
	}
}

func rowCount(t *testing.T, signal string, data []byte) int {
	t.Helper()
	if signal == "logs" {
		r, err := readLogRows(data)
		if err != nil {
			t.Fatal(err)
		}
		return len(r)
	}
	r, err := readTraceRows(data)
	if err != nil {
		t.Fatal(err)
	}
	return len(r)
}

func TestCompactor_Fence_SkipsUnknownColumnObjects(t *testing.T) {
	for _, signal := range []string{"logs", "traces"} {
		t.Run(signal, func(t *testing.T) {
			s, known := fencedSetup(t, signal)
			a, b := known(0, 2), known(1, 3)
			f := s.addFutureFile(t, signal, 2)
			futureBytes := append([]byte(nil), s.pool.get(f.Key)...)
			before := metrics.SkippedUnknownColumns(signal, "compact").Get()

			res, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{a, b, f}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.InputFiles) != 2 {
				t.Fatalf("merged %v, want only the two known objects", res.InputFiles)
			}
			for _, k := range res.InputFiles {
				if k == f.Key {
					t.Fatalf("the unknown-column object %s was merged", k)
				}
			}
			if got := rowCount(t, signal, s.pool.get(res.OutputFile)); got != 5 {
				t.Errorf("output has %d rows, want 5", got)
			}
			if !bytes.Equal(s.pool.get(f.Key), futureBytes) {
				t.Error("the unknown-column object was modified or deleted")
			}
			if !s.manifest.HasKey(f.Key) {
				t.Error("the unknown-column object left the manifest")
			}
			if metrics.SkippedUnknownColumns(signal, "compact").Get() != before+1 {
				t.Error("lakehouse_compaction_skipped_unknown_columns_total did not count the skip")
			}
		})
	}
}

// With fewer than two mergeable objects left, nothing is rewritten at all.
func TestCompactor_Fence_NothingToMergeLeavesEverything(t *testing.T) {
	for _, signal := range []string{"logs", "traces"} {
		t.Run(signal, func(t *testing.T) {
			s, known := fencedSetup(t, signal)
			a := known(0, 2)
			f1, f2 := s.addFutureFile(t, signal, 1), s.addFutureFile(t, signal, 2)
			res, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{a, f1, f2}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.InputFiles) != 0 || len(res.OutputFiles) != 0 {
				t.Fatalf("a merge happened: %+v", res)
			}
			for _, k := range []string{a.Key, f1.Key, f2.Key} {
				if s.pool.get(k) == nil || !s.manifest.HasKey(k) {
					t.Errorf("%s was removed", k)
				}
			}
		})
	}
}

// A schema-known set is merged exactly as before the fence existed.
func TestCompactor_Fence_KnownSchemaMergesAsBefore(t *testing.T) {
	for _, signal := range []string{"logs", "traces"} {
		t.Run(signal, func(t *testing.T) {
			s, known := fencedSetup(t, signal)
			a, b, c := known(0, 2), known(1, 3), known(2, 4)
			res, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{a, b, c}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.InputFiles) != 3 || rowCount(t, signal, s.pool.get(res.OutputFile)) != 9 {
				t.Fatalf("known objects not merged: %+v", res)
			}
		})
	}
}

// Property: whatever mix of known and unknown-column objects is offered, no
// unknown-column object is ever merged, rewritten or removed, and every row of
// the known ones is in the output.
func TestCompactor_Fence_PropertyNoUnknownInputIsRewritten(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for iter := 0; iter < 30; iter++ {
		signal := []string{"logs", "traces"}[r.Intn(2)]
		s, known := fencedSetup(t, signal)
		var files []manifest.FileInfo
		orig := map[string][]byte{}
		knownRows := 0
		n := 2 + r.Intn(6)
		for i := 0; i < n; i++ {
			if r.Intn(3) == 0 {
				f := s.addFutureFile(t, signal, i)
				orig[f.Key] = append([]byte(nil), s.pool.get(f.Key)...)
				files = append(files, f)
			} else {
				rows := 1 + r.Intn(4)
				files = append(files, known(i, rows))
				knownRows += rows
			}
		}
		res, err := s.compactor.Compact(context.Background(), s.partition, files, 0)
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		for k, b := range orig {
			if !bytes.Equal(s.pool.get(k), b) || !s.manifest.HasKey(k) {
				t.Fatalf("iter %d: unknown-column object %s was changed", iter, k)
			}
			for _, in := range res.InputFiles {
				if in == k {
					t.Fatalf("iter %d: unknown-column object %s was merged", iter, k)
				}
			}
		}
		if len(res.OutputFiles) > 0 {
			if got := rowCount(t, signal, s.pool.get(res.OutputFile)); got != knownRows {
				t.Fatalf("iter %d: output has %d rows, known inputs had %d", iter, got, knownRows)
			}
		}
	}
}
