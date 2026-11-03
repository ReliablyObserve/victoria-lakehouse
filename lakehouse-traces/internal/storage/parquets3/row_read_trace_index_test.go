package parquets3

// Trace-by-ID reads the footer index of every file first (LookupTraceIndex) and
// uses the bound it aggregates for the span fetch. A footer that cannot be read
// leaves its file's entry unknown: the bound of the files that could be read may
// be narrower than the trace, so the lookup reports a miss and VictoriaTraces'
// span scan, which fails when it cannot read the file, answers instead (issue
// #503: the aggregate of the readable files used to be returned as if complete). A file
// that is gone leaves its entry unknown in the same way (#772).

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

func traceIndexFixture(t *testing.T) (*faultS3, *Storage, []string) {
	t.Helper()
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Mode = config.ModeTraces
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	keys := []string{"traces/dt=2026-06-01/hour=10/ti-0.parquet", "traces/dt=2026-06-01/hour=10/ti-1.parquet", "traces/dt=2026-06-01/hour=10/ti-2.parquet"}
	entries := [][]TraceIndexEntry{
		{{TraceID: "trace-xyz", StartNs: 1_000, EndNs: 5_000}},
		{{TraceID: "trace-xyz", StartNs: 500, EndNs: 3_000}}, // the earliest start of the trace is here
		{{TraceID: "trace-other", StartNs: 9_000, EndNs: 9_500}},
	}
	for i, k := range keys {
		registerFileInMockS3(t, s, fs.mock, k, writeParquetWithTraceIndex(t, entries[i], 4), base)
	}
	return fs, s, keys
}

func TestLookupTraceIndex_UnreadableFooterBeatsTheAggregateOfTheOthers(t *testing.T) {
	t.Run("control: every footer readable", func(t *testing.T) {
		_, s, _ := traceIndexFixture(t)
		start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
		if err != nil || !found || start != 500 || end != 5_000 {
			t.Fatalf("got (%d, %d, found=%v, err=%v), want (500, 5000, true, nil)", start, end, found, err)
		}
	})
	t.Run("a footer that cannot be read: a miss, so the scan answers", func(t *testing.T) {
		fs, s, keys := traceIndexFixture(t)
		fs.setFault(keys[1], accessDenied)
		start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
		if err != nil || found {
			t.Fatalf("got (%d, %d, found=%v, err=%v): the bound of the readable files (1000..5000) would hide spans of the unreadable one", start, end, found, err)
		}
	})
	// A file that is gone (removed by a compaction or retention run, #772) leaves
	// its index entry unknown, like an unreadable footer: its rows are in an object
	// the lookup did not list, and it may hold spans before the start or after the
	// end of the bound the other files give. A miss sends the lookup to the span
	// scan, which fails with 502 on the gone object.
	for _, gone := range []struct {
		name string
		idx  int
	}{
		{"a file that is gone and does not list the trace", 2},
		{"the file with the earliest start of the trace is gone", 1},
	} {
		t.Run(gone.name+": a miss, so the scan answers", func(t *testing.T) {
			fs, s, keys := traceIndexFixture(t)
			fs.mock.mu.Lock()
			delete(fs.mock.files, keys[gone.idx])
			fs.mock.mu.Unlock()
			start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
			if err != nil || found {
				t.Fatalf("got (%d, %d, found=%v, err=%v): the bound of the files still there (1000..5000) would hide the spans of the removed one", start, end, found, err)
			}
		})
	}

	// The in-RAM pmeta bloom facet (no S3 read) can prove that a gone file never
	// held the trace: it cannot narrow the bound, so the others' bound stands (no
	// loss of the fast path). A facet that may contain the trace, or cannot answer,
	// leaves the entry unknown. An unreadable (not gone) footer is never excluded.
	facet := func(s *Storage, keys []string, holdsTrace map[int]bool, otherPartition bool) {
		s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "traces/")
		for i, k := range keys {
			ids := []string{"trace-other"}
			if holdsTrace[i] {
				ids = []string{"trace-xyz"}
			}
			partition := manifest.ExtractTenantPartition(k)
			if otherPartition {
				partition += "-elsewhere" // a facet exists, but not for these files' partition
			}
			s.catalog.OnFileFlush(pmeta.FileContribution{Partition: partition, FileKey: k,
				BloomValues: map[string][]string{"trace_id": ids}})
		}
	}
	for _, c := range []struct {
		name       string
		gone       int
		unreadable bool
		holds      map[int]bool
		noFacet    bool
		elsewhere  bool
		wantFound  bool
	}{
		{name: "a gone file the facet excludes still gives a hit", gone: 2, holds: map[int]bool{0: true, 1: true}, wantFound: true},
		{name: "a gone file the facet may contain is a miss", gone: 1, holds: map[int]bool{0: true, 1: true}},
		{name: "a gone file with no facet to ask is a miss", gone: 2, noFacet: true},
		{name: "a gone file whose partition has no facet is a miss", gone: 2, elsewhere: true, holds: map[int]bool{0: true, 1: true}},
		{name: "an unreadable file the facet excludes is still a miss", gone: 2, unreadable: true, holds: map[int]bool{0: true, 1: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			fs, s, keys := traceIndexFixture(t)
			if !c.noFacet {
				facet(s, keys, c.holds, c.elsewhere)
			}
			if c.unreadable {
				fs.setFault(keys[c.gone], accessDenied)
			} else {
				fs.mock.mu.Lock()
				delete(fs.mock.files, keys[c.gone])
				fs.mock.mu.Unlock()
			}
			start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
			if err != nil || found != c.wantFound {
				t.Fatalf("got (%d, %d, found=%v, err=%v), want found=%v", start, end, found, err, c.wantFound)
			}
			if c.wantFound && (start != 500 && start != 1_000 || end != 5_000) {
				t.Fatalf("bound (%d, %d), want the bound of the files still there", start, end)
			}
		})
	}
}

// The VictoriaTraces index lookup through the adapter: with an unreadable file
// the lookup is not answered from the footers of the others, and the scan that
// replaces it fails instead of returning the trace without that file's spans.
func TestTraceByIDIndexQuery_UnreadableObjectFailsTheLookup(t *testing.T) {
	run := func(fs *faultS3, s *Storage) ([]map[string]string, error) {
		vtstorageadapter.Init(s)
		t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
		qs := fmt.Sprintf(`{%s="0"} %s:=%q | stats min(_time) as _time, min(%s) as %s, max(%s) as %s | limit 10`,
			otelpb.TraceIDIndexStreamName, otelpb.TraceIDIndexFieldName, "trace-xyz",
			otelpb.TraceIDIndexStartTimeFieldName, otelpb.TraceIDIndexStartTimeFieldName,
			otelpb.TraceIDIndexEndTimeFieldName, otelpb.TraceIDIndexEndTimeFieldName)
		q := mustParseQueryWithTime(t, qs, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixNano())
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows := blockRowFields([]*logstorage.DataBlock{db})
			mu.Lock()
			out = append(out, rows...)
			mu.Unlock()
		})
		return out, err
	}

	fs, s, _ := traceIndexFixture(t)
	if rows, err := run(fs, s); err != nil || len(rows) != 1 {
		t.Fatalf("control: %d rows, err %v; want the one index row", len(rows), err)
	}

	fs, s, keys := traceIndexFixture(t)
	fs.setFault(keys[1], accessDenied)
	if rows, err := run(fs, s); err == nil {
		t.Fatalf("the index lookup answered %d rows with an unreadable object", len(rows))
	}

	// The same for an object removed meanwhile: the index lookup does not answer
	// from the footers of the others, and the scan that replaces it fails with 502.
	fs, s, keys = traceIndexFixture(t)
	fs.mock.mu.Lock()
	delete(fs.mock.files, keys[1])
	fs.mock.mu.Unlock()
	rows, err := run(fs, s)
	if err == nil {
		t.Fatalf("the index lookup answered %d rows with a removed object", len(rows))
	}
	var esc *httpserver.ErrorWithStatusCode
	if !errors.As(err, &esc) || esc.StatusCode != http.StatusBadGateway {
		t.Fatalf("the index lookup failed with %v, want status 502", err)
	}
}

// checkFileBloom prunes by the in-RAM pmeta bloom facet only when the facet can
// answer for the file's partition: a facet that exists only for another partition
// (or an empty one) cannot prune, and the per-file `.bloom` fallback is used (one
// GET). The facet part is shared with the gone-object gate of LookupTraceIndex.
func TestCheckFileBloom_FacetOfAnotherPartitionDoesNotPrune(t *testing.T) {
	query := `trace_id:="trace-xyz"`
	install := func(s *Storage, keys []string, partitionSuffix string) {
		s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "traces/")
		for _, k := range keys {
			s.catalog.OnFileFlush(pmeta.FileContribution{Partition: manifest.ExtractTenantPartition(k) + partitionSuffix, FileKey: k,
				BloomValues: map[string][]string{"trace_id": {"trace-other"}}})
		}
	}
	check := func(t *testing.T, suffix string) (pruned bool, gets int64) {
		fs, s, keys := traceIndexFixture(t)
		install(s, keys, suffix)
		before := fs.gets.Load()
		fi := manifest.FileInfo{Key: keys[2]}
		return s.checkFileBloom(context.Background(), fi, query), fs.gets.Load() - before
	}
	t.Run("a facet that excludes the file prunes it without a read", func(t *testing.T) {
		if pruned, gets := check(t, ""); !pruned || gets != 0 {
			t.Fatalf("pruned=%v gets=%d, want pruned by the facet with no GET", pruned, gets)
		}
	})
	t.Run("a facet of another partition cannot prune and the .bloom fallback is read", func(t *testing.T) {
		if pruned, gets := check(t, "-elsewhere"); pruned || gets != 1 {
			t.Fatalf("pruned=%v gets=%d, want not pruned and one .bloom GET", pruned, gets)
		}
	})
}
