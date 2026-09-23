package parquets3

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Twin of internal/storage/parquets3/field_values_sampled_index_test.go.
//
// The pmeta catalog is keyed by PARQUET column name (`span.name`,
// `service.name`), but VictoriaTraces requests name those fields `name` and
// `resource_attr:service.name`: the catalog lookup missed, and the miss fell
// through to the in-RAM label index, whose values are a sample of whichever
// file the first query opened. field_values then listed a subset of the values
// in range, depending on query order.

var (
	fvSpanNames = []string{"DELETE /d", "GET /a", "POST /b", "PUT /c"}
	fvServices  = []string{"svc-a", "svc-b", "svc-c", "svc-d"}
	fvStatuses  = []string{"status-a", "status-b", "status-c", "status-d"}
	fvMethods   = []string{"DELETE", "GET", "POST", "PUT"}
)

// seedSampledSpanIndex flushes a one-span partition and a four-span partition
// two hours later, then runs a query over the one-span partition only — which
// is what seeds the label index in production.
func seedSampledSpanIndex(t *testing.T, pmetaOn bool) (s *Storage, lo, hi int64, small time.Time) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s = testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	if pmetaOn {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}

	small = time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	full := small.Add(2 * time.Hour)
	rows := []schema.TraceRow{{TimestampUnixNano: small.UnixNano(), ServiceName: "svc-a", SpanName: "GET /a",
		StatusMessage: "status-a", HTTPMethod: "GET", TraceID: "t0", SpanID: "s0"}}
	for i := range fvSpanNames {
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: full.Add(time.Duration(i) * time.Second).UnixNano(),
			ServiceName:       fvServices[i], SpanName: fvSpanNames[i],
			StatusMessage: fvStatuses[i], HTTPMethod: fvMethods[i],
			TraceID: "t" + fvServices[i], SpanID: "s" + fvServices[i],
		})
	}
	bw.AddTraceRows(rows)
	bw.triggerFlush()

	lo, hi = small.Add(-time.Hour).UnixNano(), full.Add(time.Hour).UnixNano()

	q := mustParseQueryWithTime(t, "*", small.Truncate(time.Hour).UnixNano(), small.Truncate(time.Hour).Add(time.Hour).UnixNano())
	if err := s.RunQuery(context.Background(), nil, q, func(uint, *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	// Precondition: the label index now holds the one-span file's sample.
	if got := s.labelIndex.GetFieldValues("name", 0); len(got) != 1 || got[0] != "GET /a" {
		t.Fatalf("precondition: label index should hold the one-span file's sample [GET /a], got %v", got)
	}
	return s, lo, hi, small
}

func fieldValueSet(t *testing.T, s *Storage, lo, hi int64, field string, limit uint64) []string {
	t.Helper()
	q := mustParseQueryWithTime(t, "*", lo, hi)
	got, err := s.GetFieldValues(context.Background(), nil, q, field, limit)
	if err != nil {
		t.Fatalf("GetFieldValues(%s): %v", field, err)
	}
	out := make([]string, 0, len(got))
	for _, v := range got {
		out = append(out, v.Value)
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFieldValues_AliasedField_ServedFromCatalog(t *testing.T) {
	s, lo, hi, _ := seedSampledSpanIndex(t, true)

	cases := []struct {
		field string
		want  []string
	}{
		{"name", fvSpanNames},
		{"span.name", fvSpanNames},
		{"resource_attr:service.name", fvServices},
		{"service.name", fvServices},
		{"status_message", fvStatuses},
		{"status.message", fvStatuses},
		{"span_attr:http.method", fvMethods},
		{"http.method", fvMethods},
	}
	before := metrics.CatalogValueLookups.Get("catalog")
	for _, tc := range cases {
		for _, limit := range []uint64{0, 1000} {
			if got := fieldValueSet(t, s, lo, hi, tc.field, limit); !equalStrings(got, tc.want) {
				t.Errorf("field_values %s (limit=%d) = %v, want %v", tc.field, limit, got, tc.want)
			}
		}
	}
	if metrics.CatalogValueLookups.Get("catalog") < before+uint64(2*len(cases)) {
		t.Error("aliased trace fields were not all served from the pmeta catalog")
	}
}

func TestFieldValues_SampledLabelIndexIsNeverTheAnswer(t *testing.T) {
	s, lo, hi, small := seedSampledSpanIndex(t, false)

	for field, want := range map[string][]string{
		"name":                       fvSpanNames,
		"resource_attr:service.name": fvServices,
		"status_message":             fvStatuses,
		"span_attr:http.method":      fvMethods,
	} {
		for _, limit := range []uint64{0, 1000} {
			if got := fieldValueSet(t, s, lo, hi, field, limit); !equalStrings(got, want) {
				t.Errorf("field_values %s (limit=%d) = %v, want %v", field, limit, got, want)
			}
		}
	}
	empty := small.Add(48 * time.Hour)
	if got := fieldValueSet(t, s, empty.UnixNano(), empty.Add(time.Hour).UnixNano(), "name", 0); len(got) != 0 {
		t.Fatalf("field_values over an empty window = %v, want none", got)
	}
}

// TestFieldValues_AliasedField_CatalogStaysTenantScoped: the catalog answer for
// an aliased field unions only the requesting tenant's partitions (integer
// tenants at this layer; a string OrgID arrives as the integer its alias
// resolves to), and a tenant owning nothing gets nothing.
func TestFieldValues_AliasedField_CatalogStaysTenantScoped(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.manifest.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.catalogObserver = &catalogObserver{store: s.catalog}
	bw.SetTenantPrefix(func(a, p uint32) string { return fmt.Sprintf("%d/%d/traces/", a, p) })

	base := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	owned := map[logstorage.TenantID][]string{
		{AccountID: 1001}:               {"svc-a", "svc-b"},
		{AccountID: 2002, ProjectID: 7}: {"svc-c", "svc-d"},
	}
	var rows []schema.TraceRow
	i := 0
	for tn, svcs := range owned {
		for _, svc := range svcs {
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
				AccountID:         tn.AccountID, ProjectID: tn.ProjectID,
				ServiceName: svc, SpanName: "op", TraceID: fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("s%d", i),
			})
			i++
		}
	}
	bw.AddTraceRows(rows)
	bw.triggerFlush()
	s.labelIndex.Add("resource_attr:service.name", []string{"svc-a", "svc-b", "svc-c", "svc-d"})

	q := mustParseQueryWithTime(t, "*", base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	for tn, want := range owned {
		before := metrics.CatalogValueLookups.Get("catalog")
		vals, err := s.GetFieldValues(context.Background(), []logstorage.TenantID{tn}, q, "resource_attr:service.name", 0)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, 0, len(vals))
		for _, v := range vals {
			got = append(got, v.Value)
		}
		sort.Strings(got)
		if !equalStrings(got, want) {
			t.Errorf("tenant %d:%d field_values = %v, want its own %v", tn.AccountID, tn.ProjectID, got, want)
		}
		if metrics.CatalogValueLookups.Get("catalog") <= before {
			t.Errorf("tenant %d:%d was not served from the catalog", tn.AccountID, tn.ProjectID)
		}
	}
	vals, err := s.GetFieldValues(context.Background(), []logstorage.TenantID{{AccountID: 3003}}, q, "resource_attr:service.name", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 0 {
		t.Errorf("a tenant owning no objects got %v", vals)
	}
}

// TestCatalogFieldKey pins the request-name → facet-key mapping: promoted
// columns translate to their Parquet column, everything else keeps its name.
func TestCatalogFieldKey(t *testing.T) {
	s := &Storage{registry: schema.NewRegistry(schema.TracesProfile)}
	for in, want := range map[string]string{
		"name":                       "span.name",
		"span.name":                  "span.name",
		"status_message":             "status.message",
		"resource_attr:service.name": "service.name",
		"service.name":               "service.name",
		"span_attr:http.method":      "http.method",
		"resource_attr:custom.key":   "resource_attr:custom.key",
		"account_id":                 "account_id",
		// Non-label promoted columns resolve too, so a column catalogued later
		// cannot silently miss.
		"_stream":    "_stream",
		"_stream_id": "_stream_id",
		"_time":      "timestamp_unix_nano",
	} {
		if got := s.catalogFieldKey(in); got != want {
			t.Errorf("catalogFieldKey(%q) = %q, want %q", in, got, want)
		}
	}
	if got := (&Storage{}).catalogFieldKey("name"); got != "name" {
		t.Errorf("without a registry the name must pass through, got %q", got)
	}
}
