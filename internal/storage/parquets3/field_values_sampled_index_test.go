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

// The parity suite intermittently saw cold `field_values?field=level` answer a
// strict subset of hot's values (3 of 4, once 1 of 4) while every row count
// agreed. Two defects combined:
//
//  1. The pmeta catalog is keyed by PARQUET column name (`severity_text`), but
//     the request names the field by its VictoriaLogs name (`level`). The catalog
//     lookup therefore always missed for every aliased field (`level` on logs;
//     `name`, `status_message`, `resource_attr:*`, `span_attr:*` on traces).
//  2. The miss fell through to the in-RAM label index, whose values are a
//     SAMPLE — the first 512 rows of the first row group of whichever file the
//     first query happened to open (or of ≤10 files at warm-up) — and returned it
//     as the whole answer. Which file that is depends on query order, which is
//     what made the failure intermittent.
//
// These tests drive that production sequence: a query touches a small file
// first (seeding the label index from it), then field_values asks for the whole
// range.

var fvLevels = []string{"DEBUG", "ERROR", "INFO", "WARN"}

// seedSampledLevelIndex flushes a one-row partition (level INFO only) and a
// full partition (all four levels) two hours later, then runs a query over the
// one-row partition only — which is what seeds the label index in production.
// Returns the storage, the whole-range bounds and the one-row partition's hour.
func seedSampledLevelIndex(t *testing.T, pmetaOn bool) (s *Storage, lo, hi int64, small time.Time) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s = testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	if pmetaOn {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}

	small = time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	full := small.Add(2 * time.Hour)
	rows := []schema.LogRow{{TimestampUnixNano: small.UnixNano(), Body: "only", ServiceName: "svc", SeverityText: "INFO"}}
	for i, lvl := range fvLevels {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: full.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "row", ServiceName: "svc", SeverityText: lvl,
		})
	}
	bw.AddLogRows(rows)
	bw.triggerFlush()

	lo, hi = small.Add(-time.Hour).UnixNano(), full.Add(time.Hour).UnixNano()

	// The first query of the process opens the one-row file.
	q := mustParseQueryWithTime(t, "*", small.Truncate(time.Hour).UnixNano(), small.Truncate(time.Hour).Add(time.Hour).UnixNano())
	if err := s.RunQuery(context.Background(), nil, q, func(uint, *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	// Precondition: the label index now holds the sample, not the truth. If this
	// stops holding, the test no longer reproduces the production sequence.
	if got := s.labelIndex.GetFieldValues("level", 0); len(got) != 1 || got[0] != "INFO" {
		t.Fatalf("precondition: label index should hold the one-row file's sample [INFO], got %v", got)
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

// TestFieldValues_AliasedField_ServedFromCatalog: with pmeta on, `level` (stored
// as `severity_text`) is answered from the catalog — every value in range, not
// the label index's sample.
func TestFieldValues_AliasedField_ServedFromCatalog(t *testing.T) {
	s, lo, hi, _ := seedSampledLevelIndex(t, true)

	before := metrics.CatalogValueLookups.Get("catalog")
	for _, limit := range []uint64{0, 1000} {
		if got := fieldValueSet(t, s, lo, hi, "level", limit); !equalStrings(got, fvLevels) {
			t.Fatalf("field_values level (limit=%d) = %v, want %v", limit, got, fvLevels)
		}
	}
	if metrics.CatalogValueLookups.Get("catalog") <= before {
		t.Fatal("level was not served from the pmeta catalog")
	}
	// The Parquet column name keeps working too (operators type it).
	if got := fieldValueSet(t, s, lo, hi, "severity_text", 0); !equalStrings(got, fvLevels) {
		t.Fatalf("field_values severity_text = %v, want %v", got, fvLevels)
	}
}

// TestFieldValues_SampledLabelIndexIsNeverTheAnswer: with the catalog unable to
// answer (pmeta off here), field_values must come from the rows in range — the
// sampled, not time-scoped label index is neither complete nor window-bound.
func TestFieldValues_SampledLabelIndexIsNeverTheAnswer(t *testing.T) {
	s, lo, hi, small := seedSampledLevelIndex(t, false)

	for _, limit := range []uint64{0, 1000} {
		if got := fieldValueSet(t, s, lo, hi, "level", limit); !equalStrings(got, fvLevels) {
			t.Fatalf("field_values level (limit=%d) = %v, want %v", limit, got, fvLevels)
		}
	}
	// A window holding no rows has no values (upstream semantics), even though
	// the label index still remembers values from other hours.
	empty := small.Add(48 * time.Hour)
	if got := fieldValueSet(t, s, empty.UnixNano(), empty.Add(time.Hour).UnixNano(), "level", 0); len(got) != 0 {
		t.Fatalf("field_values over an empty window = %v, want none", got)
	}
}

// TestFieldValues_AliasedField_CatalogStaysTenantScoped: the catalog answer for
// an aliased field unions only the requesting tenant's partitions. Tenants are
// integer AccountID:ProjectID at this layer — a string OrgID reaches storage as
// the integer its alias resolves to — so 1001:0 and 2002:7 cover both shapes;
// a tenant that owns nothing gets nothing, even with the label index seeded.
func TestFieldValues_AliasedField_CatalogStaysTenantScoped(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.manifest.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}
	bw.SetTenantPrefix(func(a, p uint32) string { return fmt.Sprintf("%d/%d/logs/", a, p) })

	base := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	owned := map[logstorage.TenantID][]string{
		{AccountID: 1001}:               {"ERROR", "INFO"},
		{AccountID: 2002, ProjectID: 7}: {"DEBUG", "WARN"},
	}
	var rows []schema.LogRow
	i := 0
	for tn, lvls := range owned {
		for _, lvl := range lvls {
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
				AccountID:         tn.AccountID, ProjectID: tn.ProjectID,
				Body: "row", ServiceName: "svc", SeverityText: lvl,
			})
			i++
		}
	}
	bw.AddLogRows(rows)
	bw.triggerFlush()
	s.labelIndex.Add("level", []string{"DEBUG", "ERROR", "INFO", "WARN"})

	q := mustParseQueryWithTime(t, "*", base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	for tn, want := range owned {
		before := metrics.CatalogValueLookups.Get("catalog")
		vals, err := s.GetFieldValues(context.Background(), []logstorage.TenantID{tn}, q, "level", 0)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, 0, len(vals))
		for _, v := range vals {
			got = append(got, v.Value)
		}
		sort.Strings(got)
		if !equalStrings(got, want) {
			t.Errorf("tenant %d:%d field_values level = %v, want its own %v", tn.AccountID, tn.ProjectID, got, want)
		}
		if metrics.CatalogValueLookups.Get("catalog") <= before {
			t.Errorf("tenant %d:%d level was not served from the catalog", tn.AccountID, tn.ProjectID)
		}
	}
	vals, err := s.GetFieldValues(context.Background(), []logstorage.TenantID{{AccountID: 3003}}, q, "level", 0)
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
	s := testStorage()
	for in, want := range map[string]string{
		"level":              "severity_text",
		"severity_text":      "severity_text",
		"service.name":       "service.name",
		"k8s.namespace.name": "k8s.namespace.name",
		"account_id":         "account_id", // catalogued under its own name
		"log_attr:user.id":   "log_attr:user.id",
		"some.map.attribute": "some.map.attribute",
	} {
		if got := s.catalogFieldKey(in); got != want {
			t.Errorf("catalogFieldKey(%q) = %q, want %q", in, got, want)
		}
	}
	if got := (&Storage{}).catalogFieldKey("level"); got != "level" {
		t.Errorf("without a registry the name must pass through, got %q", got)
	}
}
