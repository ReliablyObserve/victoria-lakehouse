//go:build parity

package parity

// Per-tenant read-scope parity (divergence B6 in docs/parity-and-gaps.md).
//
// A read carrying tenant X's headers must answer with tenant X's rows and
// nothing else, on the cold tier exactly as on the hot tier. These tests pin
// that as an exact count rather than a directional check: the difference
// between "correct" and "leaking" is the presence of another tenant's rows,
// which a >= assertion cannot see.
//
// The logs side seeds its own two-tenant corpus so the expected numbers are
// known constants rather than whatever the shared seed happened to produce.
// It is written far enough in the past (isolationHoursBack) to sit outside
// the window every other test in this suite asks for, so it can never
// perturb another comparison. The traces side uses the two tenants the
// parity compose already seeds and takes hot VT as the reference.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// isolationHoursBack places the corpus well outside seedWindowHours (and
	// therefore outside every other test's range) while staying inside the
	// 48h retention the parity compose configures on VL and VT.
	isolationHoursBack = 40

	// Deliberately different counts: if a tier answers tenant A's query with
	// both tenants' rows, the number it returns (65) matches neither
	// expectation, so the failure names the leak instead of looking like an
	// off-by-one.
	isolationRowsTenantA = 40
	isolationRowsTenantB = 25

	isolationAccountA = "0" // the default tenant every unscoped read lands on
	isolationAccountB = "2"
	isolationProject  = "0"
)

// isolationServices gives each tenant a DISJOINT set of service.name values.
// The cold tier reports a flat hits:1 for every value it returns from
// field_values, so a hit count cannot tell a leak from a miscount there —
// but the value set can: a tier that leaks answers either tenant with all
// three services.
var isolationServices = map[string][]string{
	isolationAccountA: {"api-gateway", "user-service"},
	isolationAccountB: {"order-service"},
}

// TestTenantIsolation_Logs_PerTenantCounts asserts that each logs tier
// answers a tenant-scoped read with exactly that tenant's rows, across the
// three endpoints a leak shows up on: row retrieval, the hits histogram and
// the field-value index.
func TestTenantIsolation_Logs_PerTenantCounts(t *testing.T) {
	seedIsolationCorpus(t)

	tenants := []struct {
		account string
		want    int
	}{
		{isolationAccountA, isolationRowsTenantA},
		{isolationAccountB, isolationRowsTenantB},
	}

	for _, tier := range []struct{ name, base string }{
		{"VL", vlBaseURL},
		{"LH", lhBaseURL},
	} {
		t.Run(tier.name, func(t *testing.T) {
			for _, tn := range tenants {
				t.Run("account_"+tn.account, func(t *testing.T) {
					t.Run("query_rows", func(t *testing.T) {
						got := isolationQueryRows(t, tier.base, tn.account)
						assertTenantCount(t, "query", tn.account, tn.want, got)
					})
					t.Run("hits_total", func(t *testing.T) {
						got := isolationHitsTotal(t, tier.base, tn.account)
						assertTenantCount(t, "hits", tn.account, tn.want, got)
					})
					t.Run("field_values_set", func(t *testing.T) {
						want := sortedStrings(isolationServices[tn.account])
						got := sortedStrings(isolationFieldValues(t, tier.base, tn.account, "service.name"))
						assertTenantValues(t, tn.account, want, got)
					})
				})
			}
		})
	}
}

// TestTenantIsolation_Traces_PerTenantParity asserts the same property on the
// traces binary, against the two tenants the parity compose seeds. Hot VT is
// the reference: whatever it reports for a tenant is that tenant's share, and
// cold must report exactly the same.
func TestTenantIsolation_Traces_PerTenantParity(t *testing.T) {
	tenants := listTenants(t, lhtBaseURL)
	if len(tenants) < 2 {
		t.Fatalf("need two seeded tenants to test traces isolation, got %d — "+
			"check the datagen-seed-tenant2 service in tests/parity/docker-compose.yml",
			len(tenants))
	}

	for _, te := range tenants {
		t.Run("account_"+te.AccountID, func(t *testing.T) {
			t.Run("query_rows", func(t *testing.T) {
				hot := tracesTenantQueryRows(t, vtBaseURL, te.AccountID, te.ProjectID)
				cold := tracesTenantQueryRows(t, lhtBaseURL, te.AccountID, te.ProjectID)
				assertTenantCount(t, "traces query", te.AccountID, hot, cold)
			})
			t.Run("hits_total", func(t *testing.T) {
				hot := tracesTenantHitsTotal(t, vtBaseURL, te.AccountID, te.ProjectID)
				cold := tracesTenantHitsTotal(t, lhtBaseURL, te.AccountID, te.ProjectID)
				assertTenantCount(t, "traces hits", te.AccountID, hot, cold)
			})
			t.Run("field_values_hits", func(t *testing.T) {
				hot := tracesTenantFieldValueHits(t, vtBaseURL, te.AccountID, te.ProjectID, "kind")
				cold := tracesTenantFieldValueHits(t, lhtBaseURL, te.AccountID, te.ProjectID, "kind")
				assertTenantCount(t, "traces field_values", te.AccountID, hot, cold)
			})
		})
	}
}

func assertTenantValues(t *testing.T, account string, want, got []string) {
	t.Helper()
	if strings.Join(want, ",") != strings.Join(got, ",") {
		t.Errorf("field_values scoped to account %s returned %v, want %v — a read "+
			"carrying one tenant's headers must see that tenant's values and no "+
			"others (see B6 in docs/parity-and-gaps.md)", account, got, want)
	}
	t.Logf("field_values account=%s values=%v (want %v)", account, got, want)
}

func assertTenantCount(t *testing.T, what, account string, want, got int) {
	t.Helper()
	if got != want {
		t.Errorf("%s scoped to account %s returned %d rows, want %d — a read "+
			"carrying one tenant's headers must see that tenant's rows and no "+
			"others (see B6 in docs/parity-and-gaps.md)",
			what, account, got, want)
	}
	t.Logf("%s account=%s rows=%d (want %d)", what, account, got, want)
}

// --- the isolation corpus -------------------------------------------------

// isolationAnchor is where the corpus is written, and the centre of the
// window it is read back through. The anchor is recomputed from the clock on
// every call, so rows written at the anchor must sit in the MIDDLE of the
// read window — anchoring them at an edge means the few minutes between the
// seed and the read slide the edge straight past them, and the readback
// finds nothing.
func isolationAnchor() time.Time {
	return time.Now().Add(-isolationHoursBack * time.Hour)
}

func isolationWindow() (start, end time.Time) {
	base := isolationAnchor()
	return base.Add(-time.Hour), base.Add(time.Hour)
}

func isolationParams() url.Values {
	start, end := isolationWindow()
	return url.Values{
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
}

// seedIsolationCorpus writes the two-tenant corpus to both logs tiers, once.
// Re-running the suite against a live stack would otherwise double every
// count, so it first checks whether the corpus is already there.
func seedIsolationCorpus(t *testing.T) {
	t.Helper()
	if isolationQueryRows(t, vlBaseURL, isolationAccountA) == isolationRowsTenantA &&
		isolationQueryRows(t, vlBaseURL, isolationAccountB) == isolationRowsTenantB {
		t.Log("isolation corpus already present — skipping seed")
		return
	}

	for _, tn := range []struct {
		account string
		rows    int
	}{
		{isolationAccountA, isolationRowsTenantA},
		{isolationAccountB, isolationRowsTenantB},
	} {
		body := isolationNDJSON(tn.account, tn.rows)
		for _, base := range []string{vlBaseURL, lhBaseURL} {
			pushIsolationRows(t, base, tn.account, body)
		}
	}

	// Wait for hot to expose the rows and for cold to have flushed them.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		hotA := isolationQueryRows(t, vlBaseURL, isolationAccountA)
		hotB := isolationQueryRows(t, vlBaseURL, isolationAccountB)
		coldTotal := isolationQueryRows(t, lhBaseURL, isolationAccountA) +
			isolationQueryRows(t, lhBaseURL, isolationAccountB)
		if hotA == isolationRowsTenantA && hotB == isolationRowsTenantB &&
			coldTotal >= isolationRowsTenantA+isolationRowsTenantB {
			t.Logf("isolation corpus visible: hot a=%d b=%d, cold total=%d", hotA, hotB, coldTotal)
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatal("isolation corpus never became visible on both tiers within 2m — " +
		"ingest or flush problem, not a tenant-scope result")
}

// isolationNDJSON builds `rows` log lines inside the isolation window. Field
// names and values are the ones cmd/datagen already writes, so the corpus
// introduces no new field or value anywhere in the suite.
func isolationNDJSON(account string, rows int) []byte {
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	services := isolationServices[account]
	anchor := isolationAnchor()

	var buf bytes.Buffer
	for i := 0; i < rows; i++ {
		line := map[string]any{
			"_time":                  anchor.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			"_msg":                   fmt.Sprintf("tenant-isolation probe account=%s seq=%d", account, i),
			"level":                  levels[i%len(levels)],
			"service.name":           services[i%len(services)],
			"k8s.namespace.name":     "production",
			"deployment.environment": "production",
			"cloud.region":           "us-east-1",
			"host.name":              "ip-10-0-1-42",
		}
		enc, _ := json.Marshal(line)
		buf.Write(enc)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func pushIsolationRows(t *testing.T, base, account string, body []byte) {
	t.Helper()
	u := base + "/insert/jsonline?_stream_fields=service.name,k8s.namespace.name," +
		"deployment.environment,cloud.region,host.name,level"
	req, err := http.NewRequest("POST", u, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build insert request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("AccountID", account)
	req.Header.Set("ProjectID", isolationProject)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("push isolation rows to %s: %v", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("push isolation rows to %s returned %d", base, resp.StatusCode)
	}
}

// --- per-tenant readers ---------------------------------------------------

// tenantFetch issues a GET with tenant headers, mirroring fetch().
func tenantFetch(t *testing.T, base, path string, params url.Values, account, project string) fetchResult {
	t.Helper()
	u := base + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("AccountID", account)
	req.Header.Set("ProjectID", project)
	resp, err := httpClient.Do(req)
	if err != nil {
		return fetchResult{StatusCode: 0, Body: []byte(err.Error())}
	}
	defer func() { _ = resp.Body.Close() }()
	body := readAllOrEmpty(resp)
	return fetchResult{StatusCode: resp.StatusCode, Body: body}
}

func isolationQueryRows(t *testing.T, base, account string) int {
	t.Helper()
	params := isolationParams()
	params.Set("query", "*")
	params.Set("limit", "10000")
	r := tenantFetch(t, base, "/select/logsql/query", params, account, isolationProject)
	if r.StatusCode != 200 {
		return -1
	}
	return len(parseNDJSON(r.Body))
}

func isolationHitsTotal(t *testing.T, base, account string) int {
	t.Helper()
	params := isolationParams()
	params.Set("query", "*")
	params.Set("step", "3600s")
	r := tenantFetch(t, base, "/select/logsql/hits", params, account, isolationProject)
	if r.StatusCode != 200 {
		return -1
	}
	_, counts := extractHitsBuckets(r.Body)
	total := 0.0
	for _, c := range counts {
		total += c
	}
	return int(total)
}

func isolationFieldValues(t *testing.T, base, account, field string) []string {
	t.Helper()
	params := isolationParams()
	params.Set("query", "*")
	params.Set("field", field)
	r := tenantFetch(t, base, "/select/logsql/field_values", params, account, isolationProject)
	if r.StatusCode != 200 {
		t.Fatalf("field_values returned %d: %s", r.StatusCode, string(r.Body))
	}
	return extractValuesStrings(r.Body)
}

// --- traces readers (hot is the reference) --------------------------------

func tracesTenantQueryRows(t *testing.T, base, account, project string) int {
	t.Helper()
	params := seedWindowParams()
	params.Set("query", "span_id:* | stats count() n")
	r := tenantFetch(t, base, "/select/logsql/stats_query", params, account, project)
	if r.StatusCode != 200 {
		return -1
	}
	v, err := extractVectorCount(r.Body)
	if err != nil {
		return -1
	}
	return int(v)
}

func tracesTenantHitsTotal(t *testing.T, base, account, project string) int {
	t.Helper()
	params := seedWindowParams()
	params.Set("query", "span_id:*")
	params.Set("step", "3600s")
	r := tenantFetch(t, base, "/select/logsql/hits", params, account, project)
	if r.StatusCode != 200 {
		return -1
	}
	_, counts := extractHitsBuckets(r.Body)
	total := 0.0
	for _, c := range counts {
		total += c
	}
	return int(total)
}

func tracesTenantFieldValueHits(t *testing.T, base, account, project, field string) int {
	t.Helper()
	params := seedWindowParams()
	params.Set("query", "span_id:*")
	params.Set("field", field)
	r := tenantFetch(t, base, "/select/logsql/field_values", params, account, project)
	if r.StatusCode != 200 {
		return -1
	}
	return sumFieldValueHits(r.Body)
}
