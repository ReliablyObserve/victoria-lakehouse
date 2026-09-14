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
// perturb another comparison, and for two tenants that own no other data, so
// the cold manifest's entries for them are exactly the corpus. The traces
// side uses the two tenants the parity compose already seeds and takes hot VT
// as the reference.

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

	// Tenants no other writer in the stack uses (datagen seeds logs into 0:0
	// and traces into 0:0 and 1:0), so everything the cold manifest lists
	// for them is the corpus — which is what lets seedIsolationCorpus tell
	// a flushed corpus from one still in the write path.
	isolationAccountA = "3"
	isolationAccountB = "4"
	isolationProject  = "0"

	// coldManifestRefreshInterval is -lakehouse.manifest.refresh-interval
	// on lakehouse-logs in tests/parity/docker-compose.yml.
	coldManifestRefreshInterval = 5 * time.Second
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
	tenants := requireSeededTenants(t)

	for _, te := range tenants {
		t.Run("account_"+te.AccountID, func(t *testing.T) {
			t.Run("query_rows", func(t *testing.T) {
				hot := tracesTenantQueryRows(t, vtBaseURL, te.AccountID, te.ProjectID)
				requireHotTenantReference(t, "traces query", te.AccountID, hot)
				cold := tracesTenantQueryRows(t, lhtBaseURL, te.AccountID, te.ProjectID)
				assertTenantCount(t, "traces query", te.AccountID, hot, cold)
			})
			t.Run("hits_total", func(t *testing.T) {
				hot := tracesTenantHitsTotal(t, vtBaseURL, te.AccountID, te.ProjectID)
				requireHotTenantReference(t, "traces hits", te.AccountID, hot)
				cold := tracesTenantHitsTotal(t, lhtBaseURL, te.AccountID, te.ProjectID)
				assertTenantCount(t, "traces hits", te.AccountID, hot, cold)
			})
			t.Run("field_values_hits", func(t *testing.T) {
				hot := tracesTenantFieldValueHits(t, vtBaseURL, te.AccountID, te.ProjectID, "kind")
				requireHotTenantReference(t, "traces field_values", te.AccountID, hot)
				cold := tracesTenantFieldValueHits(t, lhtBaseURL, te.AccountID, te.ProjectID, "kind")
				assertTenantCount(t, "traces field_values", te.AccountID, hot, cold)
			})
		})
	}
}

// requireHotTenantReference fails when the hot tier — the reference for a
// traces tenant — answered with nothing. The readers return -1 on a request
// or parse error and 0 on an empty answer, so without this guard a hot error
// matched by a cold error (-1 == -1), or an empty seed on both tiers (0 == 0),
// would pass as parity.
func requireHotTenantReference(t *testing.T, what, account string, hot int) {
	t.Helper()
	if hot <= 0 {
		t.Fatalf("%s: hot reference for account %s returned %d — seed or query defect, "+
			"not parity: every tenant the manifest lists holds seeded spans", what, account, hot)
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

// seedIsolationCorpus writes the two-tenant corpus to both logs tiers, once,
// and returns when the cold tier holds it in the state it then stays in.
// Re-running the suite against a live stack would otherwise double every
// count, so it first checks whether the corpus is already there.
//
// What a cold read answers for rows written seconds ago depends on where they
// are, not only on whether the read is scoped. Until the flush they are served
// from the local buffer, which honors the request's tenant. Once one tenant's
// file lands, the buffer only serves rows newer than the newest file in the
// window, so the other tenant's still-buffered rows vanish from every read.
// And a manifest refresh that listed the bucket just before an upload drops
// the new file again until the next refresh. On the current cold tier the
// reads therefore look correctly scoped while the corpus is buffered, answer
// both tenants with one tenant's rows mid-flush, and leak the union once
// settled — so assertions taken at the wrong moment passed or failed on timing
// alone.
//
// Readiness is never judged from the per-tenant counts under test: a sum of
// two leaking reads can reach the corpus size while one tenant's rows are
// missing entirely. The corpus is ready when, for three manifest refresh
// intervals without a change, hot answers each tenant with exactly its rows,
// the cold manifest lists files for both tenants (they own no other data, so
// those files are the corpus), and the cold answers stay the same.
func seedIsolationCorpus(t *testing.T) {
	t.Helper()
	if isolationQueryRows(t, vlBaseURL, isolationAccountA) == isolationRowsTenantA &&
		isolationQueryRows(t, vlBaseURL, isolationAccountB) == isolationRowsTenantB {
		t.Log("isolation corpus already present on hot — skipping seed")
	} else {
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
	}

	stableFor := 3 * coldManifestRefreshInterval
	deadline := time.Now().Add(2 * time.Minute)
	var stable isolationState
	var stableSince time.Time
	for {
		state := observeIsolationCorpus(t)
		switch {
		case !state.ready():
			stableSince = time.Time{}
		case stableSince.IsZero() || state != stable:
			stable, stableSince = state, time.Now()
		case time.Since(stableSince) >= stableFor:
			t.Logf("isolation corpus unchanged for %s: %s", stableFor, state)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("isolation corpus did not hold steady for %s within 2m (last: %s) — "+
				"ingest, flush or manifest problem, not a tenant-scope result", stableFor, state)
		}
		time.Sleep(time.Second)
	}
}

// isolationState is one observation of the isolation corpus on both tiers.
type isolationState struct {
	hotA, hotB   int  // rows hot VictoriaLogs answers each tenant with
	coldListed   bool // the cold manifest lists files for both tenants
	coldA, coldB int  // rows the cold tier answers each tenant with
}

// ready reports whether hot holds exactly the corpus and the cold tier has
// flushed it. The cold answers are deliberately not part of it.
func (s isolationState) ready() bool {
	return s.hotA == isolationRowsTenantA && s.hotB == isolationRowsTenantB && s.coldListed
}

func (s isolationState) String() string {
	return fmt.Sprintf("hot a=%d b=%d (want %d/%d), cold manifest lists both tenants=%v, cold a=%d b=%d",
		s.hotA, s.hotB, isolationRowsTenantA, isolationRowsTenantB, s.coldListed, s.coldA, s.coldB)
}

func observeIsolationCorpus(t *testing.T) isolationState {
	t.Helper()
	return isolationState{
		hotA:       isolationQueryRows(t, vlBaseURL, isolationAccountA),
		hotB:       isolationQueryRows(t, vlBaseURL, isolationAccountB),
		coldListed: coldManifestListsTenants(t, lhBaseURL, isolationProject, isolationAccountA, isolationAccountB),
		coldA:      isolationQueryRows(t, lhBaseURL, isolationAccountA),
		coldB:      isolationQueryRows(t, lhBaseURL, isolationAccountB),
	}
}

// coldManifestListsTenants reports whether the cold tier's manifest, read
// through /lakehouse/api/v1/tenants, holds at least one file for every given
// account under project.
func coldManifestListsTenants(t *testing.T, base, project string, accounts ...string) bool {
	t.Helper()
	r := fetch(t, base, "/lakehouse/api/v1/tenants", nil)
	if r.StatusCode != 200 {
		t.Fatalf("GET %s/lakehouse/api/v1/tenants returned %d: %s", base, r.StatusCode, string(r.Body))
	}
	var d struct {
		Tenants []tenantSummary `json:"tenants"`
	}
	if err := json.Unmarshal(r.Body, &d); err != nil {
		t.Fatalf("parse %s/lakehouse/api/v1/tenants: %v", base, err)
	}
	files := make(map[string]int64, len(d.Tenants))
	for _, te := range d.Tenants {
		if te.ProjectID == project {
			files[te.AccountID] = te.TotalFiles
		}
	}
	for _, account := range accounts {
		if files[account] <= 0 {
			return false
		}
	}
	return true
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
