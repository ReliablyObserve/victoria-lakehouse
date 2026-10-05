//go:build e2e

package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

// Auto-registered string tenants must never share an AccountID with a
// configured alias (the compose stack configures acme-corp:1001 and
// staging-team:1002), and a read must never register anything. Both signals.

const (
	reservedMin = uint64(1) << 31
	reservedMax = uint64(1)<<32 - 2
)

type stAlias struct {
	OrgID     string `json:"org_id"`
	AccountID uint32 `json:"account_id"`
	ProjectID uint32 `json:"project_id"`
}

func stAliases(t *testing.T, base string) []stAlias {
	t.Helper()
	resp, err := http.Get(base + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list aliases: status %d", resp.StatusCode)
	}
	var out struct {
		Aliases []stAlias `json:"aliases"`
	}
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("list aliases: %v: %s", err, b)
	}
	return out.Aliases
}

func stAliasOf(aliases []stAlias, orgID string) (stAlias, bool) {
	for _, a := range aliases {
		if a.OrgID == orgID {
			return a, true
		}
	}
	return stAlias{}, false
}

// stAssertNoSharedIDs fails when two aliases map to the same AccountID:ProjectID.
func stAssertNoSharedIDs(t *testing.T, aliases []stAlias) {
	t.Helper()
	owner := map[string]string{}
	for _, a := range aliases {
		k := fmt.Sprintf("%d:%d", a.AccountID, a.ProjectID)
		if prev, dup := owner[k]; dup {
			t.Errorf("OrgIDs %q and %q share the tenant ID %s", prev, a.OrgID, k)
		}
		owner[k] = a.OrgID
	}
}

func stInsertLog(t *testing.T, orgID, msg string) {
	t.Helper()
	line, _ := json.Marshal(map[string]any{
		"_time":        time.Now().Format(time.RFC3339Nano),
		"_msg":         msg,
		"service.name": "st-autoregister-svc",
		"level":        "INFO",
	})
	req, err := http.NewRequest("POST", logsBaseURL+"/insert/jsonline?_stream_fields=service.name", bytes.NewReader(line))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("X-Scope-OrgID", orgID)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("insert as %s: status %d: %s", orgID, resp.StatusCode, b)
	}
}

func stRandHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func stInsertSpan(t *testing.T, orgID, traceID, service string) {
	t.Helper()
	payload := map[string]any{"resourceSpans": []map[string]any{{
		"resource": map[string]any{"attributes": []map[string]any{
			{"key": "service.name", "value": map[string]any{"stringValue": service}},
		}},
		"scopeSpans": []map[string]any{{
			"scope": map[string]any{"name": "st-autoregister"},
			"spans": []map[string]any{{
				"traceId":           traceID,
				"spanId":            stRandHex(t, 8),
				"name":              "st-autoregister-span",
				"kind":              1,
				"startTimeUnixNano": fmt.Sprintf("%d", time.Now().Add(-2*time.Second).UnixNano()),
				"endTimeUnixNano":   fmt.Sprintf("%d", time.Now().Add(-time.Second).UnixNano()),
				"attributes":        []map[string]any{},
			}},
		}},
	}}}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", tracesBaseURL+"/insert/opentelemetry/v1/traces", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", orgID)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("span insert as %s: status %d: %s", orgID, resp.StatusCode, b)
	}
}

// stQueryField runs a LogsQL query as orgID and returns the values of field.
func stQueryField(t *testing.T, base, orgID, query, field string) []string {
	t.Helper()
	params := wideTimeParams()
	params.Set("query", query)
	params.Set("limit", "1000")
	status, body := httpGetWithOrgID(t, base, "/select/logsql/query", params, orgID)
	if status != http.StatusOK {
		t.Fatalf("query %q as %s: status %d: %s", query, orgID, status, body)
	}
	var out []string
	for _, row := range assertValidNDJSON(t, body) {
		if v, ok := row[field].(string); ok {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// stEventually polls fn until it returns "" or the deadline passes.
func stEventually(t *testing.T, d time.Duration, fn func() string) {
	t.Helper()
	deadline := time.Now().Add(d)
	var last string
	for {
		if last = fn(); last == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(last)
		}
		time.Sleep(time.Second)
	}
}

func stExpectSet(orgID string, got []string, want ...string) string {
	sort.Strings(want)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		return fmt.Sprintf("%s sees %v, want exactly %v", orgID, got, want)
	}
	return ""
}

func TestStringTenant_AutoRegisteredOrgIDNeverSharesAnID_Logs(t *testing.T) {
	nano := time.Now().UnixNano()
	newOrg := fmt.Sprintf("tenfix-new-%d", nano)
	marks := map[string]string{
		"acme-corp":    fmt.Sprintf("acme-mark-%d", nano),
		"staging-team": fmt.Sprintf("stg-mark-%d", nano),
		newOrg:         fmt.Sprintf("new-mark-%d", nano),
	}
	for org, m := range marks {
		stInsertLog(t, org, m)
	}

	aliases := stAliases(t, logsBaseURL)
	stAssertNoSharedIDs(t, aliases)
	if a, ok := stAliasOf(aliases, "acme-corp"); !ok || a.AccountID != 1001 || a.ProjectID != 0 {
		t.Fatalf("acme-corp = %+v %v, want 1001:0", a, ok)
	}
	if a, ok := stAliasOf(aliases, "staging-team"); !ok || a.AccountID != 1002 || a.ProjectID != 0 {
		t.Fatalf("staging-team = %+v %v, want 1002:0", a, ok)
	}
	a, ok := stAliasOf(aliases, newOrg)
	if !ok {
		t.Fatalf("%s was not registered by the insert", newOrg)
	}
	if uint64(a.AccountID) < reservedMin || uint64(a.AccountID) > reservedMax || a.ProjectID != 0 {
		t.Fatalf("%s = %d:%d, want an AccountID in [%d, %d] and ProjectID 0", newOrg, a.AccountID, a.ProjectID, reservedMin, reservedMax)
	}

	// Each OrgID sees its own line and none of the others'.
	word := fmt.Sprintf("%d", nano)
	for org, m := range marks {
		org, m := org, m
		stEventually(t, 90*time.Second, func() string {
			return stExpectSet(org, stQueryField(t, logsBaseURL, org, word, "_msg"), m)
		})
	}
}

func TestStringTenant_UnknownOrgIDReadIsEmptyAndDoesNotRegister_Logs(t *testing.T) {
	unknown := fmt.Sprintf("tenfix-unknown-%d", time.Now().UnixNano())

	for _, path := range []string{
		"/select/logsql/query",
		"/select/logsql/field_names",
		"/select/logsql/hits",
		"/select/logsql/streams",
	} {
		params := wideTimeParams()
		params.Set("query", "*")
		params.Set("limit", "10")
		if path == "/select/logsql/hits" {
			params.Set("step", "1h")
		}
		status, body := httpGetWithOrgID(t, logsBaseURL, path, params, unknown)
		if status != http.StatusOK {
			t.Fatalf("%s as an unknown OrgID: status %d: %s", path, status, body)
		}
		if path == "/select/logsql/query" && strings.TrimSpace(string(body)) != "" {
			t.Fatalf("an unknown OrgID got rows from %s: %s", path, body)
		}
	}
	if vals := stQueryField(t, logsBaseURL, unknown, "*", "_msg"); len(vals) != 0 {
		t.Fatalf("an unknown OrgID read %d rows", len(vals))
	}

	if _, ok := stAliasOf(stAliases(t, logsBaseURL), unknown); ok {
		t.Fatalf("a read registered %s", unknown)
	}
}

func TestStringTenant_AliasConflictRejected(t *testing.T) {
	for name, base := range map[string]string{"logs": logsBaseURL, "traces": tracesBaseURL} {
		base := base
		t.Run(name, func(t *testing.T) {
			post := func(org string, account uint64) int {
				b, _ := json.Marshal(map[string]any{"org_id": org, "account_id": account, "project_id": 0})
				req, _ := http.NewRequest("POST", base+"/lakehouse/api/v1/tenants/aliases", bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(withPeerKey(req))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = resp.Body.Close() }()
				_, _ = io.Copy(io.Discard, resp.Body)
				return resp.StatusCode
			}
			nano := time.Now().UnixNano()
			// Another name on acme-corp's ID, acme-corp on another ID, and an ID inside
			// the auto-register range: all refused with 409, nothing changes.
			if got := post(fmt.Sprintf("tenfix-squat-%d", nano), 1001); got != http.StatusConflict {
				t.Errorf("a second OrgID on 1001: status %d, want 409", got)
			}
			if got := post("acme-corp", 4242); got != http.StatusConflict {
				t.Errorf("acme-corp on a second ID: status %d, want 409", got)
			}
			if got := post(fmt.Sprintf("tenfix-range-%d", nano), reservedMin); got != http.StatusConflict {
				t.Errorf("an alias inside the reserved range: status %d, want 409", got)
			}
			aliases := stAliases(t, base)
			stAssertNoSharedIDs(t, aliases)
			if a, ok := stAliasOf(aliases, "acme-corp"); !ok || a.AccountID != 1001 {
				t.Errorf("acme-corp = %+v %v after the conflicts, want 1001", a, ok)
			}
		})
	}
}

func TestStringTenant_AutoRegisteredOrgIDNeverSharesAnID_Traces(t *testing.T) {
	nano := time.Now().UnixNano()
	newOrg := fmt.Sprintf("tenfix-traces-%d", nano)
	traceIDs := map[string]string{
		"acme-corp": stRandHex(t, 16),
		newOrg:      stRandHex(t, 16),
	}
	for org, id := range traceIDs {
		stInsertSpan(t, org, id, "st-autoregister-"+strings.SplitN(org, "-", 2)[0])
	}

	aliases := stAliases(t, tracesBaseURL)
	stAssertNoSharedIDs(t, aliases)
	if a, ok := stAliasOf(aliases, "acme-corp"); !ok || a.AccountID != 1001 {
		t.Fatalf("traces acme-corp = %+v %v, want 1001", a, ok)
	}
	a, ok := stAliasOf(aliases, newOrg)
	if !ok {
		t.Fatalf("%s was not registered by the span insert", newOrg)
	}
	if uint64(a.AccountID) < reservedMin || uint64(a.AccountID) > reservedMax {
		t.Fatalf("%s = %d:%d, want an AccountID in the reserved range", newOrg, a.AccountID, a.ProjectID)
	}

	// Each OrgID sees its own trace_id and not the other's.
	for org, id := range traceIDs {
		org, id := org, id
		stEventually(t, 90*time.Second, func() string {
			own := stQueryField(t, tracesBaseURL, org, fmt.Sprintf(`trace_id:%q`, id), "trace_id")
			if len(own) == 0 {
				return fmt.Sprintf("%s does not see its own trace %s yet", org, id)
			}
			for other, otherID := range traceIDs {
				if other == org {
					continue
				}
				if leaked := stQueryField(t, tracesBaseURL, org, fmt.Sprintf(`trace_id:%q`, otherID), "trace_id"); len(leaked) != 0 {
					t.Fatalf("%s sees %s's trace %s", org, other, otherID)
				}
			}
			return ""
		})
	}
}

func TestStringTenant_UnknownOrgIDReadIsEmptyAndDoesNotRegister_Traces(t *testing.T) {
	unknown := fmt.Sprintf("tenfix-unknown-%d", time.Now().UnixNano())

	if vals := stQueryField(t, tracesBaseURL, unknown, "*", "trace_id"); len(vals) != 0 {
		t.Fatalf("an unknown OrgID read %d trace rows", len(vals))
	}
	for _, path := range []string{
		"/select/jaeger/api/services",
		"/select/tempo/api/search?" + url.Values{"tags": {"service.name=st-autoregister-acme"}}.Encode(),
	} {
		req, _ := http.NewRequest("GET", tracesBaseURL+path, nil)
		req.Header.Set("X-Scope-OrgID", unknown)
		resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s as an unknown OrgID: status %d: %s", path, resp.StatusCode, b)
		}
		if strings.Contains(string(b), "st-autoregister") {
			t.Fatalf("%s leaked another tenant's service to an unknown OrgID: %s", path, b)
		}
	}

	if _, ok := stAliasOf(stAliases(t, tracesBaseURL), unknown); ok {
		t.Fatalf("a read registered %s", unknown)
	}
}
