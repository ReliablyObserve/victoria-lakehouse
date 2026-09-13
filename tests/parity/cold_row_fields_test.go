//go:build parity

package parity

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestParity_ColdRowFields pins the single most visible cold/hot divergence: the
// field set of a row. A cold log line must carry exactly the fields the same line
// carries hot — no "<null>" placeholders for unset Parquet columns, no tenant
// bookkeeping columns, no raw Tier-2 spare slots.
func TestParity_ColdRowFields(t *testing.T) {
	params := fullRangeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	ref := fetch(t, vlBaseURL, queryEndpoint(), params)
	sut := fetch(t, lhBaseURL, queryEndpoint(), params)
	refRows := parseNDJSON(ref.Body)
	sutRows := parseNDJSON(sut.Body)
	if len(refRows) == 0 {
		t.Fatal("reference returned 0 JSONL rows")
	}
	if len(sutRows) == 0 {
		t.Fatal("SUT returned 0 JSONL rows")
	}

	assertNoInternalFields(t, "hot logs", refRows)
	assertNoInternalFields(t, "cold logs", sutRows)

	// The two sides see different rows (different retention windows), so
	// compare the VOCABULARY: every field name cold emits must be one hot also
	// emits somewhere in the sample.
	refNames := map[string]bool{}
	for _, row := range refRows {
		for k := range row {
			refNames[k] = true
		}
	}
	var extra []string
	for _, row := range sutRows {
		for k, v := range row {
			if s, ok := v.(string); ok && s == "" {
				continue
			}
			if !refNames[k] {
				extra = append(extra, fmt.Sprintf("%s=%v", k, v))
			}
		}
	}
	if len(extra) > 0 {
		t.Errorf("cold rows carry %d field(s) hot never returns: %s",
			len(extra), strings.Join(sortedStrings(extra), ", "))
	}
}

// TestParity_Traces_ColdSpanFields is the traces twin: a cold span must carry
// VictoriaTraces' own field names (resource_attr: / span_attr: prefixed), with no
// "<null>" placeholders, no tenant or slot columns, and no service-graph edge
// columns.
func TestParity_Traces_ColdSpanFields(t *testing.T) {
	now := time.Now()
	params := url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
		"query": {"span_id:*"},
		"limit": {"50"},
	}

	ref := fetch(t, vtBaseURL, "/select/logsql/query", params)
	sut := fetch(t, lhtBaseURL, "/select/logsql/query", params)
	refRows := parseNDJSON(ref.Body)
	sutRows := parseNDJSON(sut.Body)
	if len(refRows) == 0 {
		t.Fatal("reference returned 0 JSONL span rows")
	}
	if len(sutRows) == 0 {
		t.Fatal("SUT returned 0 JSONL span rows")
	}

	assertNoInternalFields(t, "hot traces", refRows)
	assertNoInternalFields(t, "cold traces", sutRows)
	assertNoServiceGraphFields(t, "cold traces", sutRows)

	refNames := map[string]bool{}
	for _, row := range refRows {
		for k := range row {
			refNames[k] = true
		}
	}
	var extra []string
	for _, row := range sutRows {
		for k, v := range row {
			if s, ok := v.(string); ok && s == "" {
				continue
			}
			if !refNames[k] {
				extra = append(extra, fmt.Sprintf("%s=%v", k, v))
			}
		}
	}
	if len(extra) > 0 {
		t.Errorf("cold spans carry %d field(s) hot VT never returns: %s",
			len(extra), strings.Join(sortedStrings(extra), ", "))
	}
}
