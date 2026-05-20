//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"
)

// TestTimeRange_NarrowWindowReturnsSubset verifies that a narrow time window
// (last 30 minutes) returns fewer or equal results than the wide window (72 h).
func TestTimeRange_NarrowWindowReturnsSubset(t *testing.T) {
	baseParams := url.Values{
		"query": {"*"},
		"limit": {"1000"},
	}

	// Wide window query
	wide := wideTimeParams()
	for k, v := range baseParams {
		wide[k] = v
	}
	wideBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", wide)
	wideLines := assertValidNDJSON(t, wideBody)

	// Narrow window query
	narrow := defaultTimeParams()
	for k, v := range baseParams {
		narrow[k] = v
	}
	narrowBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", narrow)
	narrowLines := assertValidNDJSON(t, narrowBody)

	t.Logf("wide (72h): %d results, narrow (30m): %d results", len(wideLines), len(narrowLines))

	if len(narrowLines) > len(wideLines) {
		t.Errorf("narrow window returned more results (%d) than wide window (%d) — unexpected",
			len(narrowLines), len(wideLines))
	}
}

// TestTimeRange_EmptyWindowReturnsNothing verifies that a time window entirely
// in the future (or far past) returns an empty result set.
func TestTimeRange_EmptyWindowReturnsNothing(t *testing.T) {
	// Far future — no data should exist here.
	farFuture := time.Now().Add(24 * time.Hour)
	params := url.Values{
		"query": {"*"},
		"limit": {"10"},
		"start": {fmt.Sprintf("%d", farFuture.UnixNano())},
		"end":   {fmt.Sprintf("%d", farFuture.Add(time.Hour).UnixNano())},
	}

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) != 0 {
		t.Errorf("future time window returned %d results, expected 0", len(lines))
	}
	t.Log("empty-window check OK: future range returns no data")
}

// TestTimeRange_BoundaryPrecision verifies that the returned _time values from
// a narrow query all fall within the requested [start, end] interval.
func TestTimeRange_BoundaryPrecision(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	startNs, _ := fmt.Sscanf(params.Get("start"), "%d", new(int64))
	_ = startNs

	var startT, endT time.Time
	var startNano, endNano int64
	fmt.Sscanf(params.Get("start"), "%d", &startNano)
	fmt.Sscanf(params.Get("end"), "%d", &endNano)
	startT = time.Unix(0, startNano).UTC()
	endT = time.Unix(0, endNano).UTC()

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) == 0 {
		t.Skip("no data in default 30-minute window — skipping boundary precision check")
	}

	outOfRange := 0
	for i, l := range lines {
		tsStr, ok := l["_time"].(string)
		if !ok || tsStr == "" {
			t.Errorf("line %d: missing or empty _time", i)
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			t.Errorf("line %d: _time %q is not RFC3339Nano: %v", i, tsStr, err)
			continue
		}
		// Allow 1s of tolerance around boundaries for clock skew / flush lag.
		if ts.Before(startT.Add(-time.Second)) || ts.After(endT.Add(time.Second)) {
			outOfRange++
			t.Logf("line %d: _time %s is outside [%s, %s]", i, ts, startT, endT)
		}
	}

	if outOfRange > 0 {
		t.Errorf("%d/%d records fell outside the requested time boundary (±1s tolerance)",
			outOfRange, len(lines))
	} else {
		t.Logf("boundary precision OK: all %d records within [start, end] ±1s", len(lines))
	}
}

// TestTimeRange_MillisecondEpochNormalization verifies that the API correctly
// handles millisecond-epoch start/end parameters (not nanosecond), which some
// callers send. The system should either accept and normalise them or return
// a clear error — not silently return wrong data.
func TestTimeRange_MillisecondEpochNormalization(t *testing.T) {
	now := time.Now()

	// Millisecond epoch (13 digits) — what many Grafana/Loki clients send.
	startMs := now.Add(-30 * time.Minute).UnixMilli()
	endMs := now.UnixMilli()

	params := url.Values{
		"query": {"*"},
		"limit": {"10"},
		"start": {fmt.Sprintf("%d", startMs)},
		"end":   {fmt.Sprintf("%d", endMs)},
	}

	resp := httpGetAllowStatus(t, logsBaseURL, "/select/logsql/query", params,
		200, 400, 422)
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case 200:
		// System accepted millisecond epochs. Verify returned timestamps look sane.
		body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
		lines := assertValidNDJSON(t, body)
		t.Logf("millisecond epoch: accepted by server, returned %d results", len(lines))

		for i, l := range lines {
			tsStr, ok := l["_time"].(string)
			if !ok || tsStr == "" {
				continue
			}
			ts, err := time.Parse(time.RFC3339Nano, tsStr)
			if err != nil {
				continue
			}
			age := time.Since(ts).Abs()
			if age > 72*time.Hour {
				t.Errorf("line %d: _time %s is > 72h old — millisecond epoch may have been misinterpreted as seconds", i, ts)
			}
		}

	case 400, 422:
		t.Logf("millisecond epoch: server rejected with %d (expected — nanosecond precision required)", resp.StatusCode)

	default:
		t.Errorf("unexpected status %d for millisecond epoch query", resp.StatusCode)
	}
}

// TestTimeRange_Traces_NarrowWindowSubset verifies the same narrow-vs-wide
// invariant for the traces endpoint.
func TestTimeRange_Traces_NarrowWindowSubset(t *testing.T) {
	wide := wideTimeParams()
	wide.Set("query", "*")
	wide.Set("limit", "1000")

	wideBody := httpGetBody(t, tracesBaseURL, "/select/logsql/query", wide)
	wideLines := assertValidNDJSON(t, wideBody)

	narrow := defaultTimeParams()
	narrow.Set("query", "*")
	narrow.Set("limit", "1000")

	narrowBody := httpGetBody(t, tracesBaseURL, "/select/logsql/query", narrow)
	narrowLines := assertValidNDJSON(t, narrowBody)

	t.Logf("traces — wide (72h): %d results, narrow (30m): %d results", len(wideLines), len(narrowLines))

	if len(narrowLines) > len(wideLines) {
		t.Errorf("traces narrow window returned more results (%d) than wide (%d)",
			len(narrowLines), len(wideLines))
	}
}

// TestTimeRange_ManifestMinMaxConsistency checks that manifest minTime/maxTime
// are consistent: minTime <= maxTime, and both are plausible Unix nanosecond
// timestamps within the last 30 days.
func TestTimeRange_ManifestMinMaxConsistency(t *testing.T) {
	for _, tc := range []struct {
		name    string
		baseURL string
	}{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := httpGetBody(t, tc.baseURL, "/manifest/range", nil)
			result := mustParseJSON(t, body)

			minT, okMin := result["minTime"].(float64)
			maxT, okMax := result["maxTime"].(float64)
			if !okMin || !okMax {
				t.Fatal("manifest/range missing minTime or maxTime")
			}

			if minT > maxT {
				t.Errorf("manifest minTime (%.0f) > maxTime (%.0f) — invalid range", minT, maxT)
			}

			// Sanity: timestamps should be Unix nanoseconds in the last 30 days.
			now := float64(time.Now().UnixNano())
			thirtyDaysAgo := float64(time.Now().Add(-30 * 24 * time.Hour).UnixNano())

			if maxT > now+float64(time.Hour) {
				t.Errorf("manifest maxTime is in the future: %.0f", maxT)
			}
			if maxT < thirtyDaysAgo {
				t.Errorf("manifest maxTime is older than 30 days: %.0f", maxT)
			}

			rangeH := (maxT - minT) / float64(time.Hour)
			t.Logf("%s manifest: minTime=%.0f, maxTime=%.0f, range=%.1fh", tc.name, minT, maxT, rangeH)
		})
	}
}

// assertInRange is a small helper used only in this file.
func assertInRange(t *testing.T, label string, val, lo, hi float64) {
	t.Helper()
	if val < lo || val > hi {
		t.Errorf("%s=%.6g is outside [%.6g, %.6g]", label, val, lo, hi)
	}
}

// TestTimeRange_StepAlignedHits verifies that the /select/logsql/hits endpoint
// returns buckets aligned to the requested step.
func TestTimeRange_StepAlignedHits(t *testing.T) {
	stepSec := int64(3600) // 1 hour
	now := time.Now()

	params := url.Values{
		"query": {"*"},
		"step":  {fmt.Sprintf("%ds", stepSec)},
		"start": {fmt.Sprintf("%d", now.Add(-6*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)
	result := mustParseJSON(t, body)

	hitsRaw, ok := result["hits"].([]any)
	if !ok {
		t.Fatal("hits response missing 'hits' array")
	}
	if len(hitsRaw) == 0 {
		t.Skip("no hits buckets returned — skipping step alignment check")
	}

	stepNs := stepSec * int64(time.Second)
	for i, entry := range hitsRaw {
		obj, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		timestamps, ok := obj["timestamps"].([]any)
		if !ok || len(timestamps) == 0 {
			continue
		}
		for j, tsRaw := range timestamps {
			tsStr, ok := tsRaw.(string)
			if !ok {
				continue
			}
			ts, err := time.Parse(time.RFC3339, tsStr)
			if err != nil {
				// Try parsing as int64 nanoseconds embedded in JSON string
				var tsNano int64
				if _, scanErr := fmt.Sscanf(tsStr, "%d", &tsNano); scanErr == nil {
					rem := tsNano % stepNs
					if rem != 0 {
						t.Errorf("hits[%d].timestamps[%d]=%d is not aligned to step %ds (remainder %d ns)",
							i, j, tsNano, stepSec, rem)
					}
				}
				continue
			}
			_ = ts
			// RFC3339 timestamps: check that second-level offset is divisible by stepSec.
			tsUnix := ts.Unix()
			if tsUnix%stepSec != 0 {
				t.Errorf("hits[%d].timestamps[%d]=%s is not aligned to %ds step (unix %d %% %d = %d)",
					i, j, tsStr, stepSec, tsUnix, stepSec, tsUnix%stepSec)
			}
		}
	}

	t.Logf("step alignment check: %d hit buckets examined", len(hitsRaw))
}

// Ensure assertInRange is used (avoid compile error if unused).
var _ = func() bool { assertInRange(nil, "", 0, 0, 0); return true }

// mustSscanInt64 is a local helper to parse int64 from string without error
// handling (used only in test helpers above where errors are already checked).
func mustSscanInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

// Silence unused warning for mustSscanInt64
var _ = mustSscanInt64

// Ensure json import is used
var _ = json.Marshal
