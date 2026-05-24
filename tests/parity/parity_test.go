//go:build parity

package parity

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"
)

type CompareMode string

const (
	CountEqual     CompareMode = "count_equal"
	CountTolerance CompareMode = "count_tolerance"
	SetEqual       CompareMode = "set_equal"
	SetSuperset    CompareMode = "set_superset"
	RowsMatch      CompareMode = "rows_match"
	StatusEqual    CompareMode = "status_equal"
	StructureMatch CompareMode = "structure_match"
	BucketMatch    CompareMode = "bucket_match"
	NonEmpty       CompareMode = "non_empty"
)

type ParityCase struct {
	Name       string
	Endpoint   string
	Params     map[string]string
	Compare    CompareMode
	SkipFields []string
	Tolerance  float64
}

func fullRangeParams() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}

func rangeParams(dur time.Duration) url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-dur).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}

func buildParams(pc ParityCase, base url.Values) url.Values {
	params := url.Values{}
	if base != nil {
		for k, v := range base {
			params[k] = v
		}
	}
	for k, v := range pc.Params {
		params.Set(k, v)
	}
	return params
}

func RunParity(t *testing.T, refBase, sutBase string, cases []ParityCase) {
	t.Helper()
	for _, pc := range cases {
		t.Run(pc.Name, func(t *testing.T) {
			params := buildParams(pc, fullRangeParams())
			ref := fetch(t, refBase, pc.Endpoint, params)
			sut := fetch(t, sutBase, pc.Endpoint, params)
			compareParity(t, pc, ref, sut)
		})
	}
}

func RunParityWithRange(t *testing.T, refBase, sutBase string, dur time.Duration, cases []ParityCase) {
	t.Helper()
	for _, pc := range cases {
		t.Run(pc.Name, func(t *testing.T) {
			params := buildParams(pc, rangeParams(dur))
			ref := fetch(t, refBase, pc.Endpoint, params)
			sut := fetch(t, sutBase, pc.Endpoint, params)
			compareParity(t, pc, ref, sut)
		})
	}
}

func compareParity(t *testing.T, pc ParityCase, ref, sut fetchResult) {
	t.Helper()
	switch pc.Compare {
	case CountEqual:
		compareCountEqual(t, ref, sut, 0)
	case CountTolerance:
		tol := pc.Tolerance
		if tol == 0 {
			tol = 0.01
		}
		compareCountEqual(t, ref, sut, tol)
	case SetEqual:
		compareSetEqual(t, ref, sut)
	case SetSuperset:
		compareSetSuperset(t, ref, sut)
	case RowsMatch:
		compareRowsMatch(t, ref, sut, pc.SkipFields)
	case StatusEqual:
		compareStatusEqual(t, ref, sut)
	case StructureMatch:
		compareStructureMatch(t, ref, sut)
	case BucketMatch:
		compareBucketMatch(t, ref, sut)
	case NonEmpty:
		compareNonEmpty(t, ref, sut)
	default:
		t.Fatalf("unknown compare mode: %s", pc.Compare)
	}
}

func compareCountEqual(t *testing.T, ref, sut fetchResult, tolerance float64) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d: %s", ref.StatusCode, string(ref.Body))
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d: %s", sut.StatusCode, string(sut.Body))
	}
	refCount, err := extractVectorCount(ref.Body)
	if err != nil {
		refLines := parseNDJSON(ref.Body)
		sutLines := parseNDJSON(sut.Body)
		refCount = float64(len(refLines))
		sutCount := float64(len(sutLines))
		if tolerance == 0 {
			if refCount != sutCount {
				t.Errorf("count mismatch: ref=%v sut=%v", refCount, sutCount)
			}
		} else {
			if refCount > 0 && math.Abs(refCount-sutCount)/refCount > tolerance {
				t.Errorf("count outside tolerance %.1f%%: ref=%v sut=%v", tolerance*100, refCount, sutCount)
			}
		}
		t.Logf("count_equal (NDJSON): ref=%v sut=%v", refCount, sutCount)
		return
	}
	sutCount, err := extractVectorCount(sut.Body)
	if err != nil {
		t.Fatalf("SUT extractVectorCount: %v", err)
	}
	if tolerance == 0 {
		if refCount != sutCount {
			t.Errorf("count mismatch: ref=%v sut=%v", refCount, sutCount)
		}
	} else {
		if refCount > 0 && math.Abs(refCount-sutCount)/refCount > tolerance {
			t.Errorf("count outside tolerance %.1f%%: ref=%v sut=%v", tolerance*100, refCount, sutCount)
		}
	}
	t.Logf("count_equal: ref=%v sut=%v", refCount, sutCount)
}

func compareSetEqual(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refVals := sortedStrings(extractValuesStrings(ref.Body))
	sutVals := sortedStrings(extractValuesStrings(sut.Body))
	refSet := stringSet(refVals)
	sutSet := stringSet(sutVals)
	for _, v := range refVals {
		if !sutSet[v] {
			t.Errorf("SUT missing value %q present in reference", v)
		}
	}
	for _, v := range sutVals {
		if !refSet[v] {
			t.Errorf("SUT has extra value %q not in reference", v)
		}
	}
	t.Logf("set_equal: ref=%d sut=%d", len(refVals), len(sutVals))
}

func compareSetSuperset(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refVals := extractValuesStrings(ref.Body)
	sutSet := stringSet(extractValuesStrings(sut.Body))
	for _, v := range refVals {
		if !sutSet[v] {
			t.Errorf("SUT missing value %q present in reference (superset check)", v)
		}
	}
	t.Logf("set_superset: ref=%d sut=%d", len(refVals), len(sutSet))
}

func compareRowsMatch(t *testing.T, ref, sut fetchResult, skipFields []string) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refRows := parseNDJSON(ref.Body)
	sutRows := parseNDJSON(sut.Body)
	if len(refRows) != len(sutRows) {
		t.Errorf("row count mismatch: ref=%d sut=%d", len(refRows), len(sutRows))
		return
	}
	refKeys := extractRowKeys(refRows, skipFields)
	sutKeys := extractRowKeys(sutRows, skipFields)
	mismatches := 0
	for i := range refKeys {
		if i >= len(sutKeys) {
			break
		}
		if refKeys[i] != sutKeys[i] {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("row %d mismatch:\n  ref: %s\n  sut: %s", i, refKeys[i], sutKeys[i])
			}
		}
	}
	if mismatches > 3 {
		t.Errorf("... and %d more mismatches", mismatches-3)
	}
	t.Logf("rows_match: %d rows, %d mismatches", len(refRows), mismatches)
}

func compareStatusEqual(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != sut.StatusCode {
		t.Errorf("status mismatch: ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
	}
	t.Logf("status_equal: ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
}

func compareStructureMatch(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refObj, err := parseJSON(ref.Body)
	if err != nil {
		t.Fatalf("ref parse: %v", err)
	}
	sutObj, err := parseJSON(sut.Body)
	if err != nil {
		t.Fatalf("sut parse: %v", err)
	}
	refStatus, _ := refObj["status"].(string)
	sutStatus, _ := sutObj["status"].(string)
	if refStatus != sutStatus {
		t.Errorf("status field mismatch: ref=%q sut=%q", refStatus, sutStatus)
	}
	refData, _ := refObj["data"].(map[string]any)
	sutData, _ := sutObj["data"].(map[string]any)
	if refData == nil || sutData == nil {
		t.Logf("structure_match: one or both missing data field")
		return
	}
	refType, _ := refData["resultType"].(string)
	sutType, _ := sutData["resultType"].(string)
	if refType != sutType {
		t.Errorf("resultType mismatch: ref=%q sut=%q", refType, sutType)
	}
	refResult, _ := refData["result"].([]any)
	sutResult, _ := sutData["result"].([]any)
	if len(refResult) != len(sutResult) {
		t.Errorf("result array length mismatch: ref=%d sut=%d", len(refResult), len(sutResult))
	}
	t.Logf("structure_match: type=%s ref_results=%d sut_results=%d", refType, len(refResult), len(sutResult))
}

func compareBucketMatch(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refTS, refCounts := extractHitsBuckets(ref.Body)
	sutTS, sutCounts := extractHitsBuckets(sut.Body)
	if len(refTS) != len(sutTS) {
		t.Errorf("bucket count mismatch: ref=%d sut=%d", len(refTS), len(sutTS))
		return
	}
	mismatches := 0
	for i := range refCounts {
		if i >= len(sutCounts) {
			break
		}
		if math.Abs(refCounts[i]-sutCounts[i]) > 1 {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("bucket %d (%s) count mismatch: ref=%v sut=%v", i, refTS[i], refCounts[i], sutCounts[i])
			}
		}
	}
	totalRef := 0.0
	totalSut := 0.0
	for _, c := range refCounts {
		totalRef += c
	}
	for _, c := range sutCounts {
		totalSut += c
	}
	t.Logf("bucket_match: %d buckets, %d mismatches, ref_total=%v sut_total=%v", len(refTS), mismatches, totalRef, totalSut)
}

func compareNonEmpty(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refLen := len(strings.TrimSpace(string(ref.Body)))
	sutLen := len(strings.TrimSpace(string(sut.Body)))
	if refLen == 0 {
		t.Error("reference returned empty response")
	}
	if sutLen == 0 {
		t.Error("SUT returned empty response")
	}
	t.Logf("non_empty: ref=%d bytes sut=%d bytes", refLen, sutLen)
}

func statsEndpoint() string {
	return "/select/logsql/stats_query"
}

func statsRangeEndpoint() string {
	return "/select/logsql/stats_query_range"
}

func queryEndpoint() string {
	return "/select/logsql/query"
}

func hitsEndpoint() string {
	return "/select/logsql/hits"
}
