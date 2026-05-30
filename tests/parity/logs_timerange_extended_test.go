//go:build parity

package parity

import (
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"
)

func TestParity_TimeRangeExtended(t *testing.T) {
	now := time.Now()
	dataStart := now.Add(-24 * time.Hour)

	t.Run("rfc3339_timestamps", func(t *testing.T) {
		pc := ParityCase{Name: "rfc3339_timestamps", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": dataStart.Format(time.RFC3339),
			"end":   now.Format(time.RFC3339),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("rfc3339_nano_timestamps", func(t *testing.T) {
		pc := ParityCase{Name: "rfc3339_nano_timestamps", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": dataStart.Format(time.RFC3339Nano),
			"end":   now.Format(time.RFC3339Nano),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("relative_time_1h", func(t *testing.T) {
		pc := ParityCase{Name: "relative_time_1h", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "-1h",
		}, Compare: CountTolerance, Tolerance: 0.05}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("relative_time_30m", func(t *testing.T) {
		pc := ParityCase{Name: "relative_time_30m", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "-30m",
		}, Compare: CountTolerance, Tolerance: 0.05}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("relative_time_now_minus_30m", func(t *testing.T) {
		pc := ParityCase{Name: "relative_time_now_minus_30m", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "now-30m",
		}, Compare: CountTolerance, Tolerance: 0.05}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("relative_time_24h", func(t *testing.T) {
		pc := ParityCase{Name: "relative_time_24h", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "-24h",
		}, Compare: CountTolerance, Tolerance: 0.05}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("microsecond_epoch", func(t *testing.T) {
		startUs := dataStart.UnixMicro()
		endUs := now.UnixMicro()
		pc := ParityCase{Name: "microsecond_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", startUs),
			"end":   fmt.Sprintf("%d", endUs),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("epoch_zero_start", func(t *testing.T) {
		pc := ParityCase{Name: "epoch_zero_start", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "0",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("same_timestamp_all_formats", func(t *testing.T) {
		// All format variants for the same logical time range should return the same count.
		refTime := now.Add(-12 * time.Hour)

		type formatCase struct {
			name  string
			start string
			end   string
		}
		formats := []formatCase{
			{"nanosecond", fmt.Sprintf("%d", refTime.UnixNano()), fmt.Sprintf("%d", now.UnixNano())},
			{"second", fmt.Sprintf("%d", refTime.Unix()), fmt.Sprintf("%d", now.Unix())},
			{"millisecond", fmt.Sprintf("%d", refTime.UnixMilli()), fmt.Sprintf("%d", now.UnixMilli())},
			{"rfc3339", refTime.Format(time.RFC3339), now.Format(time.RFC3339)},
		}

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				var counts []float64
				for _, fc := range formats {
					params := url.Values{
						"start": {fc.start},
						"end":   {fc.end},
						"query": {"* | stats count() rows"},
					}
					res := fetch(t, label.baseURL, statsEndpoint(), params)
					if res.StatusCode != 200 {
						t.Fatalf("%s: status %d: %s", fc.name, res.StatusCode, string(res.Body))
					}
					cnt, err := extractVectorCount(res.Body)
					if err != nil {
						t.Fatalf("%s: extractVectorCount: %v", fc.name, err)
					}
					counts = append(counts, cnt)
					t.Logf("%s: format=%s count=%v", label.name, fc.name, cnt)
				}
				// All format counts should be equal (within 1% tolerance for rounding).
				for i := 1; i < len(counts); i++ {
					if counts[0] > 0 && math.Abs(counts[0]-counts[i])/counts[0] > 0.01 {
						t.Errorf("count mismatch: %s=%v vs %s=%v",
							formats[0].name, counts[0], formats[i].name, counts[i])
					}
				}
			})
		}
	})

	t.Run("rfc3339_vs_epoch_parity", func(t *testing.T) {
		// RFC3339 timestamps on both VL and LH should match epoch-based counts.
		rfc3339Params := url.Values{
			"start": {dataStart.Format(time.RFC3339)},
			"end":   {now.Format(time.RFC3339)},
			"query": {"* | stats count() rows"},
		}
		refRFC := fetch(t, vlBaseURL, statsEndpoint(), rfc3339Params)
		sutRFC := fetch(t, lhBaseURL, statsEndpoint(), rfc3339Params)
		if refRFC.StatusCode != 200 {
			t.Fatalf("VL rfc3339 status %d: %s", refRFC.StatusCode, string(refRFC.Body))
		}
		if sutRFC.StatusCode != 200 {
			t.Fatalf("LH rfc3339 status %d: %s", sutRFC.StatusCode, string(sutRFC.Body))
		}
		refCount, err := extractVectorCount(refRFC.Body)
		if err != nil {
			t.Fatalf("VL extractVectorCount: %v", err)
		}
		sutCount, err := extractVectorCount(sutRFC.Body)
		if err != nil {
			t.Fatalf("LH extractVectorCount: %v", err)
		}
		if refCount != sutCount {
			t.Errorf("RFC3339 parity mismatch: VL=%v LH=%v", refCount, sutCount)
		}
		t.Logf("rfc3339_parity: VL=%v LH=%v", refCount, sutCount)
	})

	t.Run("relative_time_parity", func(t *testing.T) {
		// Relative time formats on VL vs LH should produce similar counts.
		relFormats := []string{"-1h", "-6h", "-12h", "-24h"}
		for _, rel := range relFormats {
			t.Run(rel, func(t *testing.T) {
				params := url.Values{
					"start": {rel},
					"query": {"* | stats count() rows"},
				}
				ref := fetch(t, vlBaseURL, statsEndpoint(), params)
				sut := fetch(t, lhBaseURL, statsEndpoint(), params)
				if ref.StatusCode != 200 {
					t.Fatalf("VL status %d: %s", ref.StatusCode, string(ref.Body))
				}
				if sut.StatusCode != 200 {
					t.Fatalf("LH status %d: %s", sut.StatusCode, string(sut.Body))
				}
				refCount, err := extractVectorCount(ref.Body)
				if err != nil {
					t.Fatalf("VL extractVectorCount: %v", err)
				}
				sutCount, err := extractVectorCount(sut.Body)
				if err != nil {
					t.Fatalf("LH extractVectorCount: %v", err)
				}
				// Allow 5% tolerance since relative times are resolved at slightly different moments.
				if refCount > 0 && math.Abs(refCount-sutCount)/refCount > 0.05 {
					t.Errorf("relative %s parity mismatch: VL=%v LH=%v", rel, refCount, sutCount)
				}
				t.Logf("relative %s: VL=%v LH=%v", rel, refCount, sutCount)
			})
		}
	})
}
