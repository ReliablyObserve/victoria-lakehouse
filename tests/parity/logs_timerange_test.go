//go:build parity

package parity

import (
	"fmt"
	"testing"
	"time"
)

func TestParity_TimeRange(t *testing.T) {
	now := time.Now()
	dataStart := now.Add(-24 * time.Hour)

	t.Run("ns_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "ns_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("sec_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "sec_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.Unix()),
			"end":   fmt.Sprintf("%d", now.Unix()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("ms_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "ms_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixMilli()),
			"end":   fmt.Sprintf("%d", now.UnixMilli()),
		}, Compare: CountTolerance, Tolerance: 0.01}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("missing_end", func(t *testing.T) {
		pc := ParityCase{Name: "missing_end", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("missing_start", func(t *testing.T) {
		pc := ParityCase{Name: "missing_start", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("future_range", func(t *testing.T) {
		future := now.Add(365 * 24 * time.Hour)
		pc := ParityCase{Name: "future_range", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", future.UnixNano()),
			"end":   fmt.Sprintf("%d", future.Add(time.Hour).UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("zero_width", func(t *testing.T) {
		ts := fmt.Sprintf("%d", now.Add(-6*time.Hour).UnixNano())
		pc := ParityCase{Name: "zero_width", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": ts,
			"end":   ts,
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("narrow_1min", func(t *testing.T) {
		mid := now.Add(-12 * time.Hour)
		pc := ParityCase{Name: "narrow_1min", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", mid.UnixNano()),
			"end":   fmt.Sprintf("%d", mid.Add(time.Minute).UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("full_range", func(t *testing.T) {
		pc := ParityCase{Name: "full_range", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "0",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("boundary_ns", func(t *testing.T) {
		pc := ParityCase{Name: "boundary_ns", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
			"end":   fmt.Sprintf("%d", dataStart.Add(time.Second).UnixNano()),
		}, Compare: CountTolerance, Tolerance: 0.1}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})
}
