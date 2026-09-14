//go:build parity

package parity

// Offline checks of the harness's own comparison rules. They issue no
// requests, so they also run without the parity stack:
//
//	GOWORK=off go test -tags=parity -run '^TestHarness_' ./tests/parity/

import (
	"math"
	"strings"
	"testing"
)

func TestHarness_ReadComparableCount(t *testing.T) {
	envelope := func(value string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"__name__":"rows"},"value":[1789306049.0,` + value + `]}]}}`
	}
	cases := []struct {
		name      string
		body      string
		want      float64 // NaN means "must be NaN"
		wantShape string
		wantErr   string
	}{
		{name: "stats sample", body: envelope(`"42"`), want: 42, wantShape: shapeStatsSample},
		{name: "fractional stats sample", body: envelope(`"10.9488"`), want: 10.9488, wantShape: shapeStatsSample},
		{name: "empty stats result is zero", body: `{"status":"success","data":{"resultType":"vector","result":[]}}`, want: 0, wantShape: shapeStatsSample},
		// An aggregate over a field no row carries: median/quantile answer "",
		// sum/avg answer "NaN". Both are "nothing to compare", not a number.
		{name: "empty sample value is NaN", body: envelope(`""`), want: math.NaN(), wantShape: shapeStatsSample},
		{name: "NaN sample value is NaN", body: envelope(`"NaN"`), want: math.NaN(), wantShape: shapeStatsSample},
		{name: "NDJSON rows are counted", body: "{\"_msg\":\"a\"}\n{\"_msg\":\"b\"}\n{\"_msg\":\"c\"}\n", want: 3, wantShape: shapeNDJSONRows},
		{name: "one NDJSON row is one row", body: `{"_msg":"only","_time":"2026-09-13T00:00:00Z"}`, want: 1, wantShape: shapeNDJSONRows},
		{name: "empty body is zero rows", body: "", want: 0, wantShape: shapeNDJSONRows},
		{name: "envelope without result array", body: `{"status":"success","data":{"resultType":"vector"}}`, wantErr: "no result array"},
		{name: "sample without value pair", body: `{"status":"success","data":{"result":[{"metric":{}}]}}`, wantErr: "no [timestamp, value] pair"},
		{name: "non-numeric sample value", body: envelope(`"many"`), wantErr: "not a number"},
		{name: "sample value that is not a string", body: envelope(`42`), wantErr: "want string"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, shape, err := readComparableCount([]byte(tc.body))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if shape != tc.wantShape {
				t.Errorf("shape = %q, want %q", shape, tc.wantShape)
			}
			if math.IsNaN(tc.want) {
				if !math.IsNaN(got) {
					t.Errorf("count = %v, want NaN", got)
				}
			} else if got != tc.want {
				t.Errorf("count = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHarness_JudgeCounts(t *testing.T) {
	count := ParityCase{Compare: CountEqual}
	tolerant := ParityCase{Compare: CountTolerance}
	empty := ParityCase{Compare: CountEqual, ExpectEmpty: true}
	nan := math.NaN()

	cases := []struct {
		name           string
		pc             ParityCase
		ref, sut, tol  float64
		emptyReference bool
		problem        string // substring of the single expected problem, "" for none
	}{
		{name: "equal counts pass", pc: count, ref: 42, sut: 42},
		{name: "unequal counts fail", pc: count, ref: 42, sut: 41, problem: "count mismatch: ref=42 sut=41"},
		// The vacuous passes this guard exists for: 0 == 0 and NaN vs NaN.
		{name: "zero reference is refused", pc: count, ref: 0, sut: 0, emptyReference: true},
		{name: "NaN reference is refused", pc: count, ref: nan, sut: nan, emptyReference: true},
		{name: "zero reference is refused whatever the SUT says", pc: count, ref: 0, sut: 7, emptyReference: true},
		{name: "expect empty passes on two zeros", pc: empty, ref: 0, sut: 0},
		{name: "expect empty fails a non-empty SUT", pc: empty, ref: 0, sut: 3, problem: "SUT returned 3"},
		{name: "expect empty fails a reference that stopped being empty", pc: empty, ref: 5, sut: 0, problem: "no longer exercises the empty path"},
		{name: "tolerance admits a small difference", pc: tolerant, ref: 100, sut: 96, tol: 0.05},
		{name: "tolerance rejects a large difference", pc: tolerant, ref: 100, sut: 90, tol: 0.05, problem: "outside tolerance"},
		{name: "tolerance rejects a NaN SUT", pc: tolerant, ref: 100, sut: nan, tol: 0.05, problem: "count mismatch"},
		{name: "tolerance measures against a negative reference's magnitude", pc: tolerant, ref: -100, sut: -96, tol: 0.05},
		{name: "tolerance refuses a zero reference", pc: tolerant, ref: 0, sut: 0, tol: 0.05, emptyReference: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := judgeCounts(tc.pc, tc.ref, tc.sut, tc.tol)
			if v.emptyReference != tc.emptyReference {
				t.Errorf("emptyReference = %v, want %v", v.emptyReference, tc.emptyReference)
			}
			switch {
			case tc.problem == "" && len(v.problems) != 0:
				t.Errorf("problems = %q, want none", v.problems)
			case tc.problem != "" && (len(v.problems) != 1 || !strings.Contains(v.problems[0], tc.problem)):
				t.Errorf("problems = %q, want exactly one containing %q", v.problems, tc.problem)
			}
		})
	}
}
