//go:build parity

package parity

import (
	"fmt"
	"strings"
	"testing"
)

// fieldNamesHitsTypeError reports the first entry of a field_names or
// stream_field_names answer whose "hits" is not a JSON number. VictoriaLogs and
// VictoriaTraces write hits as numbers ({"value":"_msg","hits":10005}), so an
// answer with "hits":"10005" is a different answer, which compareFieldValues
// (it reads either form) would wave through.
func fieldNamesHitsTypeError(body []byte) error {
	obj, err := parseJSON(body)
	if err != nil {
		return fmt.Errorf("not JSON: %w", err)
	}
	for i, e := range asSlice(obj["values"]) {
		m, _ := e.(map[string]any)
		if m == nil {
			return fmt.Errorf("entry %d is not an object: %v", i, e)
		}
		if _, ok := m["hits"].(float64); !ok {
			return fmt.Errorf("entry %d (%v) has hits %#v, want a JSON number", i, m["value"], m["hits"])
		}
		if _, ok := m["value"].(string); !ok {
			return fmt.Errorf("entry %d has value %#v, want a string", i, m["value"])
		}
	}
	return nil
}

// numericHitsErrors lists, for hot ("hot") and Lakehouse ("cold") in that order,
// the answers that do not write hits as JSON numbers. An answer whose status is
// not 200 is skipped (the status is compared elsewhere).
func numericHitsErrors(label string, ref, sut fetchResult) []string {
	var out []string
	for _, who := range []struct {
		name string
		r    fetchResult
	}{{"hot", ref}, {"cold", sut}} {
		if who.r.StatusCode != 200 {
			continue
		}
		if err := fieldNamesHitsTypeError(who.r.Body); err != nil {
			out = append(out, fmt.Sprintf("%s: %s answer: %v", label, who.name, err))
		}
	}
	return out
}

// requireNumericHits fails unless hot's and Lakehouse's answers both write
// hits as JSON numbers.
func requireNumericHits(t *testing.T, label string, ref, sut fetchResult) {
	t.Helper()
	for _, msg := range numericHitsErrors(label, ref, sut) {
		t.Error(msg)
	}
}

func TestNumericHitsErrors(t *testing.T) {
	ok := fetchResult{StatusCode: 200, Body: []byte(`{"values":[{"value":"a","hits":3}]}`)}
	str := fetchResult{StatusCode: 200, Body: []byte(`{"values":[{"value":"a","hits":"3"}]}`)}
	bad := fetchResult{StatusCode: 500, Body: []byte(`oops`)}
	for name, tc := range map[string]struct {
		ref, sut fetchResult
		want     []string
	}{
		"both numbers":                          {ok, ok, nil},
		"cold writes strings":                   {ok, str, []string{"x: cold answer"}},
		"hot writes strings":                    {str, ok, []string{"x: hot answer"}},
		"both write strings":                    {str, str, []string{"x: hot answer", "x: cold answer"}},
		"a failed status is compared elsewhere": {bad, ok, nil},
		"a failed cold status is skipped":       {ok, bad, nil},
	} {
		got := numericHitsErrors("x", tc.ref, tc.sut)
		if len(got) != len(tc.want) {
			t.Errorf("%s: %d errors %q, want %d", name, len(got), got, len(tc.want))
			continue
		}
		for i, w := range tc.want {
			if !strings.HasPrefix(got[i], w) {
				t.Errorf("%s: error %d = %q, want prefix %q", name, i, got[i], w)
			}
		}
		reportLockCells(t, 1) // one cell per case (floor in lock_cells.txt)
	}
}

func TestFieldNamesHitsTypeError_IsStrict(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		ok   bool
	}{
		"numbers":        {`{"values":[{"value":"a","hits":3},{"value":"b","hits":0}]}`, true},
		"empty":          {`{"values":[]}`, true},
		"string hits":    {`{"values":[{"value":"a","hits":"3"}]}`, false},
		"missing hits":   {`{"values":[{"value":"a"}]}`, false},
		"null hits":      {`{"values":[{"value":"a","hits":null}]}`, false},
		"non-string key": {`{"values":[{"value":7,"hits":1}]}`, false},
	} {
		if err := fieldNamesHitsTypeError([]byte(tc.body)); (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", name, err, tc.ok)
		}
		reportLockCells(t, 1) // one cell per case (floor in lock_cells.txt)
	}
}
