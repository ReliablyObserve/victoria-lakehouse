//go:build parity

package parity

import (
	"fmt"
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

// requireNumericHits fails unless hot's and Lakehouse's answers both write
// hits as JSON numbers.
func requireNumericHits(t *testing.T, label string, ref, sut fetchResult) {
	t.Helper()
	for who, r := range map[string]fetchResult{"hot": ref, "cold": sut} {
		if r.StatusCode != 200 {
			continue // the status is compared elsewhere
		}
		if err := fieldNamesHitsTypeError(r.Body); err != nil {
			t.Errorf("%s: %s answer: %v", label, who, err)
		}
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
