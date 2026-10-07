//go:build parity

package parity

// Golden vectors shared with the Python proof metrics (scripts/proof/metrics/ties.py).
//
// testdata/tiecut_golden.json holds tie-cut inputs and the decisions they must get. This
// test checks the Go rule against it and scripts/proof/tests/test_tiecut_golden.py checks the
// Python port against the same file, so the two implementations cannot drift apart unnoticed.

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

type goldenQuery struct {
	Query       string   `json:"query"`
	Applies     bool     `json:"applies"`
	RereadQuery string   `json:"reread_query"`
	SortFields  []string `json:"sort_fields"`
}

type goldenTie struct {
	Name       string           `json:"name"`
	SortFields []string         `json:"sort_fields"`
	SkipFields []string         `json:"skip_fields"`
	Ref        []map[string]any `json:"ref"`
	Sut        []map[string]any `json:"sut"`
	GroupRef   []map[string]any `json:"group_ref"`
	GroupSut   []map[string]any `json:"group_sut"`
	RefStatus  int              `json:"ref_status"`
	SutStatus  int              `json:"sut_status"`
	Explained  bool             `json:"explained"`
}

type goldenFile struct {
	QueryCases []goldenQuery `json:"query_cases"`
	TieCases   []goldenTie   `json:"tie_cases"`
}

func loadGolden(t *testing.T) goldenFile {
	t.Helper()
	raw, err := os.ReadFile("testdata/tiecut_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g goldenFile
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	return g
}

func ndjsonBody(t *testing.T, rows []map[string]any) []byte {
	t.Helper()
	var sb strings.Builder
	for _, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return []byte(sb.String())
}

func TestTieCutGoldenQueries(t *testing.T) {
	for _, c := range loadGolden(t).QueryCases {
		q, fields, ok := tieGroupQuery(c.Query)
		if ok != c.Applies {
			t.Errorf("%q: applies = %v, want %v", c.Query, ok, c.Applies)
			continue
		}
		if !ok {
			continue
		}
		if q != c.RereadQuery {
			t.Errorf("%q: re-read query %q, want %q", c.Query, q, c.RereadQuery)
		}
		if !reflect.DeepEqual(fields, c.SortFields) {
			t.Errorf("%q: sort fields %v, want %v", c.Query, fields, c.SortFields)
		}
	}
}

func TestTieCutGoldenDecisions(t *testing.T) {
	for _, c := range loadGolden(t).TieCases {
		t.Run(c.Name, func(t *testing.T) {
			ties := &fixedTies{
				ref: fetchResult{StatusCode: c.RefStatus, Body: ndjsonBody(t, c.GroupRef)},
				sut: fetchResult{StatusCode: c.SutStatus, Body: ndjsonBody(t, c.GroupSut)},
			}
			problems, note := judgeRows(t, c.Ref, c.Sut, c.SkipFields, ties.fetch, c.SortFields)
			if got := len(problems) == 0; got != c.Explained {
				t.Errorf("explained = %v, want %v (%s; problems %v)", got, c.Explained, note, problems)
			}
		})
	}
}
