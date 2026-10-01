package parquets3

import (
	"sort"
	"strconv"
	"testing"
)

func reviewCount(rows []map[string]string) int {
	if len(rows) != 1 {
		return -1
	}
	n, _ := strconv.Atoi(rows[0]["n"])
	return n
}

func TestColdReview_TracesTombstoneShapes(t *testing.T) {
	{
		s, start, end := coldFilteredStatsFixture(t)
		run := coldSelectRunner(t, s, start, end)
		r := run("*")
		var keys []string
		for k, v := range r[0] {
			keys = append(keys, k+"="+v)
		}
		sort.Strings(keys)
		t.Logf("fields: %v", keys)
	}
	for _, tq := range []string{
		`resource_attr:service.name:=alpha`,
		`"resource_attr:service.name":=alpha`,
		`{resource_attr:service.name="beta"}`,
		`"span_attr:http.route":="/warm"`,
		`span_attr:http.method:=GET`,
		`NOT name:="needle-exact"`,
		`status_code:=1`,
		`duration:>2000000`,
		`trace_id:="trace-alpha-1"`,
		`span_attr:trace_state:="state-cold" OR name:="needle-exact"`,
	} {
		t.Run(tq, func(t *testing.T) {
			s, start, end := coldFilteredStatsFixture(t)
			run := coldSelectRunner(t, s, start, end)
			all := len(run("*"))
			addHideTombstone(s, tq, start, end)
			rows := run("*")
			if len(rows) == 0 || len(rows) >= all {
				t.Skipf("tombstone %q hides none/all (all=%d visible=%d)", tq, all, len(rows))
			}
			if got := reviewCount(run(`* | stats count() n`)); got != len(rows) {
				t.Errorf("* | stats count() = %d want %d", got, len(rows))
			}
			if got := reviewCount(run(`name:* | stats count() n`)); got != len(rows) {
				t.Errorf("name:* | stats count() = %d want %d", got, len(rows))
			}
			got := statsSum(run(`* | stats by ("resource_attr:service.name") count() n`))
			if got != len(rows) {
				t.Errorf("by service sum = %d want %d", got, len(rows))
			}
		})
	}
}

func statsSum(rows []map[string]string) int {
	n := 0
	for _, r := range rows {
		v, _ := strconv.Atoi(r["n"])
		n += v
	}
	return n
}
