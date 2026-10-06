package parquets3

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// LogsQL fixes that ship in VictoriaLogs v1.53.0 and live in the library the
// Lakehouse executes its pipes with, so cold, buffered and hot answers must all
// agree with them. Each query runs over the insert buffer, then over the object
// alone (the cold path), and must return the one right answer both times.
//
//   - sort by (_time ...) limit N with a pipe that overwrites _time
//     (VictoriaLogs #1360, #1727): the rows came back out of order.
//   - a quoted constant in the math pipe (e.g. "2026-10-01T00:00:00Z") crashed
//     the query when it ran with a limit.
//   - week_range[Sun,Sun] inside the filter pipe missed Sundays (#1335).

// sunday and thursday are in the window the tests query.
var (
	fixSunday   = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	fixThursday = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
)

func fixStages(e *restartEnv) []struct {
	name string
	do   func()
} {
	return []struct {
		name string
		do   func()
	}{
		{"buffer", func() {}},
		{"object only (cold)", func() { e.flush(); e.reap() }},
	}
}

func (e *restartEnv) fixRows(queryStr string) []map[string]string {
	e.t.Helper()
	from, to := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	return coldSelectRunner(e.t, e.s, from.UnixNano(), to.UnixNano())(queryStr)
}

func TestVL153_SortLimitWithPipeWritingTime(t *testing.T) {
	e := newRestartEnv(t)
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	// _time (the row's own) ascends with i; the time inside the message
	// descends, so a sort by the extracted _time reverses the order.
	for i := 0; i < 5; i++ {
		lr.MustAdd(logstorage.TenantID{}, fixThursday.Add(time.Duration(i)*time.Minute).UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "svc"},
			{Name: "_msg", Value: fmt.Sprintf("2026-10-02T09:0%d:00Z m%d", 4-i, i)},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()

	for _, st := range fixStages(e) {
		st.do()
		rows := e.fixRows(`* | extract "<_time> <rest>" from _msg | sort by (_time desc) limit 3 | fields _time, rest`)
		var got []string
		for _, r := range rows {
			got = append(got, r["rest"])
		}
		if strings.Join(got, ",") != "m0,m1,m2" {
			t.Errorf("%s: sort by (_time desc) limit 3 over extracted _time returned %v (rows %v), want m0,m1,m2 in that order", st.name, got, rows)
		}
		rows = e.fixRows(`* | extract "<_time> <rest>" from _msg | sort by (_time) limit 2 | fields _time, rest`)
		got = got[:0]
		for _, r := range rows {
			got = append(got, r["rest"])
		}
		if strings.Join(got, ",") != "m4,m3" {
			t.Errorf("%s: sort by (_time) limit 2 over extracted _time returned %v, want m4,m3 in that order", st.name, got)
		}
	}
}

func TestVL153_MathQuotedConstantWithLimit(t *testing.T) {
	e := newRestartEnv(t)
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 3; i++ {
		lr.MustAdd(logstorage.TenantID{}, fixThursday.Add(time.Duration(i)*time.Second).UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "svc"},
			{Name: "_msg", Value: fmt.Sprintf("m%d", i)},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()

	for _, st := range fixStages(e) {
		st.do()
		// _time minus the constant is 12h in nanoseconds plus the row's offset.
		rows := e.fixRows(`* | sort by (_time) | math _time - "2026-10-01T00:00:00Z" as since | fields since | limit 5`)
		want := []string{"43200000000000", "43201000000000", "43202000000000"}
		var got []string
		for _, r := range rows {
			got = append(got, r["since"])
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: math with a quoted constant returned %v, want %v", st.name, got, want)
		}
	}
}

func TestVL153_WeekRangeSundayInFilterPipe(t *testing.T) {
	e := newRestartEnv(t)
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for _, c := range []struct {
		at  time.Time
		msg string
	}{{fixSunday, "sunday"}, {fixThursday, "thursday"}} {
		lr.MustAdd(logstorage.TenantID{}, c.at.UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "svc"},
			{Name: "_msg", Value: c.msg},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()

	for _, st := range fixStages(e) {
		st.do()
		for _, q := range []string{`* | filter _time:week_range[Sun, Sun] | fields _msg`, `_time:week_range[Sun, Sun] | fields _msg`} {
			rows := e.fixRows(q)
			if len(rows) != 1 || rows[0]["_msg"] != "sunday" {
				t.Errorf("%s: %s returned %v, want only the Sunday row", st.name, q, rows)
			}
		}
	}
}
