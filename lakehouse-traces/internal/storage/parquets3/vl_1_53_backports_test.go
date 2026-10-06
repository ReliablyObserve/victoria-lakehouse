package parquets3

import (
	"strings"
	"testing"
	"time"
)

// Two VictoriaLogs v1.53.0 fixes the traces binary takes by patch, because the
// VictoriaLogs revision VictoriaTraces v0.12.0 pins (c945d2949e98, v1.52.0)
// predates them:
//
//   - patches/vl-traces/vl-math-keep-quoted-constants.patch (upstream
//     901ca58e0): a quoted constant in the math pipe lost its quotes in the
//     query's string form, so Query.Clone, which the insert-buffer read runs
//     when the window holds a cold file plus buffered rows, re-parsed it,
//     failed and called logger.Panicf; the process exits.
//   - patches/vl-traces/vl-syslog-rfc5424-incomplete-sd.patch (upstream
//     877a61959, VictoriaLogs #1786): unpack_syslog over a stored message whose
//     structured data ends right after `name=` indexed past the line and
//     panicked; the cold path swallows panics and answered with no rows.
//
// Both tests fail without the patches (a process-killing panic, or no rows)
// and pass with them.

func TestVL153Backport_MathQuotedConstantThroughBufferAndColdPath(t *testing.T) {
	e := newRestartEnv(t)
	// Three spans flushed to an object, then two more only in the insert
	// buffer, in the same window: the buffer read clones the query with a time
	// filter while the cold file is also read.
	e.ingest("COLD", at(rwHour, 0), at(rwHour, time.Second), at(rwHour, 2*time.Second))
	e.flush()
	e.reap()
	e.ingest("BUF", at(rwHour, 3*time.Second), at(rwHour, 4*time.Second))
	if objs := len(e.objects()); objs == 0 {
		t.Fatal("fixture: want a cold object")
	}

	from, to := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	run := coldSelectRunner(t, e.s, from.UnixNano(), to.UnixNano())
	rows := run(`* | sort by (_time) | math _time - "2026-10-01T00:00:00Z" as since | fields since | limit 5`)
	// 07:00:00 minus the constant is 7h in nanoseconds, plus the row's offset.
	want := []string{"25200000000000", "25201000000000", "25202000000000", "25203000000000", "25204000000000"}
	var got []string
	for _, r := range rows {
		got = append(got, r["since"])
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("math with a quoted constant returned %v, want %v", got, want)
	}
}

func TestVL153Backport_UnpackSyslogIncompleteStructuredData(t *testing.T) {
	e := newRestartEnv(t)
	const msg = `<165>1 2026-10-06T10:00:00Z myhost app 1 ID47 [exampleSDID@32473 iut=`
	e.ingest(msg, at(rwHour, time.Minute))
	from, to := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	run := coldSelectRunner(t, e.s, from.UnixNano(), to.UnixNano())
	for _, stage := range []struct {
		name string
		do   func()
	}{{"buffer", func() {}}, {"object only (cold)", func() { e.flush(); e.reap() }}} {
		stage.do()
		rows := run(`* | unpack_syslog from name | fields hostname`)
		if len(rows) != 1 || rows[0]["hostname"] != "myhost" {
			t.Errorf("%s: unpack_syslog over incomplete structured data returned %v, want one row with hostname=myhost", stage.name, rows)
		}
	}
}
