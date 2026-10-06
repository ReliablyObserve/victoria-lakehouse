package parquets3

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// A log carrying the same field name twice, where that name is a stream field,
// gives the row two stream tags with one name. VictoriaLogs v1.52.0 rejected
// that when it registered the stream and panicked (single-node ingest, and the
// query of such data written earlier); v1.53.0 accepts it (VictoriaLogs #1603,
// #1604). The Lakehouse ingests through the same upstream storage, so the
// insert buffer must take the row, every select surface must see it, and the
// flush to Parquet must keep it: in the buffer, with the buffer and the object
// both there, and with the object alone (the cold path).

// ingestDuplicateStream adds one row per time whose service.name appears twice
// (so the stream is {service.name="a",service.name="b"}), labelled level.
func (e *restartEnv) ingestDuplicateStream(level string, at ...time.Time) {
	e.t.Helper()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for _, ts := range at {
		lr.MustAdd(logstorage.TenantID{}, ts.UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "a"},
			{Name: "service.name", Value: "b"},
			{Name: "level", Value: level},
			{Name: "_msg", Value: "dup-row"},
		}, -1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()
	e.written[level] += uint64(len(at))
	e.total += len(at)
}

// rowsOf runs queryStr the way the logs binary does (upstream's RunQuery, with
// the pipes executed by VictoriaLogs' own pipe machinery) over the buffer and
// the objects, and returns the rows.
func (e *restartEnv) rowsOf(queryStr string) []map[string]string {
	e.t.Helper()
	from, to := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	return coldSelectRunner(e.t, e.s, from.UnixNano(), to.UnixNano())(queryStr)
}

func TestDuplicateStreamFields_IngestFlushQuery(t *testing.T) {
	e := newRestartEnv(t)
	e.ingestDuplicateStream("DUP", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute), at(rwHour, 12*time.Minute))
	// A row of an ordinary stream next to them: the duplicate must not disturb it.
	e.ingest("ONE", at(rwHour, 13*time.Minute))

	stages := []struct {
		name string
		do   func()
	}{
		{"in the buffer", func() {}},
		{"object and committed segment", e.flush},
		{"object only (cold)", e.reap},
	}
	for _, st := range stages {
		st.do()
		e.check(st.name)
		rows := e.rowsOf(`level:=DUP | fields _msg, level`)
		if len(rows) != 3 {
			t.Fatalf("%s: level:=DUP returned %d rows, want 3: %v", st.name, len(rows), rows)
		}
		for _, r := range rows {
			if r["_msg"] != "dup-row" || r["level"] != "DUP" {
				t.Errorf("%s: row %v, want _msg=dup-row level=DUP", st.name, r)
			}
		}
		// The ordinary stream is still found by its own stream filter.
		if n := len(e.rowsOf(`{service.name="svc-ONE"} | fields _msg`)); n != 1 {
			t.Errorf("%s: the ordinary stream returned %d rows, want 1", st.name, n)
		}
	}
}

// unpack_syslog over stored messages reaches the same RFC5424 parser as syslog
// ingestion. A message whose structured data ends right after `name=` made
// VictoriaLogs v1.52.0 panic there (VictoriaLogs #1786); on v1.53.0 the
// structured data is refused and the row passes through unparsed. Cold rows go
// through the same pipe code, so the query must answer after the flush too.
func TestUnpackSyslog_IncompleteStructuredDataDoesNotPanicCold(t *testing.T) {
	e := newRestartEnv(t)
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i, msg := range []string{
		"<165>1 2026-10-06T10:00:00Z host app 1 ID47 [exampleSDID@32473 iut=",
		"<165>1 2026-10-06T10:00:01Z host app 1 ID48 - well-formed",
	} {
		lr.MustAdd(logstorage.TenantID{}, at(rwHour, time.Duration(10+i)*time.Minute).UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "syslog"},
			{Name: "level", Value: "SYS"},
			{Name: "_msg", Value: msg},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()
	e.written["SYS"] += 2
	e.total += 2

	for _, st := range []struct {
		name string
		do   func()
	}{
		{"in the buffer", func() {}},
		{"object only (cold)", func() { e.flush(); e.reap() }},
	} {
		st.do()
		rows := e.rowsOf(`* | unpack_syslog from _msg | fields hostname, app_name`)
		if len(rows) != 2 {
			t.Fatalf("%s: unpack_syslog returned %d rows, want 2: %v", st.name, len(rows), rows)
		}
	}
}
