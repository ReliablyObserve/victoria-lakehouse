package parquets3

import (
	"context"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestMessageBloomRequiredTokensSoundness(t *testing.T) {
	cases := []struct {
		q         string
		wantEmpty bool
	}{
		{`"body":="HTTP GET /api/v1/users"`, true},
		{`"message":="HTTP GET /api/v1/users"`, true},
		{`"span_attr:_msg":="HTTP GET /api/v1/users"`, true},
		{`span_id:* name:="HTTP GET /api/v1/users" | stats count()`, true},
		{`"span_attr:body":="HTTP GET /api/v1/users"`, true},
		{`"span_attr:message":="HTTP GET /api/v1/users"`, true},
		{`name:"the body:missingtoken message:missingtoken _msg:missingtoken text"`, true},
		{`NOT _msg:missingtoken`, true},
		{`_msg:missingtoken OR name:="HTTP GET /api/v1/users"`, true},
		{`_msg:nativepre*`, true},
		{`_msg:=nativepre*`, true},
		{`_msg:stablemarker`, false},
		{`_msg:stablemarker | format "missingtoken" as _msg | filter _msg:missingtoken`, false},
		{`_msg:stablemarker name:="HTTP GET /api/v1/users"`, false},
		{`"_msg":stablemarker`, false},
		{`_msg:"stablemarker nativepre"*`, false},
		{`(_msg:stablemarker name:foo) OR (_msg:stablemarker name:bar)`, false},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			got := extractSearchTokens(tc.q)
			if tc.wantEmpty && len(got) != 0 {
				t.Errorf("non-guaranteed message tokens %v from %s", got, tc.q)
			}
			for _, tok := range got {
				if tok == "nativepre" {
					t.Errorf("partial prefix is not a required full token: %v", got)
				}
			}
			if !tc.wantEmpty {
				seen := false
				for _, tok := range got {
					if tok == "stablemarker" {
						seen = true
					}
				}
				if !seen {
					t.Errorf("native required token lost: %v", got)
				}
			}
		})
	}
}
func TestPersistedMessageBloomFilters(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	// Written as the segment drain writes it: one trace group, one object.
	rows := []schema.TraceRow{{TimestampUnixNano: base.UnixNano(), TraceID: "trace", SpanID: "span", Body: "nativeprefixSuffix stablemarker", SpanName: "HTTP GET /api/v1/users", SpanAttributes: map[string]string{"_msg": "HTTP GET /api/v1/users", "body": "HTTP GET /api/v1/users", "message": "HTTP GET /api/v1/users"}}}
	up := &traceGroupUpload{partition: partitionFromNano(rows[0].TimestampUnixNano), rows: rows}
	if err := bw.uploadTraceGroup(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := s.manifest.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	fresh := testStorageWithS3(t, mock.url())
	fresh.cfg.Mode = config.ModeTraces
	fresh.registry = schema.NewRegistry(schema.TracesProfile)
	if err := fresh.manifest.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	run := coldSelectRunner(t, fresh, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	for _, q := range []string{
		`"span_attr:_msg":="HTTP GET /api/v1/users"`,
		`span_id:* name:="HTTP GET /api/v1/users"`,
		`"span_attr:body":="HTTP GET /api/v1/users"`,
		`"span_attr:message":="HTTP GET /api/v1/users"`,
		`NOT _msg:missingtoken`,
		`_msg:missingtoken OR name:="HTTP GET /api/v1/users"`,
		`_msg:nativepre*`,
		`_msg:=nativepre*`,
		`_msg:"stablemarker"`,
		`_msg:"nativeprefixSuffix stablemarker"`,
		`(_msg:stablemarker name:="HTTP GET /api/v1/users") OR (_msg:stablemarker name:missing)`,
	} {
		t.Run(q, func(t *testing.T) {
			control := run("(" + q + ") OR trace_id:=definitely_missing")
			if len(control) != 1 || control[0]["trace_id"] != "trace" {
				t.Fatalf("bloom-bypass fixture oracle query=%s rows=%v", q, control)
			}
			got := run(q)
			if len(got) != 1 || got[0]["trace_id"] != "trace" {
				t.Errorf("persisted message bloom false negative query=%s got=%v tokens=%v", q, got, extractSearchTokens(q))
			}
		})
	}
	pipeline := `_msg:stablemarker | format "missingtoken" as _msg | filter _msg:missingtoken`
	output := run(pipeline)
	if len(output) != 1 || output[0]["trace_id"] != "trace" || output[0]["_msg"] != "missingtoken" {
		t.Errorf("post-message transformation must use original bloom: %v tokens=%v", output, extractSearchTokens(pipeline))
	}
	q := `span_id:* name:="HTTP GET /api/v1/users" | stats count() as n`
	got := run(q)
	if len(got) != 1 || got[0]["n"] != "1" {
		t.Errorf("exact CI regression count=%v tokens=%v", got, extractSearchTokens(q))
	}
	t.Run("direct-file-malformed-query-clears-inherited-tokens", func(t *testing.T) {
		parent, err := logstorage.ParseQuery(`_msg:missingtoken`)
		if err != nil {
			t.Fatal(err)
		}
		lo, hi := base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano()
		files := fresh.manifest.GetFilesForRange(lo, hi)
		if len(files) != 1 {
			t.Fatalf("expected one persisted file: %v", files)
		}
		rows := 0
		err = fresh.QuerySpecificFiles(withSearchTokens(context.Background(), parent), []string{files[0].Key}, lo, hi, `unterminated:"`, nil,
			func(_ uint, db *logstorage.DataBlock) { rows += db.RowsCount() })
		if err != nil || rows != 1 {
			t.Fatalf("malformed direct-file query must decline inherited bloom pruning: rows=%d err=%v", rows, err)
		}
	})
}

func TestMessageTokensNativeMatchImplication(t *testing.T) {
	bodies := []string{"", "-", "nativeprefixSuffix stablemarker", "stablemarker nativeprefixSuffix", "GET plain", "alphabeta stablemarker", "éclair café", "FOO_bar", "xxHTTPGETxx", "Ωmega ²foo"}
	queries := []string{
		`name:="HTTP GET /api/v1/users"`,
		`"span_attr:body":="HTTP GET /api/v1/users"`,
		`"span_attr:message":="HTTP GET /api/v1/users"`,
		`_msg:stablemarker`, `_msg:"stablemarker nativepre"*`, `_msg:"nativeprefixSuffix stablemarker"`, `_msg:nativepre*`, `_msg:=nativepre*`,
		`NOT _msg:missingtoken`, `_msg:missingtoken OR name:="HTTP GET /api/v1/users"`,
		`(_msg:stablemarker name:foo) OR (_msg:stablemarker name:="HTTP GET /api/v1/users")`,
		`_msg:stablemarker NOT name:missing`, `_msg:~"native"`, `_msg:~".*stablemarker.*"`,
		`_msg:~"stablemarker|plain"`, `_msg:~"(?i)foo"`, `_msg:"FOO_bar"`, `_msg:éclair`,
		`* | filter _msg:missingtoken`,
	}
	matched := 0
	for _, text := range queries {
		q, err := logstorage.ParseQueryAtTimestamp(text, 123)
		if err != nil {
			t.Fatalf("bad native query fixture %s: %v", text, err)
		}
		f := logstorage.QueryFilter(q)
		for _, body := range bodies {
			fields := []logstorage.Field{{Name: "_msg", Value: body}, {Name: "name", Value: "HTTP GET /api/v1/users"}, {Name: "span_attr:body", Value: "HTTP GET /api/v1/users"}, {Name: "span_attr:message", Value: "HTTP GET /api/v1/users"}}
			if !f.MatchRow(fields) {
				continue
			}
			matched++
			truth := map[string]bool{}
			for _, tok := range tokenize(body) {
				truth[tok] = true
			}
			for _, tok := range extractSearchTokens(text) {
				if !truth[tok] {
					t.Errorf("native matched body=%q filter=%s but required message token %q is absent", body, text, tok)
				}
			}
		}
	}
	t.Logf("native match implication exercised %d matching query/body pairs", matched)
	if matched < 40 {
		t.Fatalf("native match oracle not exercised: %d", matched)
	}
}

func FuzzMessageTokensMatchImplication(f *testing.F) {
	for _, seed := range []struct {
		body, customer string
		op             uint8
	}{{"-", "HTTP GET /api/v1/users", 1}, {"nativeprefixSuffix stablemarker", "HTTP GET /api/v1/users", 2}, {"nativeprefixSuffix", "customer", 4}, {"stablemarker nativeprefixSuffix", "HTTP GET", 6}, {"éclair café", "éclair", 8}, {"FOO_bar", "foo", 8}, {"", "_msg:missingtoken", 7}, {"actual native", "body:missing message:missing", 3}} {
		f.Add(seed.body, seed.customer, seed.op)
	}
	f.Add("éclair 000é", "\xa9clair", uint8(5))
	f.Fuzz(func(t *testing.T, body, customer string, op uint8) {
		if len(body) > 2048 || len(customer) > 2048 {
			t.Skip()
		}
		quote := strconv.Quote
		runes := []rune(body)
		prefix := string(runes[:len(runes)/2])
		var text string
		switch op % 12 {
		case 0:
			text = `_msg:=` + quote(body)
		case 1:
			text = `name:=` + quote(customer)
		case 2:
			text = `_msg:` + quote(customer) + ` OR name:=` + quote(customer)
		case 3:
			text = `NOT _msg:=` + quote(customer)
		case 4:
			text = `_msg:` + quote(prefix) + `*`
		case 5:
			text = `_msg:` + quote(customer) + ` AND name:=` + quote(customer)
		case 6:
			text = `(_msg:` + quote(prefix) + `* AND name:=` + quote(customer) + `) OR (_msg:=` + quote(body) + `)`
		case 7:
			text = `* | format ` + quote(customer) + ` as _msg | filter _msg:=` + quote(customer)
		case 8:
			text = `_msg:~` + quote(regexp.QuoteMeta(customer))
		case 9:
			text = `name:~` + quote(regexp.QuoteMeta(customer))
		case 10:
			text = `_msg:=` + quote(body) + ` NOT name:=` + quote(customer+"suffix")
		case 11:
			text = `NOT (_msg:=` + quote(customer) + ` OR name:=` + quote("never"+customer) + `)`
		}
		q, err := logstorage.ParseQueryAtTimestamp(text, 123)
		if err != nil {
			t.Skip()
		}
		native := logstorage.QueryFilter(q)
		fields := []logstorage.Field{{Name: "_msg", Value: body}, {Name: "name", Value: customer}}
		if !native.MatchRow(fields) {
			return
		}
		truth := map[string]bool{}
		for _, tok := range tokenize(body) {
			truth[tok] = true
		}
		for _, tok := range extractSearchTokens(text) {
			if !truth[tok] {
				t.Fatalf("native matching body=%q customer=%q filter=%s has no required physical token %q", body, customer, text, tok)
			}
		}
	})
}

func TestNativeMessageTokenOwnershipAndQueryCache(t *testing.T) {
	positive, err := logstorage.ParseQuery(`_msg:"stable message"`)
	if err != nil {
		t.Fatal(err)
	}
	negative, err := logstorage.ParseQuery(`name:="HTTP GET /api/v1/users"`)
	if err != nil {
		t.Fatal(err)
	}
	first := logstorage.QueryRequiredMessageTokens(positive)
	if len(first) != 2 {
		t.Fatalf("native positive tokens=%v", first)
	}
	first[0] = "poisoned"
	second := logstorage.QueryRequiredMessageTokens(positive)
	for _, tok := range second {
		if tok == "poisoned" {
			t.Fatal("export aliases native query token cache")
		}
	}
	empty := withSearchTokens(withSearchTokens(context.Background(), positive), negative)
	if got := searchTokensFromContext(empty, `_msg:unrelated`); len(got) != 0 {
		t.Fatalf("empty query cache must override parent guarantees: %v", got)
	}
	filled := withSearchTokens(empty, positive)
	if got := searchTokensFromContext(filled, `unterminated:"`); len(got) != 2 || got[0] != "stable" || got[1] != "message" {
		t.Fatalf("parsed query cache must survive unrelated unparseable file text: %v", got)
	}
	if got := extractSearchTokens(`unterminated:"`); len(got) != 0 {
		t.Fatalf("direct parse failure must decline pruning: %v", got)
	}
}
