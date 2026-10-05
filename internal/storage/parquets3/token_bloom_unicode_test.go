package parquets3

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestNativeTokenUnicodeBoundaries(t *testing.T) {
	bodies := []string{"éclair 000é", "éclair", "Ωmega", "café", "foo_ébar", "²foo", "a\u0301foo", "\xffclair", "\xa9clair", "🙂clair", "000é", "clair000", "_éclair", "é_clair"}
	phrases := []string{"\xa9clair", "clair", "éclair", "\xc3", "é", "\xa9", "000", "\xc3\xa9clair", "Ωmega", "mega", "foo", "²foo", "a", "clair", "bar", "_é", "é_clair"}
	matches := 0
	for _, body := range bodies {
		for _, phrase := range phrases {
			for _, suffix := range []string{"", "*"} {
				text := "_msg:" + strconv.Quote(phrase) + suffix
				q, err := logstorage.ParseQuery(text)
				if err != nil {
					t.Fatal(err)
				}
				if !logstorage.QueryFilter(q).MatchRow([]logstorage.Field{{Name: "_msg", Value: body}}) {
					continue
				}
				matches++
				truth := map[string]bool{}
				for _, tok := range tokenize(body) {
					truth[tok] = true
				}
				for _, tok := range searchTokensFromQuery(q) {
					if !truth[tok] {
						t.Errorf("body=%q query=%s native=%q requires absent %q", body, text, logstorage.QueryRequiredMessageTokens(q), tok)
					}
				}
			}
		}
	}
	t.Logf("checked %d native matches", matches)
	if matches < 20 {
		t.Fatal("vacuous native corpus")
	}
}

func TestPersistedInvalidByteMessagePhrase(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.AddTraceRows([]schema.TraceRow{{TimestampUnixNano: base.UnixNano(), TraceID: "unicode-trace", SpanID: "unicode-span", Body: "éclair 000é", SpanName: "valid customer name"}})
	bw.triggerFlush()
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
	predicate := `_msg:"\xa9clair"`
	for _, query := range []string{"(" + predicate + ") OR trace_id:=definitely_missing", predicate} {
		rows := run(query)
		if len(rows) != 1 || rows[0]["trace_id"] != "unicode-trace" || rows[0]["_msg"] != "éclair 000é" {
			t.Fatalf("persisted valid Body with native byte-phrase match must not be pruned: query=%s rows=%v tokens=%q", query, rows, extractSearchTokens(query))
		}
	}
}

func TestRawMessageLiteralGuard(t *testing.T) {
	bad := strconv.Quote("\xa9clair")
	for _, literal := range []string{bad, bad + "*", "=" + bad, "=" + bad + "*", "*" + bad + "*", "seq(" + bad + ")", "pattern_match(" + bad + ")", "pattern_match_full(" + bad + ")", "pattern_match_prefix(" + bad + ")", "pattern_match_suffix(" + bad + ")"} {
		for _, query := range []string{"_msg:" + literal + " _msg:stablemarker", "(_msg:" + literal + ") OR _msg:stablemarker", "NOT _msg:" + literal + " _msg:stablemarker"} {
			q, err := logstorage.ParseQuery(query)
			if err != nil {
				t.Fatalf("invalid test syntax %q: %v", query, err)
			}
			if got := logstorage.QueryRequiredMessageTokens(q); len(got) != 0 {
				t.Errorf("raw invalid message literal escaped guard: query=%s tokens=%q", query, got)
			}
		}
	}
	q, err := logstorage.ParseQuery("name:" + bad + " _msg:stablemarker")
	if err != nil {
		t.Fatal(err)
	}
	if got := logstorage.QueryRequiredMessageTokens(q); len(got) != 1 || got[0] != "stablemarker" {
		t.Fatalf("invalid customer value must preserve safe native pruning: %q", got)
	}
}
