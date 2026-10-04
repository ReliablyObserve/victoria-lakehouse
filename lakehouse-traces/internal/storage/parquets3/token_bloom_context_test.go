package parquets3

import (
	"context"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestSearchTokensQueryScope(t *testing.T) {
	for _, query := range []string{`_msg:"stable message"`, `name:="HTTP GET /api/v1/users"`} {
		q, err := logstorage.ParseQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		ctx := withSearchTokens(context.Background(), q)
		want := searchTokensFromQuery(q)
		// The file scanner must reuse even an empty cached token set rather
		// than parsing unrelated text or allowing another file to change it.
		for i := 0; i < 10; i++ {
			if got := searchTokensFromContext(ctx, `_msg:unrelated`); !reflect.DeepEqual(got, want) {
				t.Fatalf("query=%q cached=%v want=%v", query, got, want)
			}
		}
		if got := searchTokensFromContext(context.Background(), query); !reflect.DeepEqual(got, want) {
			t.Fatalf("direct file caller=%v want=%v", got, want)
		}
	}
}

func BenchmarkTokenBloomCachedQuery(b *testing.B) {
	q, err := logstorage.ParseQuery(`_msg:"stable message"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := withSearchTokens(context.Background(), q)
	queryStr := q.String()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if tokens := searchTokensFromContext(ctx, queryStr); len(tokens) != 2 || tokens[0] != "stable" || tokens[1] != "message" {
			b.Fatalf("invalid cached guarantees=%v", tokens)
		}
	}
}
