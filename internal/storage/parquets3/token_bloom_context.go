package parquets3

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type searchTokensContextKey struct{}

// withSearchTokens computes native message guarantees once per query. An empty
// slice is also cached, so customer-field queries do not parse again per file.
func withSearchTokens(ctx context.Context, q *logstorage.Query) context.Context {
	return context.WithValue(ctx, searchTokensContextKey{}, searchTokensFromQuery(q))
}

func searchTokensFromContext(ctx context.Context, queryStr string) []string {
	if tokens, ok := ctx.Value(searchTokensContextKey{}).([]string); ok {
		return tokens
	}
	// Direct per-file callers may not have a parsed query attached.
	return extractSearchTokens(queryStr)
}
