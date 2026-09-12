package traceql

// Mirrors the real pipe_metrics.go shape ExtractTraceQL parses: metric-pipe
// function names are recognized via lex.isKeyword(...) calls, some with
// multiple alternatives in one call, plus a structural "with" keyword that
// must NOT be surfaced as a function name.
func parseRate(lex *lexer) {
	if !lex.isKeyword("rate") {
		return
	}
	if !lex.isKeyword("(") {
		return
	}
}

func parseAggOverTime(lex *lexer) {
	if !lex.isKeyword("count_over_time", "histogram_over_time") {
		return
	}
}

func parseCompareWith(lex *lexer) {
	if !lex.isKeyword("with") {
		return
	}
	if !lex.isKeyword(")") {
		return
	}
}
