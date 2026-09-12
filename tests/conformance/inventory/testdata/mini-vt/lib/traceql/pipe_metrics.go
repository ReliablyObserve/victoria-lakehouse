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

// parseWithHint mirrors real VT's parsePipeWith: "with" is its own
// standalone query-hint pipe stage (`| with(...)`), not a clause attached
// to another function — see lib/traceql/pipe.go's "with": parsePipeWith
// dispatch entry.
func parseWithHint(lex *lexer) {
	if !lex.isKeyword("with") {
		return
	}
	if !lex.isKeyword(")") {
		return
	}
}
