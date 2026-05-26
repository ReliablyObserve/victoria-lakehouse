package parquets3

import (
	"testing"
)

// TestExtractSearchTokens_SinglePipe verifies that extractSearchTokens correctly
// strips the stats/transform portion after a pipe delimiter.
func TestExtractSearchTokens_SinglePipe(t *testing.T) {
	tokens := extractSearchTokens(`_msg:"error" | stats count()`)
	tokenSet := toSet(tokens)

	if !tokenSet["error"] {
		t.Error("expected token 'error' to be extracted from filter part before pipe")
	}
	// "stats" and "count" are in the transform part after the pipe and should
	// not be included as search tokens.
	if tokenSet["stats"] {
		t.Error("token 'stats' should not be extracted — it's after the pipe")
	}
	if tokenSet["count"] {
		t.Error("token 'count' should not be extracted — it's after the pipe")
	}
}

// TestExtractSearchTokens_MultiplePipes verifies behavior with chained pipes.
// The current implementation does `strings.Index(queryStr, " | ")` which only
// strips at the first pipe. With multiple pipes, the filter portion is still
// correctly extracted since everything after the first pipe is discarded.
func TestExtractSearchTokens_MultiplePipes(t *testing.T) {
	tokens := extractSearchTokens(`_msg:"timeout" | stats count() | sort by (count) desc`)
	tokenSet := toSet(tokens)

	if !tokenSet["timeout"] {
		t.Error("expected token 'timeout' to be extracted from filter part")
	}
	if tokenSet["stats"] {
		t.Error("'stats' should not be in search tokens (after pipe)")
	}
	if tokenSet["sort"] {
		t.Error("'sort' should not be in search tokens (after second pipe)")
	}
	if tokenSet["count"] {
		t.Error("'count' should not be in search tokens (after pipe)")
	}
	if tokenSet["desc"] {
		t.Error("'desc' should not be in search tokens (after pipe)")
	}
}

// TestExtractSearchTokens_NoPipe verifies basic extraction without pipes.
func TestExtractSearchTokens_NoPipe(t *testing.T) {
	tokens := extractSearchTokens(`_msg:"connection refused"`)
	tokenSet := toSet(tokens)

	if !tokenSet["connection"] {
		t.Error("expected 'connection' token")
	}
	if !tokenSet["refused"] {
		t.Error("expected 'refused' token")
	}
}

// TestExtractSearchTokens_PipeInQuotedValue documents the behavior when a pipe
// character appears inside a quoted field value. The current implementation
// uses `strings.Index(queryStr, " | ")` which will incorrectly split the query
// if the quoted value contains " | ".
func TestExtractSearchTokens_PipeInQuotedValue(t *testing.T) {
	tokens := extractSearchTokens(`_msg:"error | failed"`)
	tokenSet := toSet(tokens)

	// The quoted value "error | failed" contains " | " which the current
	// implementation treats as a pipe separator. This means it will truncate
	// to `_msg:"error` and only extract "error", losing "failed".
	//
	// Correct behavior: both "error" and "failed" should be extracted since
	// the pipe is inside quotes.
	if !tokenSet["error"] {
		t.Error("expected 'error' token — always present regardless of pipe handling")
	}
	if !tokenSet["failed"] {
		t.Errorf("expected 'failed' token — pipe inside quotes should not be a separator; "+
			"BUG: strings.Index splits on ' | ' even inside quoted values; got tokens: %v", tokens)
	}
}

// TestExtractSearchTokens_EmptyQuery verifies empty input returns no tokens.
func TestExtractSearchTokens_EmptyQuery(t *testing.T) {
	tokens := extractSearchTokens("")
	if len(tokens) != 0 {
		t.Errorf("expected no tokens for empty query, got %v", tokens)
	}
}

// TestExtractSearchTokens_WildcardOnly verifies that a bare wildcard produces
// no search tokens (wildcards match everything, so no bloom filtering needed).
func TestExtractSearchTokens_WildcardOnly(t *testing.T) {
	tokens := extractSearchTokens("*")
	// "*" is a LogsQL keyword and should be filtered out by isLogsQLKeyword.
	if len(tokens) != 0 {
		t.Errorf("expected no tokens for wildcard query, got %v", tokens)
	}
}

// TestExtractSearchTokens_OnlyPipeNoFilter verifies that a query starting with
// a pipe (no filter portion) returns no tokens.
func TestExtractSearchTokens_OnlyPipeNoFilter(t *testing.T) {
	tokens := extractSearchTokens(` | stats count()`)
	if len(tokens) != 0 {
		t.Errorf("expected no tokens for pipe-only query, got %v", tokens)
	}
}

// TestExtractSearchTokens_NonBodyFieldIgnored verifies that non-body fields
// (like service, level) are not included in bloom filter tokens.
func TestExtractSearchTokens_NonBodyFieldIgnored(t *testing.T) {
	tokens := extractSearchTokens(`service:"api-gateway" _msg:"timeout"`)
	tokenSet := toSet(tokens)

	if !tokenSet["timeout"] {
		t.Error("expected 'timeout' from _msg field")
	}
	// service is not a body field, so "api-gateway" should not be extracted
	// for bloom filtering (bloom filters are built on body content).
	if tokenSet["api"] || tokenSet["gateway"] {
		t.Error("service field values should not be in body bloom tokens")
	}
}

// TestExtractSearchTokens_BareWordBeforePipe verifies that bare words
// (implicitly searching _msg) before a pipe are correctly extracted.
func TestExtractSearchTokens_BareWordBeforePipe(t *testing.T) {
	tokens := extractSearchTokens(`error timeout | stats count()`)
	tokenSet := toSet(tokens)

	if !tokenSet["error"] {
		t.Error("expected bare word 'error' before pipe")
	}
	if !tokenSet["timeout"] {
		t.Error("expected bare word 'timeout' before pipe")
	}
	if tokenSet["stats"] || tokenSet["count"] {
		t.Error("tokens after pipe should not be included")
	}
}

// toSet converts a string slice to a set for easier lookups in tests.
func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}
