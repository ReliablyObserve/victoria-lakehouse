package parquets3

import (
	"strings"
	"testing"
)

// FuzzParseFilterFromQuery feeds parseFilterFromQueryStr arbitrary strings.
// The parser must never panic. Returning nil is acceptable for any input —
// including the recent-bug logfmt shape (`http.status_code=200 error=true`)
// that 77e2f0d tried to support and cacd0d3 reverted. The invariant under
// test is: any future attempt to broaden the grammar via the same wrong
// direction (logfmt-tolerant tags) must produce nil/error from the parser,
// not a panic, and must not silently change semantics.
func FuzzParseFilterFromQuery(f *testing.F) {
	// Canonical shapes the parser is supposed to handle.
	f.Add(`service.name:="api-gateway"`)
	f.Add(`{service.name="api-gateway"}`)
	f.Add(`*`)
	f.Add(`level:"ERROR"`)
	f.Add(`trace_id:in(a,b,c)`)
	f.Add(`service.name:="api-gateway" AND level:="ERROR"`)
	f.Add(`service.name:="api" OR service.name:="web"`)
	f.Add(`NOT service.name:="api"`)

	// Adversarial shapes — must not panic.
	f.Add(`field:="unclosed`)
	f.Add(`{{nested:braces}}`)
	f.Add(`service.name:="foo OR bar"`)         // OR inside quoted literal
	f.Add(`service.name:="foo\"bar"`)           // escaped quote in value
	f.Add(``)                                   // empty
	f.Add(strings.Repeat("*", 1000))            // long input
	f.Add("svc:=\"a\x00b\"")                    // embedded NUL
	f.Add("service.name:=\"héllo-世界\"")        // unicode
	f.Add(`trace_id:in(`)                       // unclosed in()
	f.Add(`field:="a" AND AND field:="b"`)      // doubled keyword
	f.Add(`field:`)                             // dangling colon
	f.Add(`:="value"`)                          // missing field name
	f.Add(`service.name:="api" |`)              // trailing pipe
	f.Add(`service.name:="api" | stats count()`) // pipe with stage

	// Recent-bug shapes (intentionally INVALID; must be rejected, not panic).
	// 77e2f0d attempted logfmt-style tag tolerance; cacd0d3 reverted it
	// to keep 100% VT parity. This fuzz pins that revert.
	f.Add(`http.status_code=200 error=true`)
	f.Add(`http.status_code=200`)
	f.Add(`error=true component=router`)
	f.Add(`k=v k2=v2 k3="quoted v"`)

	f.Fuzz(func(t *testing.T, input string) {
		// Must not panic. Result may be nil.
		_ = parseFilterFromQueryStr(input)
	})
}

// FuzzExtractFilterValuesAST feeds extractFilterValuesAST arbitrary
// (query, fieldName) pairs. Must never panic. Returned slice may be empty.
// Seeds exercise registry-resolution-shaped field names (top-level and
// resource_attr:-prefixed) plus adversarial inputs.
func FuzzExtractFilterValuesAST(f *testing.F) {
	// Canonical pairs.
	f.Add(`service.name:="api-gateway"`, "service.name")
	f.Add(`{service.name="api-gateway"}`, "service.name")
	f.Add(`trace_id:in(a,b,c)`, "trace_id")
	f.Add(`level:="ERROR"`, "level")
	f.Add(`service.name:="api" AND level:="ERROR"`, "service.name")
	f.Add(`service.name:="api" AND level:="ERROR"`, "level")
	f.Add(`service.name:="api" OR service.name:="web"`, "service.name")

	// Registry-resolution shapes.
	f.Add(`resource_attr:service.name:="api"`, "resource_attr:service.name")
	f.Add(`service.name:="api"`, "resource_attr:service.name")
	f.Add(`service.name:="api"`, "")
	f.Add(`service.name:="api"`, strings.Repeat("x", 4096))

	// Adversarial.
	f.Add(`field:="unclosed`, "field")
	f.Add(`*`, "service.name")
	f.Add(``, "")
	f.Add(`field:="val with \"quotes\""`, "field")
	f.Add("svc:=\"a\x00b\"", "svc")
	f.Add(strings.Repeat("*", 1000), "service.name")
	f.Add(`trace_id:in(`, "trace_id")

	// Recent-bug shapes (parser must reject; helper must not panic).
	f.Add(`http.status_code=200 error=true`, "http.status_code")
	f.Add(`http.status_code=200 error=true`, "error")
	f.Add(`k=v k2=v2 k3="quoted v"`, "k")

	f.Fuzz(func(t *testing.T, query, fieldName string) {
		// Must not panic. May return empty slice.
		_ = extractFilterValuesAST(query, fieldName)
	})
}

// FuzzContainsOrOperatorQuoted feeds containsOrOperatorQuoted arbitrary
// strings. The boolean return is allowed to be wrong (this is a pure
// string scanner with no semantic guarantee on adversarial input); the
// only test invariant is "must not panic".
func FuzzContainsOrOperatorQuoted(f *testing.F) {
	// Substring " or " inside vs outside quoted regions.
	f.Add(`service.name:="api" OR service.name:="web"`)
	f.Add(`service.name:="foo or bar"`)
	f.Add(`service.name:="foo OR bar"`)
	f.Add(`a or b`)
	f.Add(`a OR b`)
	f.Add(``)
	f.Add(`or`)
	f.Add(` or `)
	f.Add(` OR `)

	// Escape sequences before quotes.
	f.Add(`field:="a\"b OR c"`)
	f.Add(`field:="a\\" OR field:="b"`)
	f.Add(`field:="trailing escape\`)
	f.Add(`"`)
	f.Add(`""`)
	f.Add(`'mixed " quotes' OR x`)

	// Adversarial bulk.
	f.Add(strings.Repeat(" or ", 250))
	f.Add(strings.Repeat(`"`, 100))
	f.Add("\x00 or \x00")

	f.Fuzz(func(t *testing.T, input string) {
		// Must not panic; correctness of the boolean is not asserted.
		_ = containsOrOperatorQuoted(input)
	})
}
