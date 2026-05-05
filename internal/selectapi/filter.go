package selectapi

import (
	"regexp"
	"strings"
	"unicode"
)

// FilterOp represents the type of filter operation.
type FilterOp int

const (
	FilterAnd       FilterOp = iota // Logical AND of children
	FilterOr                        // Logical OR of children
	FilterNot                       // Logical NOT of children[0]
	FilterExact                     // field:="value"
	FilterSubstring                 // field:value or field:"value"
	FilterRegex                     // field:~"regex"
	FilterMatchAll                  // *
)

// FilterNode is a node in the filter AST.
type FilterNode struct {
	Op       FilterOp
	Field    string
	Value    string
	Regex    *regexp.Regexp
	Children []*FilterNode
}

// ParseFilter parses a LogsQL-style query string into a filter AST.
// Supports: field:="exact", field:value (substring), field:~"regex",
// AND, OR, NOT, parentheses, and * (match all).
// Default conjunction (space-separated terms without explicit operator) is AND.
// Operator precedence: NOT > AND > OR.
func ParseFilter(query string) *FilterNode {
	query = strings.TrimSpace(query)
	if query == "" || query == "*" {
		return &FilterNode{Op: FilterMatchAll}
	}
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return &FilterNode{Op: FilterMatchAll}
	}
	node, _ := parseOr(tokens, 0)
	if node == nil {
		return &FilterNode{Op: FilterMatchAll}
	}
	return node
}

// EvaluateFilter evaluates a filter AST against a column map at a given row index.
func EvaluateFilter(node *FilterNode, colMap map[string][]string, rowIdx int) bool {
	if node == nil {
		return true
	}
	switch node.Op {
	case FilterMatchAll:
		return true
	case FilterExact:
		val := getColVal(colMap, node.Field, rowIdx)
		return val == node.Value
	case FilterSubstring:
		val := getColVal(colMap, node.Field, rowIdx)
		return strings.Contains(val, node.Value)
	case FilterRegex:
		val := getColVal(colMap, node.Field, rowIdx)
		if node.Regex == nil {
			return false
		}
		return node.Regex.MatchString(val)
	case FilterNot:
		if len(node.Children) == 0 {
			return true
		}
		return !EvaluateFilter(node.Children[0], colMap, rowIdx)
	case FilterAnd:
		for _, child := range node.Children {
			if !EvaluateFilter(child, colMap, rowIdx) {
				return false
			}
		}
		return true
	case FilterOr:
		for _, child := range node.Children {
			if EvaluateFilter(child, colMap, rowIdx) {
				return true
			}
		}
		return len(node.Children) == 0
	}
	return false
}

func getColVal(colMap map[string][]string, field string, rowIdx int) string {
	if vals, ok := colMap[field]; ok && rowIdx < len(vals) {
		return vals[rowIdx]
	}
	return ""
}

// tokenize splits a query string into tokens: "(", ")", "AND", "OR", "NOT", and filter terms.
func tokenize(query string) []string {
	var tokens []string
	i := 0
	runes := []rune(query)
	n := len(runes)

	for i < n {
		ch := runes[i]

		// Skip whitespace
		if unicode.IsSpace(ch) {
			i++
			continue
		}

		// Parentheses
		if ch == '(' || ch == ')' {
			tokens = append(tokens, string(ch))
			i++
			continue
		}

		// Read a word/term (may contain colons, quotes, tildes, etc.)
		start := i
		for i < n && !unicode.IsSpace(runes[i]) && runes[i] != '(' && runes[i] != ')' {
			if runes[i] == '"' {
				// Consume quoted string (including quotes)
				i++
				for i < n && runes[i] != '"' {
					i++
				}
				if i < n {
					i++ // closing quote
				}
			} else {
				i++
			}
		}
		token := string(runes[start:i])
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// parseOr handles OR expressions (lowest precedence).
func parseOr(tokens []string, pos int) (*FilterNode, int) {
	left, pos := parseAnd(tokens, pos)
	if left == nil {
		return nil, pos
	}

	var children []*FilterNode
	children = append(children, left)

	for pos < len(tokens) && strings.EqualFold(tokens[pos], "OR") {
		pos++ // consume OR
		right, newPos := parseAnd(tokens, pos)
		if right == nil {
			break
		}
		children = append(children, right)
		pos = newPos
	}

	if len(children) == 1 {
		return children[0], pos
	}
	return &FilterNode{Op: FilterOr, Children: children}, pos
}

// parseAnd handles AND expressions and implicit conjunction (space-separated = AND).
func parseAnd(tokens []string, pos int) (*FilterNode, int) {
	left, pos := parseNot(tokens, pos)
	if left == nil {
		return nil, pos
	}

	var children []*FilterNode
	children = append(children, left)

	for pos < len(tokens) {
		// Explicit AND
		if strings.EqualFold(tokens[pos], "AND") {
			pos++ // consume AND
			right, newPos := parseNot(tokens, pos)
			if right == nil {
				break
			}
			children = append(children, right)
			pos = newPos
			continue
		}

		// Implicit AND: next token is not OR, not ")", and not empty
		tok := tokens[pos]
		if strings.EqualFold(tok, "OR") || tok == ")" {
			break
		}

		// It's an implicit AND — parse next term
		right, newPos := parseNot(tokens, pos)
		if right == nil {
			break
		}
		children = append(children, right)
		pos = newPos
	}

	if len(children) == 1 {
		return children[0], pos
	}
	return &FilterNode{Op: FilterAnd, Children: children}, pos
}

// parseNot handles NOT prefix (highest precedence among logical ops).
func parseNot(tokens []string, pos int) (*FilterNode, int) {
	if pos >= len(tokens) {
		return nil, pos
	}

	if strings.EqualFold(tokens[pos], "NOT") {
		pos++                                  // consume NOT
		child, newPos := parseNot(tokens, pos) // NOT is right-associative
		if child == nil {
			return nil, newPos
		}
		return &FilterNode{Op: FilterNot, Children: []*FilterNode{child}}, newPos
	}

	return parsePrimary(tokens, pos)
}

// parsePrimary handles parenthesized expressions and leaf filter terms.
func parsePrimary(tokens []string, pos int) (*FilterNode, int) {
	if pos >= len(tokens) {
		return nil, pos
	}

	tok := tokens[pos]

	// Parenthesized expression
	if tok == "(" {
		pos++ // consume (
		node, newPos := parseOr(tokens, pos)
		pos = newPos
		if pos < len(tokens) && tokens[pos] == ")" {
			pos++ // consume )
		}
		return node, pos
	}

	// Match-all wildcard
	if tok == "*" {
		return &FilterNode{Op: FilterMatchAll}, pos + 1
	}

	// Filter term: field:="value", field:~"regex", field:value, field:"value"
	return parseFilterTerm(tok), pos + 1
}

// parseFilterTerm parses a single filter term like field:="value", field:~"regex", field:value.
func parseFilterTerm(term string) *FilterNode {
	// Exact match: field:="value"
	if idx := strings.Index(term, `:="`); idx > 0 {
		field := term[:idx]
		val := term[idx+3:]
		val = strings.TrimSuffix(val, `"`)
		return &FilterNode{Op: FilterExact, Field: field, Value: val}
	}

	// Regex match: field:~"regex"
	if idx := strings.Index(term, `:~"`); idx > 0 {
		field := term[:idx]
		pattern := term[idx+3:]
		pattern = strings.TrimSuffix(pattern, `"`)
		re, err := regexp.Compile(pattern)
		if err != nil {
			// Invalid regex — treat as substring match
			return &FilterNode{Op: FilterSubstring, Field: field, Value: pattern}
		}
		return &FilterNode{Op: FilterRegex, Field: field, Value: pattern, Regex: re}
	}

	// Substring match: field:value or field:"value"
	if idx := strings.Index(term, ":"); idx > 0 {
		field := term[:idx]
		val := term[idx+1:]
		val = strings.Trim(val, `"`)
		if val == "" || val == "*" {
			return &FilterNode{Op: FilterMatchAll}
		}
		return &FilterNode{Op: FilterSubstring, Field: field, Value: val}
	}

	// No colon — bare term, treat as substring search across _msg
	return &FilterNode{Op: FilterSubstring, Field: "_msg", Value: term}
}
