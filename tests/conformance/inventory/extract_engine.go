package inventory

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// LogsQL features are one file per feature in lib/logstorage:
//
//	pipe_<name>.go, filter_<name>.go, stats_<name>.go (tests and *_local/_remote/_timing variants excluded).
var engineFileRe = regexp.MustCompile(`^(pipe|filter|stats)_([a-z0-9_]+)\.go$`)
var engineSkip = regexp.MustCompile(`(_local|_remote|_timing)$`)
var parserTableRe = regexp.MustCompile(`"([a-z_0-9]+)"\s*:\s*parse[A-Za-z0-9_]+`)

func parserTableNames(vlDir, relFile string) (map[string]bool, error) {
	data, err := os.ReadFile(filepath.Join(vlDir, relFile))
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	for _, m := range parserTableRe.FindAllStringSubmatch(string(data), -1) {
		result[m[1]] = true
	}
	return result, nil
}

func ExtractEngine(vlDir string) ([]Item, error) {
	dir := filepath.Join(vlDir, "lib", "logstorage")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Load parser tables for validation
	pipeNames, err := parserTableNames(vlDir, filepath.Join("lib", "logstorage", "pipe.go"))
	if err != nil {
		return nil, fmt.Errorf("read pipe.go: %w", err)
	}
	statsNames, err := parserTableNames(vlDir, filepath.Join("lib", "logstorage", "pipe_stats.go"))
	if err != nil {
		return nil, fmt.Errorf("read pipe_stats.go: %w", err)
	}

	var out []Item
	for _, e := range ents {
		// Skip test files before regex matching
		if strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		m := engineFileRe.FindStringSubmatch(e.Name())
		if m == nil || engineSkip.MatchString(m[2]) {
			continue
		}

		kind := m[1]
		name := m[2]

		// Validate against parser tables
		if kind == "pipe" {
			if !pipeNames[name] {
				continue
			}
		} else if kind == "stats" {
			if !statsNames[name] {
				continue
			}
		} else if kind == "filter" {
			// Skip the generic filter (internal wrapper)
			if name == "generic" {
				continue
			}
		}

		out = append(out, Item{Kind: kind, Surface: "vl", Name: name, Source: filepath.ToSlash(filepath.Join("lib", "logstorage", e.Name()))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind+out[i].Name < out[j].Kind+out[j].Name })
	return out, nil
}

// TraceQL metric-pipe functions are recognized in lib/traceql/pipe_metrics.go
// by the lexer testing the next token against a keyword literal:
// lex.isKeyword("rate"), lex.isKeyword("count_over_time", "min_over_time",
// ...), etc. Rather than hard-coding the function-name list here (which
// silently stops seeing a 10th upstream function if VT ever adds one),
// isKeywordCallRe extracts every such call's argument list so the function
// names can be derived straight from the source.
var isKeywordCallRe = regexp.MustCompile(`lex\.isKeyword\(([^)]*)\)`)
var quotedArgRe = regexp.MustCompile(`"([^"]*)"`)

// traceqlIdentRe matches an isKeyword() argument that looks like a function
// name (lower-snake-case identifier) rather than punctuation ("(", ")",
// ",", "{", "}") or the empty-string sentinel used for EOF checks.
var traceqlIdentRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// traceqlNonFunctionKeywords lists identifier-shaped isKeyword() literals
// that are structural syntax, not metric-pipe function names in their own
// right: "with" introduces the `compare(...) with (...)` clause modifier
// that follows the compare() function call, so it is a keyword the parser
// checks for but never a pipe-stage function name on its own.
var traceqlNonFunctionKeywords = map[string]bool{"with": true}

func ExtractTraceQL(vtDir string) ([]Item, error) {
	src := filepath.Join("lib", "traceql", "pipe_metrics.go")
	data, err := os.ReadFile(filepath.Join(vtDir, src))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []Item
	for _, call := range isKeywordCallRe.FindAllStringSubmatch(string(data), -1) {
		for _, arg := range quotedArgRe.FindAllStringSubmatch(call[1], -1) {
			name := arg[1]
			if !traceqlIdentRe.MatchString(name) || traceqlNonFunctionKeywords[name] {
				continue
			}
			if !seen[name] {
				seen[name] = true
				out = append(out, Item{Kind: "traceql", Surface: "vt", Name: name, Source: filepath.ToSlash(src)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) == 0 {
		return nil, fmt.Errorf("no TraceQL metric functions found in %s", src)
	}
	return out, nil
}
