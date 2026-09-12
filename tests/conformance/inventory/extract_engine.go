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

		out = append(out, Item{Kind: kind, Name: name, Source: filepath.ToSlash(filepath.Join("lib", "logstorage", e.Name()))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind+out[i].Name < out[j].Kind+out[j].Name })
	return out, nil
}

// TraceQL metric functions appear as quoted keywords in lib/traceql/pipe_metrics.go.
var traceqlFuncRe = regexp.MustCompile(`"(rate|count_over_time|min_over_time|max_over_time|avg_over_time|sum_over_time|quantile_over_time|histogram_over_time|compare)"`)

func ExtractTraceQL(vtDir string) ([]Item, error) {
	src := filepath.Join("lib", "traceql", "pipe_metrics.go")
	data, err := os.ReadFile(filepath.Join(vtDir, src))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []Item
	for _, m := range traceqlFuncRe.FindAllStringSubmatch(string(data), -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, Item{Kind: "traceql", Name: m[1], Source: filepath.ToSlash(src)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) == 0 {
		return nil, fmt.Errorf("no TraceQL metric functions found in %s", src)
	}
	return out, nil
}
