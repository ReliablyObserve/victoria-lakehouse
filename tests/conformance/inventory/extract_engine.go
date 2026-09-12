package inventory

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

// LogsQL features are one file per feature in lib/logstorage:
//
//	pipe_<name>.go, filter_<name>.go, stats_<name>.go (tests and *_local/_remote/_timing variants excluded).
var engineFileRe = regexp.MustCompile(`^(pipe|filter|stats)_([a-z0-9_]+)\.go$`)
var engineSkip = regexp.MustCompile(`(_test|_local|_remote|_timing)$`)

func ExtractEngine(vlDir string) ([]Item, error) {
	dir := filepath.Join(vlDir, "lib", "logstorage")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Item
	for _, e := range ents {
		m := engineFileRe.FindStringSubmatch(e.Name())
		if m == nil || engineSkip.MatchString(m[2]) {
			continue
		}
		out = append(out, Item{Kind: m[1], Name: m[2], Source: filepath.ToSlash(filepath.Join("lib", "logstorage", e.Name()))})
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
		return nil, os.ErrNotExist
	}
	return out, nil
}
