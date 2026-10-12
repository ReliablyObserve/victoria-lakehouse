package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

// A tool error can carry a path the author chose (`x\n::warning title=...::PASS`). It must reach the
// log as one inert line, never as a workflow command.
func TestToolErrIsLogSafe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	rc := toolErr("registry summary:", errors.New("tests/x\n::warning title=forged::PASS: parse error"), nil)
	os.Stderr = old
	_ = w.Close()
	out, _ := io.ReadAll(r)
	if rc != 2 {
		t.Errorf("exit code %d, want 2", rc)
	}
	text := string(out)
	if strings.Count(text, "\n") != 1 {
		t.Errorf("the message must be one line, got %q", text)
	}
	if strings.Contains(text, "\n::warning") || strings.Contains(text, "\r") {
		t.Errorf("a forged command survived: %q", text)
	}
	if !strings.HasPrefix(text, "testlinks: registry summary: ") || !strings.Contains(text, `\u000a::warning`) {
		t.Errorf("unexpected rendering %q", text)
	}
}

// A path that holds ": " (git allows it) must reach the report whole, with its reason apart.
func TestWriteReportKeepsAPathWithColonWhole(t *testing.T) {
	dir := t.TempDir()
	rules := []*rule{{id: "Rule 3", title: "t", note: "n"}}
	lock := []registry.LockChange{{Path: "tests/parity/a: b.go", Reason: "an existing file of a package that holds a lock test"}}
	if err := writeReport(dir, rules, lock, "summary\n"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "details.md"))
	if err != nil {
		t.Fatal(err)
	}
	want := "- `tests/parity/a: b.go`: an existing file of a package that holds a lock test"
	if !strings.Contains(string(raw), want) {
		t.Errorf("details.md lacks %q:\n%s", want, raw)
	}
}

func TestRuleVerdictAndPerfSection(t *testing.T) {
	raise := func(row string, old, now int64) registry.PerfRaise {
		return registry.PerfRaise{Row: row, File: "f.yaml", Field: "s3_bytes", Old: old, New: now}
	}
	cases := []struct {
		name string
		r    rule
		want string
	}{
		{"bare", rule{}, "PASS"},
		{"one lock file", rule{review: 1}, "PASS - 1 lock-affecting file needs review (listed below)"},
		{"two lock files", rule{review: 2}, "PASS - 2 lock-affecting files need review (listed below)"},
		{"one perf counter", rule{perf: []registry.PerfRaise{raise("a", 1000, 1001)}}, "PASS - 1 perf counter raised within 1.0 % (listed below)"},
		{"two perf counters", rule{perf: []registry.PerfRaise{raise("a", 1000, 1001), raise("b", 1000, 1002)}}, "PASS - 2 perf counters raised within 1.0 % (listed below)"},
		{"lock file and perf", rule{review: 1, perf: []registry.PerfRaise{raise("a", 1000, 1001), raise("b", 1000, 1002)}}, "PASS - 1 lock-affecting file needs review and 2 perf counters raised within 1.0 % (listed below)"},
		{"locks files and perf", rule{review: 3, perf: []registry.PerfRaise{raise("a", 1000, 1001)}}, "PASS - 3 lock-affecting files need review and 1 perf counter raised within 1.0 % (listed below)"},
		{"a failure wins over a listed raise", rule{fails: []string{"x"}, perf: []registry.PerfRaise{raise("a", 1000, 1001)}}, "FAIL"},
		{"skipped", rule{skipped: "no"}, "n/a"},
	}
	for _, c := range cases {
		if got := c.r.verdict(); got != c.want {
			t.Errorf("%s: verdict %q, want %q", c.name, got, c.want)
		}
	}
	if perfSection([]*rule{{}}) != "" {
		t.Error("no raise, no section")
	}
	sec := perfSection([]*rule{{perf: []registry.PerfRaise{raise("a", 1000, 1001), raise("b|c", 1000, 1009), raise("a", 2000, 2001)}}})
	for _, want := range []string{"3 counters of 2 rows", "Largest relative increase: +0.90 % (`b\\|c` s3_bytes, 1000 -> 1009)", "- `a` s3_bytes 1000 -> 1001 (+0.10 %)"} {
		if !strings.Contains(sec, want) {
			t.Errorf("section lacks %q:\n%s", want, sec)
		}
	}
	var many []registry.PerfRaise
	for i := 0; i < maxReportLines+5; i++ {
		many = append(many, raise("r", 1000, 1001))
	}
	if !strings.Contains(perfSection([]*rule{{perf: many}}), "… and 5 more") {
		t.Error("the section is capped and says how many are left")
	}
}
