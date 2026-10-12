package registry

import (
	"strings"
	"testing"
)

func TestMDEscape(t *testing.T) {
	cases := []struct{ in, want string }{
		{"internal/x/x.go", "internal/x/x.go"},
		{"scripts/ci/a`<!--", "scripts/ci/a'&lt;!--"},
		{"a|b", `a\|b`},
		{"a&b<c>d", "a&amp;b&lt;c&gt;d"},
		{"a\r\nb", "a  b"},
		{"a b c\u0085d", "a b c d"},
		{"tab\there", "tab here"},
		{"::error::boom", "::error::boom"}, // Markdown-inert; the log path handles `::`
	}
	for _, c := range cases {
		if got := MDEscape(c.in); got != c.want {
			t.Errorf("MDEscape(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	for _, in := range []string{"x`y", "x<!--y", "x\ny", "x|y"} {
		out := MDEscape(in)
		if strings.ContainsAny(out, "`<\n\r") || strings.Contains(strings.ReplaceAll(out, `\|`, ""), "|") {
			t.Errorf("MDEscape(%q) = %q still holds Markdown syntax", in, out)
		}
	}
}

func TestWorkflowEscape(t *testing.T) {
	if got := WorkflowData("100%\r\nnext"); got != "100%25%0D%0Anext" {
		t.Errorf("WorkflowData = %q", got)
	}
	if got := WorkflowProperty("a:b,c%\n"); got != "a%3Ab%2Cc%25%0A" {
		t.Errorf("WorkflowProperty = %q", got)
	}
}

func TestLogSafe(t *testing.T) {
	for _, in := range []string{"::error::x", "  ::stop-commands::y", "a\n::error::forged", "a\r::warning::b", "a ::error::c"} {
		got := LogSafe(in)
		if strings.ContainsAny(got, "\r\n ") {
			t.Errorf("LogSafe(%q) = %q holds a line break", in, got)
		}
		for _, line := range strings.Split(got, "\n") {
			if strings.HasPrefix(strings.TrimLeft(line, " \t"), "::") {
				t.Errorf("LogSafe(%q) = %q can be parsed as a workflow command", in, got)
			}
		}
	}
	if got := LogSafe("tests/parity/a_test.go#TestX"); got != "- tests/parity/a_test.go#TestX" {
		t.Errorf("a plain line is only prefixed: %q", got)
	}
}

func TestWorkflowWarnings(t *testing.T) {
	var paths []string
	for i := 0; i < 12; i++ {
		paths = append(paths, "tests/parity/f"+string(rune('a'+i))+"_test.go")
	}
	got := WorkflowWarnings(paths, "lock-affecting file: the owner's approval is the control", 10)
	if len(got) != 11 {
		t.Fatalf("10 annotations and one summary expected, got %d: %v", len(got), got)
	}
	if got[0] != "::warning file=tests/parity/fa_test.go::lock-affecting file: the owner's approval is the control" {
		t.Errorf("first annotation: %q", got[0])
	}
	if !strings.Contains(got[10], "and 2 more") {
		t.Errorf("last annotation: %q", got[10])
	}
	forged := WorkflowWarnings([]string{"a:b,c\n::error::x"}, "m", 10)
	if len(forged) != 1 || strings.ContainsAny(forged[0], "\n\r") || strings.Count(forged[0], "::") != 2 {
		t.Errorf("a hostile path must stay inside its own property: %q", forged)
	}
	if got := WorkflowWarnings([]string{"a"}, "m", 10); len(got) != 1 {
		t.Errorf("no 'more' line under the cap: %v", got)
	}
}
