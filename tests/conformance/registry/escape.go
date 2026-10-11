package registry

import (
	"fmt"
	"strings"
	"unicode"
)

// The gate prints strings an author controls (file paths, registry row ids, test names, YAML
// values) into three places that treat some characters as syntax: the Markdown report, the
// workflow commands (::warning:: and friends) that GitHub parses out of the job log, and the
// plain log lines around them. Each destination has its own escape; nothing the author wrote
// reaches one of them raw.

// MDEscape makes s safe inside a Markdown table cell, list item or code span: CR, LF and every other
// control or separator character become a space, `&`, `<` and `>` become entities, `|` is escaped, and
// a backtick becomes an apostrophe (it would close a code span). A string without those characters is
// returned unchanged.
func MDEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '&':
			b.WriteString("&amp;")
		case r == '<':
			b.WriteString("&lt;")
		case r == '>':
			b.WriteString("&gt;")
		case r == '|':
			b.WriteString(`\|`)
		case r == '`':
			b.WriteByte('\'')
		case r == '\t':
			b.WriteByte(' ')
		case unicode.IsControl(r) || r == ' ' || r == ' ' || !unicode.IsPrint(r) && !unicode.IsSpace(r):
			b.WriteByte(' ')
		case r != ' ' && unicode.IsSpace(r):
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// WorkflowData escapes the message part of a workflow command (`::warning file=x::<data>`), the way the
// runner decodes it: `%`, CR and LF.
func WorkflowData(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	return strings.ReplaceAll(s, "\n", "%0A")
}

// WorkflowProperty escapes a property value of a workflow command (`file=<property>`): everything
// WorkflowData escapes, and `:` and `,`.
func WorkflowProperty(s string) string {
	s = WorkflowData(s)
	s = strings.ReplaceAll(s, ":", "%3A")
	return strings.ReplaceAll(s, ",", "%2C")
}

// LogSafe makes s one plain, inert log line: control and separator characters are shown as escapes and
// the line is prefixed so it can never start with `::` (the runner trims leading spaces before it looks
// for a command, so indentation alone does not protect a line).
func LogSafe(s string) string {
	var b strings.Builder
	b.WriteString("- ")
	for _, r := range s {
		if unicode.IsControl(r) || r == ' ' || r == ' ' || !unicode.IsPrint(r) && r != ' ' {
			if r < 0x10000 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				fmt.Fprintf(&b, `\U%08x`, r)
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// WorkflowWarnings renders one `::warning file=<path>::<message>` annotation per path, at most max of
// them, then one `::warning::... and N more` annotation. Paths and messages are escaped.
func WorkflowWarnings(paths []string, message string, max int) []string {
	var out []string
	for i, p := range paths {
		if i == max {
			out = append(out, "::warning::"+WorkflowData(fmt.Sprintf("... and %d more (%s); see the job summary", len(paths)-max, message)))
			break
		}
		out = append(out, "::warning file="+WorkflowProperty(p)+"::"+WorkflowData(message))
	}
	return out
}
