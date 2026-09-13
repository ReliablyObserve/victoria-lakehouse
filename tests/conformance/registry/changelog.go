package registry

import (
	"os"
	"regexp"
	"strings"
)

// ChangelogBullet is one top-level bullet of one `### Added` section of
// CHANGELOG.md. The feature catalog must account for every one of them that
// carries a bold lead-in, so a release note can never describe something the
// catalog does not know about.
type ChangelogBullet struct {
	Version string // "0.100.0", or "Unreleased" for the unreleased section
	LeadIn  string // the bold lead-in (text between the leading "**" pair), "" when the bullet has none
	Text    string // the whole bullet, whitespace-normalized
	Line    int    // 1-based line number of the bullet's first line
}

var (
	versionHeadingRe = regexp.MustCompile(`^## \[([^\]]+)\]`)
	sectionHeadingRe = regexp.MustCompile(`^### +(.+?)\s*$`)
	headingRe        = regexp.MustCompile(`^#{1,6} `)
)

// normalizeSpace collapses every run of whitespace (including the newlines of
// a bullet that wraps over several lines) into a single space and trims the
// ends, so a lead-in is one comparable string no matter how the bullet is
// wrapped in the file.
func normalizeSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

// leadIn returns the bold lead-in of a normalized bullet text: the text
// between the opening "**" (which must start the bullet) and the next "**".
// A bullet that does not open with bold — a plain sentence, a sub-detail
// bullet promoted to top level — has no lead-in and returns "".
func leadIn(text string) string {
	if !strings.HasPrefix(text, "**") {
		return ""
	}
	rest := text[2:]
	end := strings.Index(rest, "**")
	if end <= 0 {
		return ""
	}
	return strings.TrimSpace(rest[:end])
}

// ParseChangelogAdded returns every top-level bullet of every `### Added`
// section of the changelog at path, oldest section last (file order).
func ParseChangelogAdded(path string) ([]ChangelogBullet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseChangelogAddedBytes(data), nil
}

// ParseChangelogAddedBytes is ParseChangelogAdded on an in-memory changelog.
//
// A bullet starts at a line beginning with "- " in column 0 and continues
// until the next such line, the next heading, or a horizontal rule — so a
// bullet that wraps over several lines, or carries an indented sub-list,
// is returned as one bullet (its lead-in lives on the first line or two).
func ParseChangelogAddedBytes(data []byte) []ChangelogBullet {
	lines := strings.Split(string(data), "\n")
	var out []ChangelogBullet
	version, section := "", ""
	var cur []string
	curLine := 0

	flush := func() {
		if cur == nil {
			return
		}
		text := normalizeSpace(strings.Join(cur, " "))
		text = strings.TrimPrefix(text, "- ")
		out = append(out, ChangelogBullet{Version: version, LeadIn: leadIn(text), Text: text, Line: curLine})
		cur = nil
	}

	for i, line := range lines {
		switch {
		case versionHeadingRe.MatchString(line):
			flush()
			version = versionHeadingRe.FindStringSubmatch(line)[1]
			section = ""
		case sectionHeadingRe.MatchString(line):
			flush()
			section = sectionHeadingRe.FindStringSubmatch(line)[1]
		case headingRe.MatchString(line) || strings.TrimSpace(line) == "---":
			flush()
		case strings.HasPrefix(line, "- "):
			flush()
			if section == "Added" {
				cur = []string{line}
				curLine = i + 1
			}
		default:
			if cur != nil {
				cur = append(cur, line)
			}
		}
	}
	flush()
	return out
}

// ChangelogVersions returns every version heading of the changelog, in file
// order (newest first), with the brackets stripped.
func ChangelogVersions(data []byte) []string {
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if m := versionHeadingRe.FindStringSubmatch(line); m != nil {
			out = append(out, m[1])
		}
	}
	return out
}
