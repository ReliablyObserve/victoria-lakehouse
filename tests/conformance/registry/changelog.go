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

// ChangelogUnreleased is the version name of the `## [Unreleased]` section.
const ChangelogUnreleased = "Unreleased"

// Changelog is what the feature catalog reads from CHANGELOG.md: the version
// headings and every `### Added` bullet. It is the only source of a feature's
// release information — the release workflow rewrites CHANGELOG.md after
// every release (it moves the `[Unreleased]` bullets under a new version
// heading) and never touches the catalog, so a release recorded in the catalog
// itself would go stale the moment the feature shipped.
type Changelog struct {
	Versions []string          // version headings in file order, newest first, brackets stripped
	Added    []ChangelogBullet // every top-level `### Added` bullet, in file order
}

// ParseChangelog reads the changelog at path.
func ParseChangelog(path string) (*Changelog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseChangelogBytes(data), nil
}

// ParseChangelogBytes is ParseChangelog on an in-memory changelog.
func ParseChangelogBytes(data []byte) *Changelog {
	return &Changelog{Versions: ChangelogVersions(data), Added: ParseChangelogAddedBytes(data)}
}

// Released returns the released versions — every version heading except
// `[Unreleased]` — newest first. A nil changelog has none.
func (c *Changelog) Released() []string {
	if c == nil {
		return nil
	}
	out := make([]string, 0, len(c.Versions))
	for _, v := range c.Versions {
		if v != ChangelogUnreleased {
			out = append(out, v)
		}
	}
	return out
}

// IsReleased reports whether v is a released version heading.
func (c *Changelog) IsReleased(v string) bool {
	for _, r := range c.Released() {
		if r == v {
			return true
		}
	}
	return false
}

// Newest returns the newest released version, or "" when nothing has been
// released yet.
func (c *Changelog) Newest() string {
	if r := c.Released(); len(r) > 0 {
		return r[0]
	}
	return ""
}

// Previous returns the released version that directly precedes the released
// version v in the changelog — the release v came right after — or "" when v
// is the oldest release or not a release at all.
func (c *Changelog) Previous(v string) string {
	r := c.Released()
	for i := range r {
		if r[i] == v && i+1 < len(r) {
			return r[i+1]
		}
	}
	return ""
}

// FeatureVersions returns the distinct versions of the `### Added` bullets
// whose lead-ins the feature claims, oldest first (`Unreleased`, when present,
// comes last). A claimed lead-in the changelog has no bullet for contributes
// nothing; the feature gate reports it.
func (c *Changelog) FeatureVersions(f *Feature) []string {
	if c == nil || len(f.ChangelogBullets) == 0 {
		return nil
	}
	claimed := make(map[string]bool, len(f.ChangelogBullets))
	for _, lead := range f.ChangelogBullets {
		claimed[lead] = true
	}
	seen := map[string]bool{}
	var newestFirst []string
	for _, b := range c.Added {
		if b.Version == "" || !claimed[b.LeadIn] || seen[b.Version] {
			continue
		}
		seen[b.Version] = true
		newestFirst = append(newestFirst, b.Version)
	}
	out := make([]string, 0, len(newestFirst))
	for i := len(newestFirst) - 1; i >= 0; i-- {
		out = append(out, newestFirst[i])
	}
	return out
}

// HasLeadIn reports whether some `### Added` bullet carries exactly this bold
// lead-in.
func (c *Changelog) HasLeadIn(lead string) bool {
	if c == nil {
		return false
	}
	for _, b := range c.Added {
		if b.LeadIn == lead {
			return true
		}
	}
	return false
}
