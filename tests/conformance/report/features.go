package report

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

// README markers delimiting the generated "Key Features" bullets. Everything
// between them is regenerated from the feature catalog; the surrounding
// README structure (the heading, the section order, the prose around it) is
// hand-written and left alone.
const (
	ReadmeFeaturesBegin = "<!-- features:begin -->"
	ReadmeFeaturesEnd   = "<!-- features:end -->"
)

// Status icons for a catalog feature. The distinction that matters is
// IconShipped vs IconDeclared: both are shipped code, but only the first is
// covered by something that can fail. The catalog checks that a linked test
// file and test function exist; it does not run them.
const (
	IconShipped    = "✅" // shipped and covered by a test: >= 1 linked test or >= 1 non-pending registry row
	IconDeclared   = "🟡" // shipped, but every registry row is still pending and no test is linked
	IconInProgress = "🔧"
	IconPlanned    = "📝"
)

// RelinkMarkdown rewrites the in-repo Markdown links of text so they resolve
// from outDir instead of from the repository root.
//
// Catalog highlights and descriptions are authored with repo-root-relative
// targets (`docs/durability.md`) because that is what README.md needs. The
// same text is also rendered into docs/features.md, which lives INSIDE docs/,
// where that target would resolve to docs/docs/durability.md — a broken link
// that fails the documentation site build. outDir is the directory of the
// output file, repo-root-relative ("" for a file at the root), and anchors
// are preserved.
func RelinkMarkdown(text, outDir string) string {
	if outDir == "" || outDir == "." {
		return text
	}
	return markdownLinkRe.ReplaceAllStringFunc(text, func(link string) string {
		m := markdownLinkRe.FindStringSubmatch(link)
		target := m[1]
		if len(registry.MarkdownLinkTargets(link)) == 0 {
			return link // external link or in-page anchor: left alone
		}
		path, anchor, hasAnchor := strings.Cut(target, "#")
		rel, err := filepath.Rel(filepath.FromSlash(outDir), filepath.FromSlash(path))
		if err != nil {
			return link
		}
		out := filepath.ToSlash(rel)
		if hasAnchor {
			out += "#" + anchor
		}
		return strings.Replace(link, "("+target+")", "("+out+")", 1)
	})
}

// markdownLinkRe matches an inline Markdown link: [text](target).
var markdownLinkRe = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)\)`)

// FeatureStatus is one feature with its verification resolved against the
// registry: which rows it cites, whether any of them actually executes, and
// the icon the generated documents show for it.
type FeatureStatus struct {
	Feature  *registry.Feature
	Rows     []*registry.Row // rows cited by the feature, in citation order (missing ids are dropped)
	Missing  []string        // cited row ids the registry does not have
	Icon     string
	Verified bool // covered: a linked test, or a registry row that is not pending
}

// StatusOf resolves one feature's rows and verification state.
func StatusOf(f *registry.Feature, reg *registry.Registry) FeatureStatus {
	st := FeatureStatus{Feature: f}
	executing := len(f.Tests) > 0
	for _, id := range f.Rows {
		r := reg.ByID[id]
		if r == nil {
			st.Missing = append(st.Missing, id)
			continue
		}
		st.Rows = append(st.Rows, r)
		if !r.Pending {
			executing = true
		}
	}
	st.Verified = executing
	switch {
	case f.Status == registry.StatusPlanned:
		st.Icon = IconPlanned
	case f.Status == registry.StatusInProgress:
		st.Icon = IconInProgress
	case executing:
		st.Icon = IconShipped
	default:
		st.Icon = IconDeclared
	}
	return st
}

// Statuses resolves every feature of the catalog, in catalog report order.
func Statuses(set *registry.FeatureSet, reg *registry.Registry) []FeatureStatus {
	out := make([]FeatureStatus, 0, len(set.Features))
	for i := range set.Features {
		out = append(out, StatusOf(&set.Features[i], reg))
	}
	return out
}

// VerificationGaps returns the ids of shipped features with no linked test
// and no executing row (icon 🟡) — the coverage-gap list a maintainer works
// down: shipped code whose only claim of correctness is a declared,
// not-yet-executed row.
func VerificationGaps(set *registry.FeatureSet, reg *registry.Registry) []string {
	var out []string
	for _, st := range Statuses(set, reg) {
		if st.Feature.Status == registry.StatusShipped && !st.Verified {
			out = append(out, st.Feature.ID)
		}
	}
	sort.Strings(out)
	return out
}

// areaTitle renders an area name as a document heading ("ui" -> "UI").
func areaTitle(a registry.Area) string {
	switch a {
	case "ui":
		return "UI"
	case "ops":
		return "Ops"
	default:
		return strings.ToUpper(string(a[:1])) + string(a[1:])
	}
}

// rowSummary renders one cited row for the verification line: its id plus
// the expectation it declares and whether it executes yet.
func rowSummary(r *registry.Row) string {
	state := "executes"
	if r.Pending {
		state = "pending"
	}
	return fmt.Sprintf("`%s` (%s, %s)", r.ID, r.Expect, state)
}

// codeList renders a list of references as inline-code, comma separated.
func codeList(items []string) string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, "`"+it+"`")
	}
	return strings.Join(out, ", ")
}

// FeaturesDocDir is the repo-relative directory docs/features.md lives in;
// Markdown links inside the rendered highlights and descriptions are rewritten
// relative to it (RelinkMarkdown).
const FeaturesDocDir = "docs"

// Document is a generated document whose release references — the releases
// a feature shipped in, read from CHANGELOG.md — are kept apart from its fixed
// text.
//
// After every release the release workflow opens a PR that moves the
// `[Unreleased]` changelog bullets under a new version heading, without
// regenerating the documents. A feature whose bullet was unreleased when
// docs/features.md was generated reads "since: the release after v0.121.0";
// once its bullet sits under `[0.122.0]` the exact rendering is "since:
// v0.122.0". Both statements are true, and that PR must stay green, so
// Accepts takes, for each release reference, either the exact rendering or
// the older one that is still true — "the release after vP" where P is the
// release the entry's version directly follows — while every other byte must
// match. String always renders the exact form, so the next regeneration
// replaces the older one.
type Document struct {
	parts []docPart
}

// docPart is a run of fixed text, or one release reference.
type docPart struct {
	text string   // the fixed text, or the exact rendering of a release reference
	alts []string // for a release reference: the other renderings that are still true
	ref  bool
}

// releaseText is one rendered release reference: its exact form and the older
// forms that Document.Accepts still takes.
type releaseText struct {
	exact string
	alts  []string
}

// text appends fixed text.
func (d *Document) text(s string) {
	d.parts = append(d.parts, docPart{text: s})
}

// printf appends formatted fixed text.
func (d *Document) printf(format string, args ...any) {
	d.text(fmt.Sprintf(format, args...))
}

// release appends a release reference.
func (d *Document) release(r releaseText) {
	d.parts = append(d.parts, docPart{text: r.exact, alts: r.alts, ref: true})
}

// String renders the document with every release reference in its exact form.
func (d *Document) String() string {
	var b strings.Builder
	for _, p := range d.parts {
		b.WriteString(p.text)
	}
	return b.String()
}

// Accepts reports whether have is this document: the same fixed text byte for
// byte, with every release reference in its exact form or in an older form
// that is still true.
func (d *Document) Accepts(have []byte) bool {
	return d.matchFrom(string(have), 0, 0)
}

// matchFrom matches parts[part:] against have[pos:]. A release reference
// whose renderings are prefixes of one another tries each of them.
func (d *Document) matchFrom(have string, part, pos int) bool {
	for ; part < len(d.parts); part++ {
		p := d.parts[part]
		if !p.ref {
			if !strings.HasPrefix(have[pos:], p.text) {
				return false
			}
			pos += len(p.text)
			continue
		}
		var ends []int
		for _, candidate := range append([]string{p.text}, p.alts...) {
			if strings.HasPrefix(have[pos:], candidate) {
				ends = append(ends, pos+len(candidate))
			}
		}
		if len(ends) == 0 {
			return false
		}
		for _, end := range ends[1:] {
			if d.matchFrom(have, part+1, end) {
				return true
			}
		}
		pos = ends[0]
	}
	return pos == len(have)
}

// releaseStyle is where a release reference appears: in a feature's header
// line ("since: v0.100.0") or in its changelog list ("`0.100.0`").
type releaseStyle int

const (
	styleSince releaseStyle = iota
	styleList
)

func (s releaseStyle) version(v string) string {
	if s == styleSince {
		return "v" + v
	}
	return "`" + v + "`"
}

// renderRelease renders a reference to the changelog section version sits
// under. A released version renders as itself. An entry still under
// `[Unreleased]` renders as "the release after vN", N being the newest
// release: a statement that stays true when the release workflow moves the
// entry under the next version heading — which is why a released version
// also accepts "the release after vP", P being the release it directly
// follows ("the first release" when there is none).
func renderRelease(cl *registry.Changelog, version string, style releaseStyle) releaseText {
	after := func(previous string) string {
		if previous == "" {
			return "the first release"
		}
		return "the release after " + style.version(previous)
	}
	if version == registry.ChangelogUnreleased {
		return releaseText{exact: after(cl.Newest())}
	}
	return releaseText{exact: style.version(version), alts: []string{after(cl.Previous(version))}}
}

// RenderFeatures produces docs/features.md: a summary table, the coverage-gap
// list, then every feature grouped by area. The releases each feature shipped
// in are read from changelog (see Document). outDir is the directory the
// document will be written to (FeaturesDocDir in production), which decides
// how in-repo Markdown links are rewritten.
func RenderFeatures(set *registry.FeatureSet, reg *registry.Registry, changelog *registry.Changelog, outDir string) *Document {
	b := &Document{}
	b.printf("# Lakehouse features (GENERATED by `make conformance-gen` — do not edit)\n\n")
	b.printf("Every Lakehouse capability, where it is verified, and where it is documented. Source of truth: `tests/conformance/registry/features/`; the releases a feature shipped in are read from `CHANGELOG.md`, and one whose changelog entry is not released yet shows \"the release after\" the newest release. The same catalog generates the \"Key Features\" bullets in `README.md`; the conformance gate fails a PR whose new `### Added` changelog bullet, route, handler, flag or config key is not represented here — see `tests/conformance/README.md`.\n\n")

	statuses := Statuses(set, reg)

	rowClause := "or a registry row the conformance runner executes"
	if !citesExecutingRow(statuses) {
		rowClause += " (none yet: every registry row the catalog cites is still `pending`, so today " + IconShipped + " means \"a linked test\")"
	}
	b.printf("Legend: %s shipped and covered — the catalog links at least one regression test (the file and the test function are checked to exist; the catalog does not run them) %s · %s shipped, declared only — every linked row is still `pending` and no test is linked · %s in progress · %s planned (documented, not implemented)\n\n",
		IconShipped, rowClause, IconDeclared, IconInProgress, IconPlanned)

	// Summary table: features per area x status.
	b.printf("## Summary\n\n| Area | %s covered by a test | %s declared only | %s in progress | %s planned | Total |\n|---|---|---|---|---|---|\n",
		IconShipped, IconDeclared, IconInProgress, IconPlanned)
	type counts struct{ verified, declared, inProgress, planned int }
	perArea := map[registry.Area]*counts{}
	total := counts{}
	for _, st := range statuses {
		c := perArea[st.Feature.Area]
		if c == nil {
			c = &counts{}
			perArea[st.Feature.Area] = c
		}
		switch st.Icon {
		case IconShipped:
			c.verified++
			total.verified++
		case IconDeclared:
			c.declared++
			total.declared++
		case IconInProgress:
			c.inProgress++
			total.inProgress++
		default:
			c.planned++
			total.planned++
		}
	}
	for _, area := range registry.Areas {
		c := perArea[area]
		if c == nil {
			continue
		}
		n := c.verified + c.declared + c.inProgress + c.planned
		b.printf("| %s | %d | %d | %d | %d | %d |\n", areaTitle(area), c.verified, c.declared, c.inProgress, c.planned, n)
	}
	n := total.verified + total.declared + total.inProgress + total.planned
	b.printf("| **Total** | **%d** | **%d** | **%d** | **%d** | **%d** |\n\n", total.verified, total.declared, total.inProgress, total.planned, n)

	// Coverage gaps.
	b.printf("## Coverage gaps\n\nShipped features with no linked test, whose only verification is a declared, not-yet-executed registry row. Closing a gap means linking a real test in the feature's `tests:`.\n\n")
	gaps := 0
	for _, st := range statuses {
		if st.Feature.Status != registry.StatusShipped || st.Verified {
			continue
		}
		gaps++
		b.printf("- `%s` — %s (%s)\n", st.Feature.ID, st.Feature.Title, areaTitle(st.Feature.Area))
	}
	if gaps == 0 {
		b.printf("None: every shipped feature links at least one test or a registry row that executes.\n")
	}
	b.text("\n")

	for _, area := range registry.Areas {
		features := set.ByArea(area)
		if len(features) == 0 {
			continue
		}
		b.printf("## %s (%d)\n\n", areaTitle(area), len(features))
		for _, f := range features {
			st := StatusOf(f, reg)
			b.printf("### %s %s\n\n", st.Icon, f.Title)
			surfaces := make([]string, 0, len(f.Surfaces))
			for _, s := range f.Surfaces {
				surfaces = append(surfaces, string(s))
			}
			// A feature's releases come from the changelog bullets it claims;
			// only a declared `since:` (an override, or a feature without
			// bullets) and a declared `changelog:` are fixed text.
			versions := changelog.FeatureVersions(f)
			b.printf("`%s` · status: %s", f.ID, f.Status)
			switch {
			case f.Since != "":
				b.printf(" · since: %s", f.Since)
			case len(versions) > 0:
				b.text(" · since: ")
				b.release(renderRelease(changelog, versions[0], styleSince))
			}
			b.printf(" · surfaces: %s\n\n", strings.Join(surfaces, ", "))
			b.printf("%s\n\n", RelinkMarkdown(strings.TrimSpace(f.Highlight), outDir))
			b.printf("%s\n\n", RelinkMarkdown(strings.TrimSpace(f.Description), outDir))

			var verification []string
			if len(st.Rows) > 0 {
				parts := make([]string, 0, len(st.Rows))
				for _, r := range st.Rows {
					parts = append(parts, rowSummary(r))
				}
				verification = append(verification, "rows: "+strings.Join(parts, ", "))
			}
			if len(f.Tests) > 0 {
				verification = append(verification, "tests: "+codeList(f.Tests))
			}
			if len(f.Bench) > 0 {
				verification = append(verification, "bench: "+codeList(f.Bench))
			}
			if len(verification) == 0 {
				verification = append(verification, "none linked yet")
			}
			b.printf("- Verification: %s\n", strings.Join(verification, " · "))
			if len(f.Docs) > 0 {
				b.printf("- Docs: %s\n", codeList(f.Docs))
			}
			switch {
			case len(versions) > 0:
				b.text("- Changelog: ")
				for i, v := range versions {
					if i > 0 {
						b.text(", ")
					}
					b.release(renderRelease(changelog, v, styleList))
				}
				b.text("\n")
			case len(f.Changelog) > 0:
				b.printf("- Changelog: %s\n", codeList(f.Changelog))
			}
			if strings.TrimSpace(f.Notes) != "" {
				b.printf("- Note: %s\n", strings.TrimSpace(f.Notes))
			}
			b.text("\n")
		}
	}

	return b
}

// citesExecutingRow reports whether any feature cites a registry row that is
// not pending — the only way a feature is covered without a linked test.
func citesExecutingRow(statuses []FeatureStatus) bool {
	for _, st := range statuses {
		for _, r := range st.Rows {
			if !r.Pending {
				return true
			}
		}
	}
	return false
}

// RenderReadmeFeatures produces the generated body of README.md's "Key
// Features" block: the highlight of every feature that names a
// `readme_section`, grouped by those sections in README order. Planned and
// in-progress features are marked inline so the README never claims
// something ships before it does.
func RenderReadmeFeatures(set *registry.FeatureSet) string {
	bySection := map[string][]*registry.Feature{}
	for i := range set.Features {
		f := &set.Features[i]
		if f.ReadmeSection == "" {
			continue
		}
		bySection[f.ReadmeSection] = append(bySection[f.ReadmeSection], f)
	}

	var b strings.Builder
	for _, section := range registry.ReadmeSections {
		features := bySection[section]
		if len(features) == 0 {
			continue
		}
		sort.SliceStable(features, func(i, j int) bool { return features[i].ID < features[j].ID })
		fmt.Fprintf(&b, "### %s\n", section)
		for _, f := range features {
			prefix := ""
			switch f.Status {
			case registry.StatusInProgress:
				prefix = IconInProgress + " *(in progress)* "
			case registry.StatusPlanned:
				prefix = IconPlanned + " *(planned)* "
			}
			fmt.Fprintf(&b, "- %s%s\n", prefix, strings.TrimSpace(f.Highlight))
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n") + "\n"
}

// ReplaceMarkedBlock returns doc with the text between the begin and end
// markers replaced by block (the markers themselves are kept). It fails
// loudly when a marker is missing or out of order rather than appending or
// silently rewriting the whole document.
func ReplaceMarkedBlock(doc, begin, end, block string) (string, error) {
	i := strings.Index(doc, begin)
	if i < 0 {
		return "", fmt.Errorf("marker %q not found", begin)
	}
	j := strings.Index(doc, end)
	if j < 0 {
		return "", fmt.Errorf("marker %q not found", end)
	}
	if j < i+len(begin) {
		return "", fmt.Errorf("marker %q appears before %q", end, begin)
	}
	return doc[:i+len(begin)] + "\n" + block + doc[j:], nil
}
