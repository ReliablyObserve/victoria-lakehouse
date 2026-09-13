package registry

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The feature catalog (`registry/features/<area>.yaml`) is the companion of
// the row registry: a row says "this endpoint behaves like X", a Feature says
// "this is a Lakehouse capability, here is where it is verified and where it
// is documented". Every Lakehouse capability that ships must appear here, so
// nothing working can be missed by verification, and the feature highlights
// in README.md / docs/features.md are generated from it rather than written
// by hand.

type Status string
type Area string
type FeatureSurface string

const (
	StatusShipped    Status = "shipped"
	StatusInProgress Status = "in-progress"
	StatusPlanned    Status = "planned"

	// SinceUnreleased marks a feature that has not been released yet: its
	// changelog entry still lives in the `[Unreleased]` section.
	SinceUnreleased = "unreleased"
)

// Areas lists every valid feature area, in the order the generated documents
// group them (docs/features.md sections follow this order, not alphabetical:
// it reads write-path first, then read-path, then everything around them).
var Areas = []Area{
	"ingest",
	"storage",
	"query",
	"cache",
	"compaction",
	"deletion",
	"traces",
	"tenancy",
	"ui",
	"observability",
	"ops",
	"deploy",
	"security",
}

// FeatureSurfaces lists how a feature is reachable: an HTTP API, the UI, a
// command-line flag, the on-S3 storage layout, the ingest path, or a CLI
// tool shipped in the repo.
var FeatureSurfaces = map[FeatureSurface]bool{
	"api": true, "ui": true, "flag": true, "storage": true, "ingest": true, "cli": true,
}

// BenchScenarios lists the query kinds `scripts/bench/run.sh` measures
// (LOG_QUERIES ∪ TRACE_QUERIES). A feature's `bench:` entries must name one
// of these, so a renamed or deleted scenario is caught instead of leaving a
// feature pointing at a benchmark that no longer runs.
// TestBenchScenarios_MatchRunScript keeps this list honest against the script.
var BenchScenarios = map[string]bool{
	"count_total": true, "count_by_service": true, "fulltext": true, "level_filter": true,
	"multi_filter": true, "negation": true, "trace_lookup": true, "high_card": true,
	"scan": true, "service_filter": true, "trace_by_id": true, "span_name": true,
	"slow_spans": true,
}

// ReadmeSections lists the subsections of README.md's "Key Features" block,
// in the order they appear there. A feature's `readme_section` places its
// highlight in one of them; a feature with no `readme_section` is documented
// in docs/features.md only (internal plumbing, tooling, planned work).
var ReadmeSections = []string{
	"Write Path",
	"Read Path",
	"Smart Cache",
	"Cross-Signal Prefetch",
	"Deletion",
	"Loki Drilldown Compatibility",
	"Multi-Tenancy",
	"Tenant Stats & Storage Metrics",
	"Configuration Profiles",
	"Infrastructure",
}

var (
	featureIDRe    = regexp.MustCompile(`^lh\.feature\.[a-z0-9]+\.[a-z0-9_]+$`)
	featureSinceRe = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)
	changelogVerRe = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
)

// Feature is one Lakehouse capability: what it is, whether it ships, and
// every place it is verified (registry rows, tests, benchmark scenarios) or
// documented.
type Feature struct {
	ID     string `yaml:"id"`
	Title  string `yaml:"title"`
	Status Status `yaml:"status"`
	Since  string `yaml:"since"` // "vX.Y.Z" (a CHANGELOG version) or "unreleased"
	Area   Area   `yaml:"area"`

	ReadmeSection string           `yaml:"readme_section,omitempty"`
	Surfaces      []FeatureSurface `yaml:"surfaces"`

	Rows  []string `yaml:"rows,omitempty"`  // registry row ids verifying this feature
	Tests []string `yaml:"tests,omitempty"` // "path" or "path#TestName", relative to the repo root
	Bench []string `yaml:"bench,omitempty"` // bench scenario ids (see BenchScenarios)
	Docs  []string `yaml:"docs,omitempty"`  // "path.md" or "path.md#anchor", relative to the repo root

	Highlight   string `yaml:"highlight"`   // one line, rendered into README.md and docs/features.md
	Description string `yaml:"description"` // a paragraph, rendered into docs/features.md

	// Changelog lists the released versions whose `### Added` bullets belong
	// to this feature; ChangelogBullets lists the exact bold lead-ins of
	// those bullets, which is what the gate matches on.
	Changelog        []string `yaml:"changelog,omitempty"`
	ChangelogBullets []string `yaml:"changelog_bullets,omitempty"`

	Notes string `yaml:"notes,omitempty"`
}

// FeatureSet is the loaded catalog in report order (Areas order, then id).
type FeatureSet struct {
	Features []Feature
	// ByID is built after Features is final; never append to Features
	// afterwards (the map holds pointers into the slice).
	ByID map[string]*Feature
}

// AreaIndex returns the position of an area in Areas, or len(Areas) for an
// unknown one (which Validate rejects anyway, so it only affects ordering of
// an already-failing load).
func AreaIndex(a Area) int {
	for i, known := range Areas {
		if known == a {
			return i
		}
	}
	return len(Areas)
}

// Validate checks everything about a feature that can be decided without
// touching the filesystem or the rest of the catalog (id shape, enums,
// required prose). Cross-references — rows that must exist in the registry,
// tests and docs that must exist on disk — are checked by LoadFeatures and
// by the feature drift gate.
func (f *Feature) Validate() error {
	var errs []string
	add := func(format string, a ...any) { errs = append(errs, fmt.Sprintf(format, a...)) }

	if !featureIDRe.MatchString(f.ID) {
		add("id %q must match %s", f.ID, featureIDRe)
	} else if want := "lh.feature." + string(f.Area) + "."; !strings.HasPrefix(f.ID, want) {
		add("id %q must start with %q for area %q", f.ID, want, f.Area)
	}

	if strings.TrimSpace(f.Title) == "" {
		add("title required")
	}

	switch f.Status {
	case StatusShipped, StatusInProgress, StatusPlanned:
	default:
		add("status %q invalid (shipped|in-progress|planned)", f.Status)
	}

	if AreaIndex(f.Area) == len(Areas) {
		add("area %q invalid", f.Area)
	}

	switch {
	case f.Since == SinceUnreleased:
	case featureSinceRe.MatchString(f.Since):
	default:
		add("since %q must be vX.Y.Z or %q", f.Since, SinceUnreleased)
	}

	if f.ReadmeSection != "" && !validReadmeSection(f.ReadmeSection) {
		add("readme_section %q is not a README Key Features subsection (%s)", f.ReadmeSection, strings.Join(ReadmeSections, ", "))
	}

	if len(f.Surfaces) == 0 {
		add("surfaces required")
	}
	seenSurface := map[FeatureSurface]bool{}
	for _, s := range f.Surfaces {
		if !FeatureSurfaces[s] {
			add("surfaces: %q invalid", s)
			continue
		}
		if seenSurface[s] {
			add("surfaces: duplicate %q", s)
		}
		seenSurface[s] = true
	}

	for _, b := range f.Bench {
		if !BenchScenarios[b] {
			add("bench: %q is not a scripts/bench/run.sh scenario", b)
		}
	}

	for _, v := range f.Changelog {
		if !changelogVerRe.MatchString(v) && v != "Unreleased" {
			add("changelog: %q must be X.Y.Z or Unreleased", v)
		}
	}
	for _, b := range f.ChangelogBullets {
		if strings.TrimSpace(b) == "" {
			add("changelog_bullets: empty entry")
		}
	}

	if strings.TrimSpace(f.Highlight) == "" {
		add("highlight required")
	}
	if strings.Contains(f.Highlight, "\n") {
		add("highlight must be a single line")
	}
	if strings.TrimSpace(f.Description) == "" {
		add("description required")
	}

	for _, list := range [][]string{f.Rows, f.Tests, f.Bench, f.Docs} {
		for _, entry := range list {
			if strings.TrimSpace(entry) == "" {
				add("empty reference entry")
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return fmt.Errorf("feature %s: %s", f.ID, strings.Join(errs, "; "))
}

func validReadmeSection(s string) bool {
	for _, known := range ReadmeSections {
		if known == s {
			return true
		}
	}
	return false
}

// LoadFeatures reads every *.yaml file under dir (recursively); each file
// holds one or more YAML documents, each a list of features. It validates
// each feature, rejects duplicate ids, rejects unknown YAML keys, and — when
// repoRoot is non-empty — checks that every referenced test and doc actually
// exists, and that no changelog lead-in is claimed by two features.
//
// Features are returned in report order: by area (Areas order), then by id.
func LoadFeatures(dir, repoRoot string) (*FeatureSet, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("features dir %q: %w", dir, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("features dir %q: not a directory", dir)
	}

	set := &FeatureSet{ByID: map[string]*Feature{}}
	var errs []string
	seenIDs := map[string]bool{}
	seenLeadIn := map[string]string{} // lead-in -> feature id

	walk := func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", path, err))
			return nil
		}
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		for {
			var doc []Feature
			err := dec.Decode(&doc)
			if err == io.EOF {
				break
			}
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", path, flatten(err)))
				break
			}
			for _, f := range doc {
				if err := f.Validate(); err != nil {
					errs = append(errs, fmt.Sprintf("%s: %v", path, err))
					continue
				}
				if seenIDs[f.ID] {
					errs = append(errs, fmt.Sprintf("%s: duplicate id %s", path, f.ID))
					continue
				}
				seenIDs[f.ID] = true
				for _, lead := range f.ChangelogBullets {
					if other, ok := seenLeadIn[lead]; ok {
						errs = append(errs, fmt.Sprintf("%s: changelog bullet %q claimed by both %s and %s", path, lead, other, f.ID))
						continue
					}
					seenLeadIn[lead] = f.ID
				}
				if repoRoot != "" {
					for _, ref := range f.Tests {
						if err := CheckTestRef(repoRoot, ref); err != nil {
							errs = append(errs, fmt.Sprintf("%s: feature %s: tests: %v", path, f.ID, err))
						}
					}
					for _, ref := range f.Docs {
						if err := CheckDocRef(repoRoot, ref); err != nil {
							errs = append(errs, fmt.Sprintf("%s: feature %s: docs: %v", path, f.ID, err))
						}
					}
				}
				set.Features = append(set.Features, f)
			}
		}
		return nil
	}

	if err := filepath.WalkDir(dir, walk); err != nil {
		return nil, fmt.Errorf("features dir %q: %w", dir, err)
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return nil, fmt.Errorf("feature catalog invalid:\n  %s", strings.Join(errs, "\n  "))
	}

	sort.SliceStable(set.Features, func(i, j int) bool {
		a, b := set.Features[i], set.Features[j]
		if ai, bi := AreaIndex(a.Area), AreaIndex(b.Area); ai != bi {
			return ai < bi
		}
		return a.ID < b.ID
	})
	for i := range set.Features {
		set.ByID[set.Features[i].ID] = &set.Features[i]
	}
	return set, nil
}

// ByArea returns the features of one area in report order.
func (s *FeatureSet) ByArea(a Area) []*Feature {
	var out []*Feature
	for i := range s.Features {
		if s.Features[i].Area == a {
			out = append(out, &s.Features[i])
		}
	}
	return out
}

// LeadIns maps every changelog lead-in claimed by the catalog to the id of
// the feature claiming it (LoadFeatures has already rejected a lead-in
// claimed twice).
func (s *FeatureSet) LeadIns() map[string]string {
	out := map[string]string{}
	for i := range s.Features {
		for _, lead := range s.Features[i].ChangelogBullets {
			out[lead] = s.Features[i].ID
		}
	}
	return out
}

// splitRef splits a "path#name" reference into its path and name; name is ""
// when the reference is a bare path.
func splitRef(ref string) (path, name string) {
	if i := strings.Index(ref, "#"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return ref, ""
}

// CheckTestRef verifies that a `tests:` entry names something that exists:
// the file must be present under repoRoot, and when the entry carries a
// "#Name" suffix that name must be findable in the file — `func Name(` for a
// Go test, and the bare word for a shell/script scenario key (e.g.
// `scripts/bench/run.sh#count_total`). Linking a test that does not exist is
// worse than linking none: it claims verification the repo does not have.
func CheckTestRef(repoRoot, ref string) error {
	path, name := splitRef(ref)
	if path == "" {
		return fmt.Errorf("%q: empty path", ref)
	}
	if filepath.IsAbs(path) || strings.Contains(path, "..") {
		return fmt.Errorf("%q: must be a repo-relative path", ref)
	}
	full := filepath.Join(repoRoot, filepath.FromSlash(path))
	info, err := os.Stat(full)
	if err != nil {
		return fmt.Errorf("%q: %v", ref, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%q: is a directory, name a file", ref)
	}
	if name == "" {
		return nil
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return fmt.Errorf("%q: %v", ref, err)
	}
	if strings.HasSuffix(path, ".go") {
		if !regexp.MustCompile(`(?m)^func +` + regexp.QuoteMeta(name) + `\s*\(`).Match(data) {
			return fmt.Errorf("%q: no `func %s(` in %s", ref, name, path)
		}
		return nil
	}
	if !regexp.MustCompile(`(?m)\b` + regexp.QuoteMeta(name) + `\b`).Match(data) {
		return fmt.Errorf("%q: %s does not mention %q", ref, path, name)
	}
	return nil
}

// CheckDocRef verifies that a `docs:` entry names a file that exists under
// repoRoot and, when it carries a "#anchor", that some heading in that file
// slugifies to the anchor — so reorganizing a document breaks the gate
// instead of silently leaving the catalog pointing at a vanished section.
func CheckDocRef(repoRoot, ref string) error {
	path, anchor := splitRef(ref)
	if path == "" {
		return fmt.Errorf("%q: empty path", ref)
	}
	if filepath.IsAbs(path) || strings.Contains(path, "..") {
		return fmt.Errorf("%q: must be a repo-relative path", ref)
	}
	full := filepath.Join(repoRoot, filepath.FromSlash(path))
	info, err := os.Stat(full)
	if err != nil {
		return fmt.Errorf("%q: %v", ref, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%q: is a directory, name a file", ref)
	}
	if anchor == "" {
		return nil
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return fmt.Errorf("%q: %v", ref, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !headingRe.MatchString(line) {
			continue
		}
		if SlugifyHeading(strings.TrimLeft(line, "# ")) == anchor {
			return nil
		}
	}
	return fmt.Errorf("%q: no heading in %s slugifies to %q", ref, path, anchor)
}

// SlugifyHeading renders a Markdown heading the way GitHub anchors it:
// lowercased, punctuation dropped, spaces turned into hyphens. Inline
// formatting markers (`code`, **bold**, _italic_) are dropped with the rest
// of the punctuation, matching GitHub's behavior for the heading styles this
// repo uses.
func SlugifyHeading(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '-':
			b.WriteRune('-')
		case r == '_':
			b.WriteRune('_')
		}
	}
	return b.String()
}
