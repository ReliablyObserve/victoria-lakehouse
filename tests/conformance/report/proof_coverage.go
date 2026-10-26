package report

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

// ProofCoverageMD and ProofCoverageJSON are the repo-relative paths of the two
// generated proof coverage files. The Markdown is for people, the JSON is the
// machine-readable twin (the verdict and every table) that a PR comment reads.
const (
	ProofCoverageMD   = "docs/proof-coverage.md"
	ProofCoverageJSON = "docs/proof-coverage.json"
)

// proofCoverageSchema is the version of the JSON document's shape. Bump it when
// a field is renamed or removed (adding a field does not need a bump).
const proofCoverageSchema = 1

// differentialTestPrefixes are the test trees whose tests compare Lakehouse
// with a hot VictoriaLogs / VictoriaTraces: tests/parity (the differential
// suite) and tests/e2e (the compose stack with the hot reference). A row that
// targets hot and links a test under one of them is "differentially linked".
// This is a link heuristic, not a proof that the cited test exercises that
// row's exact request: it says a differential test is named, nothing more.
var differentialTestPrefixes = []string{"tests/parity/", "tests/e2e/"}

// Proof classes of a shipped feature, in precedence order.
const (
	ClassUpstreamExact = "upstream-exact"
	ClassLHExtension   = "lh-extension"
	ClassPerfProven    = "perf-proven"
	ClassUnproven      = "unproven"
)

// Reasons a row is still pending, derived from the row's own fields.
const (
	ReasonPerfCell       = "perf-cell"
	ReasonDurabilityCell = "durability-cell"
	ReasonHarness        = "chaos-or-ui-harness"
	ReasonFlagRow        = "flag-row"
	ReasonRunnerLinked   = "http-runner-test-linked"
	ReasonRunnerBare     = "http-runner-nothing-linked"
	ReasonUnclassified   = "unclassified"
)

// pendingReasonOrder is the display order of the pending reasons.
var pendingReasonOrder = []string{
	ReasonPerfCell, ReasonDurabilityCell, ReasonHarness, ReasonFlagRow,
	ReasonRunnerLinked, ReasonRunnerBare, ReasonUnclassified,
}

var pendingReasonText = map[string]struct {
	label     string
	elsewhere bool
}{
	ReasonPerfCell:       {"layer `perf`: a measurement cell run by a perf CI job (its `refs.doc` names the harness), not by the registry runner", true},
	ReasonDurabilityCell: {"kind `durability`: backed by the cited Go tests, no HTTP request", true},
	ReasonHarness:        {"layer `chaos` or `ui`: needs a fault or browser harness, not the HTTP runner", false},
	ReasonFlagRow:        {"kind `flag`: no HTTP request; the flag drift checks and cited tests apply", false},
	ReasonRunnerLinked:   {"has an HTTP request; waits for the registry HTTP runner, a cited Go test covers part of it", false},
	ReasonRunnerBare:     {"has an HTTP request; waits for the registry HTTP runner and links no test", false},
	ReasonUnclassified:   {"none of the above (no request, no cited test, not a perf/durability/flag/chaos/ui row)", false},
}

// ProofInputs is everything the proof coverage report is computed from. Shared
// may be nil (the report then has an empty shared-code section), which keeps
// the registry-only computation testable on a fixture.
type ProofInputs struct {
	Inventory *inventory.Inventory
	Registry  *registry.Registry
	Features  *registry.FeatureSet
	Shared    *SharedCodeInput
}

// ProofCoverage is the computed report. Every slice is sorted and every count
// is derived from the inputs, so the same inputs always give the same bytes.
type ProofCoverage struct {
	SchemaVersion int             `json:"schema_version"`
	Verdict       Verdict         `json:"verdict"`
	Upstream      []KindCoverage  `json:"upstream_by_kind"`
	PassRows      []PassRowSplit  `json:"upstream_pass_rows"`
	PassRowsNone  []RowRef        `json:"upstream_pass_rows_without_test"`
	Registry      RegistryTotals  `json:"registry"`
	Features      FeatureCoverage `json:"features"`
	Shared        SharedCoverage  `json:"shared_code"`
	Pending       []PendingReason `json:"pending_by_reason"`
	NotInventory  []string        `json:"not_inventoried"`
}

// Verdict is the headline numbers: each one is also a line of the Markdown
// header, and the keys are stable for PR comments to read.
type Verdict struct {
	UpstreamItems                     int `json:"upstream_items"`
	UpstreamItemsWithRow              int `json:"upstream_items_with_row"`
	UpstreamItemsNoRow                int `json:"upstream_items_no_row"`
	UpstreamItemsVerified             int `json:"upstream_items_verified"`
	UpstreamItemsDeclaredOnly         int `json:"upstream_items_declared_only"`
	UpstreamItemsDifferentiallyLinked int `json:"upstream_items_differentially_linked"`
	IngestCells                       int `json:"ingest_protocol_cells"`
	IngestCellsVerified               int `json:"ingest_protocol_cells_verified"`

	UpstreamPassRows             int `json:"upstream_pass_rows"`
	UpstreamPassRowsDifferential int `json:"upstream_pass_rows_differential"`
	UpstreamPassRowsOtherTest    int `json:"upstream_pass_rows_other_test"`
	UpstreamPassRowsNoTest       int `json:"upstream_pass_rows_no_test"`

	RowsTotal           int `json:"rows_total"`
	RowsPending         int `json:"rows_pending"`
	PendingNoRunner     int `json:"rows_pending_waiting_for_runner"`
	PendingElsewhere    int `json:"rows_pending_executed_elsewhere"`
	PendingUnclassified int `json:"rows_pending_unclassified"`

	ShippedFeatures         int `json:"shipped_features"`
	ShippedFeaturesNoRows   int `json:"shipped_features_no_rows"`
	ShippedFeaturesNoPerf   int `json:"shipped_features_no_perf_scenario"`
	ShippedFeaturesUnproven int `json:"shipped_features_unproven"`

	SharedCodeItems     int `json:"shared_code_items"`
	SharedCodeUnguarded int `json:"shared_code_unguarded"`
}

// KindCoverage is one line of the per-kind table: the items of one upstream
// kind on one signal ("vl" logs, "vt" traces, "all" both), classified the way
// UPSTREAM_COVERAGE.md aggregates a status (worst covering row wins).
type KindCoverage struct {
	Kind              string `json:"kind"`
	Signal            string `json:"signal"`
	Items             int    `json:"items"`
	WithRow           int    `json:"with_row"`
	Verified          int    `json:"verified"`
	Pending           int    `json:"pending"`
	Differs           int    `json:"differs"`
	AbsentUnsupported int    `json:"absent_or_unsupported"`
	NoRow             int    `json:"no_row"`
	// DifferentiallyLinked counts the items with a covering row that targets
	// hot and links a differential test (it overlaps the status columns).
	DifferentiallyLinked int `json:"differentially_linked"`
}

// PassRowSplit splits the upstream pass rows (surface vl or vt, not a perf
// cell, expect pass) of one signal by how they link tests.
type PassRowSplit struct {
	Signal       string `json:"signal"`
	Rows         int    `json:"rows"`
	TargetHot    int    `json:"target_hot"`
	Differential int    `json:"differential"`
	OtherTest    int    `json:"other_test"`
	NoTest       int    `json:"no_test"`
}

// RowRef identifies a row in a list.
type RowRef struct {
	ID        string `json:"id"`
	Signal    string `json:"signal"`
	Kind      string `json:"kind"`
	TargetHot bool   `json:"target_hot"`
	Pending   bool   `json:"pending"`
}

// RegistryTotals are the plain row counts the other tables are read against.
type RegistryTotals struct {
	Rows        int            `json:"rows"`
	BySurface   map[string]int `json:"by_surface"`
	ByExpect    map[string]int `json:"by_expect"`
	ByOrigin    map[string]int `json:"by_origin"`
	PerfCells   int            `json:"perf_cells"`
	NonPerf     int            `json:"non_perf_rows"`
	Pending     int            `json:"pending_rows"`
	PendingPerf int            `json:"pending_perf_cells"`
}

// FeatureCoverage is the shipped-feature table with its counts.
type FeatureCoverage struct {
	Total      int            `json:"total"`
	ByStatus   map[string]int `json:"by_status"`
	Shipped    int            `json:"shipped"`
	NoRows     int            `json:"shipped_no_rows"`
	NoTests    int            `json:"shipped_no_tests"`
	NoPerf     int            `json:"shipped_no_perf_scenario"`
	HotRows    int            `json:"shipped_citing_a_hot_row"`
	ByClass    map[string]int `json:"shipped_by_proof_class"`
	ShippedAll []FeatureProof `json:"shipped_features"`
}

// FeatureProof is one shipped feature and how it is proven.
type FeatureProof struct {
	ID           string   `json:"id"`
	Area         string   `json:"area"`
	Rows         int      `json:"rows"`
	RowsPending  int      `json:"rows_pending"`
	HotRows      int      `json:"rows_targeting_hot"`
	Tests        int      `json:"tests"`
	Bench        []string `json:"bench"`
	PerfCells    int      `json:"perf_cells"`
	PerfScenario bool     `json:"perf_scenario"`
	ProofClass   string   `json:"proof_class"`
}

// PendingReason is one bucket of pending rows.
type PendingReason struct {
	Reason            string `json:"reason"`
	Rows              int    `json:"rows"`
	ExecutedElsewhere bool   `json:"executed_elsewhere"`
	Description       string `json:"description"`
	// Refs lists the distinct refs.doc paths of the bucket's rows (sorted),
	// which for perf cells names the harness that runs them.
	Refs []string `json:"refs_doc,omitempty"`
}

// ComputeProofCoverage derives the report from the inputs. It reads nothing
// but its arguments.
func ComputeProofCoverage(in ProofInputs) *ProofCoverage {
	pc := &ProofCoverage{
		SchemaVersion: proofCoverageSchema,
		NotInventory: []string{
			"metric names and labels (only the lakehouse_* names are registered; no vl_*/vt_*/vm_* comparison)",
			"error bodies and status codes (rows exist, no inventory of the upstream set)",
			"Jaeger and Tempo response schemas beyond the rows",
			"look-alike re-implementations under internal/ (no scan; tracked in an open issue)",
		},
	}
	reg := in.Registry
	if reg == nil {
		reg = &registry.Registry{ByID: map[string]*registry.Row{}}
	}
	if in.Inventory != nil {
		pc.Upstream = computeKinds(in.Inventory, reg)
	}
	pc.Upstream = append(pc.Upstream, computeIngest(reg)...)
	pc.PassRows, pc.PassRowsNone = computePassRows(reg)
	pc.Registry = computeTotals(reg)
	pc.Pending = computePending(reg)
	if in.Features != nil {
		pc.Features = computeFeatures(in.Features, reg)
	} else {
		pc.Features = FeatureCoverage{ByStatus: map[string]int{}, ByClass: map[string]int{}}
	}
	pc.Shared = computeShared(in.Shared)
	pc.Verdict = computeVerdict(pc)
	return pc
}

func hasPrefixAny(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func rowTargetsHot(r *registry.Row) bool {
	for _, t := range r.Targets {
		if t == registry.TargetHot {
			return true
		}
	}
	return false
}

func rowTests(r *registry.Row) []string {
	if r.Refs == nil {
		return nil
	}
	return r.Refs.Tests
}

// IsDifferentiallyLinked reports whether r targets hot and links at least one
// test under tests/parity/ or tests/e2e/.
func IsDifferentiallyLinked(r *registry.Row) bool {
	if !rowTargetsHot(r) {
		return false
	}
	for _, t := range rowTests(r) {
		if hasPrefixAny(t, differentialTestPrefixes) {
			return true
		}
	}
	return false
}

// itemClass is the status class of one upstream item, aggregated over every
// row covering it exactly as aggregateStatus does.
type itemClass int

const (
	classNoRow itemClass = iota
	classVerified
	classDeclared
	classDiffer
	classAbsent
)

func classifyItem(rows []*registry.Row) itemClass {
	if len(rows) == 0 {
		return classNoRow
	}
	worst := rows[0]
	allPending := true
	for _, r := range rows {
		if expectRank(r.Expect) > expectRank(worst.Expect) {
			worst = r
		}
		if !r.Pending {
			allPending = false
		}
	}
	switch worst.Expect {
	case registry.ExpectDiffer:
		return classDiffer
	case registry.ExpectUnsupported, registry.ExpectAbsent:
		return classAbsent
	}
	if allPending {
		return classDeclared
	}
	return classVerified
}

func (k *KindCoverage) add(c itemClass, diff bool) {
	k.Items++
	if c != classNoRow {
		k.WithRow++
	}
	switch c {
	case classNoRow:
		k.NoRow++
	case classVerified:
		k.Verified++
	case classDeclared:
		k.Pending++
	case classDiffer:
		k.Differs++
	case classAbsent:
		k.AbsentUnsupported++
	}
	if diff {
		k.DifferentiallyLinked++
	}
}

// inventoryKinds is the display order of the inventory kinds.
var inventoryKinds = []string{"route", "pipe", "filter", "stats", "traceql", "flag"}

func computeKinds(inv *inventory.Inventory, reg *registry.Registry) []KindCoverage {
	keys := CoveredKeys(inv, reg)
	type acc struct{ all, vl, vt KindCoverage }
	byKind := map[string]*acc{}
	for _, k := range inventoryKinds {
		byKind[k] = &acc{
			all: KindCoverage{Kind: k, Signal: "all"},
			vl:  KindCoverage{Kind: k, Signal: "vl"},
			vt:  KindCoverage{Kind: k, Signal: "vt"},
		}
	}
	for _, it := range inv.Items {
		a := byKind[it.Kind]
		if a == nil {
			continue
		}
		var rows []*registry.Row
		diff := false
		for _, id := range keys[inv.Key(it)] {
			if r := reg.ByID[id]; r != nil {
				rows = append(rows, r)
				if IsDifferentiallyLinked(r) {
					diff = true
				}
			}
		}
		c := classifyItem(rows)
		a.all.add(c, diff)
		switch it.Surface {
		case "vl":
			a.vl.add(c, diff)
		case "vt":
			a.vt.add(c, diff)
		}
	}
	var out []KindCoverage
	for _, k := range inventoryKinds {
		a := byKind[k]
		if a.all.Items == 0 {
			continue
		}
		out = append(out, a.all)
		// A per-signal line only where that signal has items: pipes, filters
		// and stats exist on logs only, TraceQL on traces only.
		for _, s := range []KindCoverage{a.vl, a.vt} {
			if s.Items > 0 {
				out = append(out, s)
			}
		}
	}
	return out
}

// computeIngest is the row-based "ingest" kind: the ingest parity matrix has
// no inventory item of its own, each cell is a row (vl.ingest.* / vt.ingest.*).
func computeIngest(reg *registry.Registry) []KindCoverage {
	all := KindCoverage{Kind: "ingest", Signal: "all"}
	vl := KindCoverage{Kind: "ingest", Signal: "vl"}
	vt := KindCoverage{Kind: "ingest", Signal: "vt"}
	for i := range reg.Rows {
		r := &reg.Rows[i]
		if !IsIngestMatrixRow(r) {
			continue
		}
		c := classifyItem([]*registry.Row{r})
		diff := IsDifferentiallyLinked(r)
		all.add(c, diff)
		switch r.Surface {
		case registry.SurfaceVL:
			vl.add(c, diff)
		case registry.SurfaceVT:
			vt.add(c, diff)
		}
	}
	if all.Items == 0 {
		return nil
	}
	return []KindCoverage{all, vl, vt}
}

func signalOf(s registry.Surface) string { return string(s) }

func computePassRows(reg *registry.Registry) ([]PassRowSplit, []RowRef) {
	vl := PassRowSplit{Signal: "vl"}
	vt := PassRowSplit{Signal: "vt"}
	var none []RowRef
	for i := range reg.Rows {
		r := &reg.Rows[i]
		if (r.Surface != registry.SurfaceVL && r.Surface != registry.SurfaceVT) || IsPerfRow(r) || r.Expect != registry.ExpectPass {
			continue
		}
		s := &vl
		if r.Surface == registry.SurfaceVT {
			s = &vt
		}
		s.Rows++
		if rowTargetsHot(r) {
			s.TargetHot++
		}
		switch {
		case IsDifferentiallyLinked(r):
			s.Differential++
		case len(rowTests(r)) > 0:
			s.OtherTest++
		default:
			s.NoTest++
			none = append(none, RowRef{ID: r.ID, Signal: signalOf(r.Surface), Kind: string(r.Kind), TargetHot: rowTargetsHot(r), Pending: r.Pending})
		}
	}
	sort.Slice(none, func(i, j int) bool { return none[i].ID < none[j].ID })
	all := PassRowSplit{Signal: "all"}
	for _, s := range []PassRowSplit{vl, vt} {
		all.Rows += s.Rows
		all.TargetHot += s.TargetHot
		all.Differential += s.Differential
		all.OtherTest += s.OtherTest
		all.NoTest += s.NoTest
	}
	return []PassRowSplit{all, vl, vt}, none
}

func computeTotals(reg *registry.Registry) RegistryTotals {
	t := RegistryTotals{BySurface: map[string]int{}, ByExpect: map[string]int{}, ByOrigin: map[string]int{}}
	for i := range reg.Rows {
		r := &reg.Rows[i]
		t.Rows++
		t.BySurface[string(r.Surface)]++
		t.ByExpect[string(r.Expect)]++
		t.ByOrigin[string(r.Origin)]++
		if IsPerfRow(r) {
			t.PerfCells++
			if r.Pending {
				t.PendingPerf++
			}
		} else {
			t.NonPerf++
		}
		if r.Pending {
			t.Pending++
		}
	}
	return t
}

// pendingReason names why a pending row is not run by the registry runner,
// from the row's own fields; the first matching rule wins.
func pendingReason(r *registry.Row) string {
	hasLayer := func(name string) bool {
		for _, l := range r.Layers {
			if l == name {
				return true
			}
		}
		return false
	}
	switch {
	case IsPerfRow(r):
		return ReasonPerfCell
	case r.Kind == registry.KindDurability:
		return ReasonDurabilityCell
	case hasLayer("chaos") || hasLayer("ui"):
		return ReasonHarness
	case r.Kind == registry.KindFlag:
		return ReasonFlagRow
	case r.Request != nil && len(rowTests(r)) > 0:
		return ReasonRunnerLinked
	case r.Request != nil:
		return ReasonRunnerBare
	}
	return ReasonUnclassified
}

func computePending(reg *registry.Registry) []PendingReason {
	counts := map[string]int{}
	refs := map[string]map[string]bool{}
	for i := range reg.Rows {
		r := &reg.Rows[i]
		if !r.Pending {
			continue
		}
		reason := pendingReason(r)
		counts[reason]++
		if r.Refs != nil && r.Refs.Doc != "" {
			if refs[reason] == nil {
				refs[reason] = map[string]bool{}
			}
			refs[reason][r.Refs.Doc] = true
		}
	}
	var out []PendingReason
	for _, reason := range pendingReasonOrder {
		if counts[reason] == 0 {
			continue
		}
		pr := PendingReason{
			Reason:            reason,
			Rows:              counts[reason],
			ExecutedElsewhere: pendingReasonText[reason].elsewhere,
			Description:       pendingReasonText[reason].label,
		}
		for d := range refs[reason] {
			pr.Refs = append(pr.Refs, d)
		}
		sort.Strings(pr.Refs)
		out = append(out, pr)
	}
	return out
}

// featureProofClass is the single class of a shipped feature, by precedence:
// upstream-exact (a cited upstream row that expects pass, targets hot and links
// a differential test), lh-extension (any other cited row), perf-proven (no
// row, but a bench scenario or a perf cell), unproven (none of those; unit
// tests may still exist, the tests column shows them).
func featureProofClass(rows []*registry.Row, perfScenario bool) string {
	for _, r := range rows {
		upstream := r.Origin != registry.OriginLHAddition && !r.Upstream.IsZero()
		if upstream && r.Expect == registry.ExpectPass && !IsPerfRow(r) && IsDifferentiallyLinked(r) {
			return ClassUpstreamExact
		}
	}
	if len(rows) > 0 {
		return ClassLHExtension
	}
	if perfScenario {
		return ClassPerfProven
	}
	return ClassUnproven
}

func computeFeatures(fs *registry.FeatureSet, reg *registry.Registry) FeatureCoverage {
	fc := FeatureCoverage{ByStatus: map[string]int{}, ByClass: map[string]int{}}
	for i := range fs.Features {
		f := &fs.Features[i]
		fc.Total++
		fc.ByStatus[string(f.Status)]++
		if f.Status != registry.StatusShipped {
			continue
		}
		fc.Shipped++
		var rows []*registry.Row
		fp := FeatureProof{ID: f.ID, Area: string(f.Area), Tests: len(f.Tests), Bench: append([]string(nil), f.Bench...)}
		if fp.Bench == nil {
			fp.Bench = []string{}
		}
		sort.Strings(fp.Bench)
		for _, id := range f.Rows {
			r := reg.ByID[id]
			if r == nil {
				continue
			}
			rows = append(rows, r)
			fp.Rows++
			if r.Pending {
				fp.RowsPending++
			}
			if rowTargetsHot(r) {
				fp.HotRows++
			}
			if IsPerfRow(r) {
				fp.PerfCells++
			}
		}
		fp.PerfScenario = len(f.Bench) > 0 || fp.PerfCells > 0
		fp.ProofClass = featureProofClass(rows, fp.PerfScenario)
		fc.ByClass[fp.ProofClass]++
		if fp.Rows == 0 {
			fc.NoRows++
		}
		if fp.Tests == 0 {
			fc.NoTests++
		}
		if !fp.PerfScenario {
			fc.NoPerf++
		}
		if fp.HotRows > 0 {
			fc.HotRows++
		}
		fc.ShippedAll = append(fc.ShippedAll, fp)
	}
	// FeatureSet is already in report order (area, then id); keep it, but make
	// the order independent of how the set was built.
	sort.SliceStable(fc.ShippedAll, func(i, j int) bool {
		ai, aj := registry.AreaIndex(registry.Area(fc.ShippedAll[i].Area)), registry.AreaIndex(registry.Area(fc.ShippedAll[j].Area))
		if ai != aj {
			return ai < aj
		}
		return fc.ShippedAll[i].ID < fc.ShippedAll[j].ID
	})
	return fc
}

func computeVerdict(pc *ProofCoverage) Verdict {
	var v Verdict
	for _, k := range pc.Upstream {
		if k.Signal != "all" {
			continue
		}
		if k.Kind == "ingest" {
			v.IngestCells = k.Items
			v.IngestCellsVerified = k.Verified
			continue
		}
		v.UpstreamItems += k.Items
		v.UpstreamItemsWithRow += k.WithRow
		v.UpstreamItemsNoRow += k.NoRow
		v.UpstreamItemsVerified += k.Verified
		v.UpstreamItemsDeclaredOnly += k.Pending
		v.UpstreamItemsDifferentiallyLinked += k.DifferentiallyLinked
	}
	for _, s := range pc.PassRows {
		if s.Signal == "all" {
			v.UpstreamPassRows = s.Rows
			v.UpstreamPassRowsDifferential = s.Differential
			v.UpstreamPassRowsOtherTest = s.OtherTest
			v.UpstreamPassRowsNoTest = s.NoTest
		}
	}
	v.RowsTotal = pc.Registry.Rows
	v.RowsPending = pc.Registry.Pending
	for _, p := range pc.Pending {
		switch {
		case p.Reason == ReasonUnclassified:
			v.PendingUnclassified += p.Rows
		case p.ExecutedElsewhere:
			v.PendingElsewhere += p.Rows
		default:
			v.PendingNoRunner += p.Rows
		}
	}
	v.ShippedFeatures = pc.Features.Shipped
	v.ShippedFeaturesNoRows = pc.Features.NoRows
	v.ShippedFeaturesNoPerf = pc.Features.NoPerf
	v.ShippedFeaturesUnproven = pc.Features.ByClass[ClassUnproven]
	v.SharedCodeItems = pc.Shared.Items
	v.SharedCodeUnguarded = pc.Shared.Unguarded
	return v
}

// JSON renders the machine-readable report: indented, key-sorted for maps (the
// encoder sorts them), one trailing newline.
func (pc *ProofCoverage) JSON() ([]byte, error) {
	b, err := json.MarshalIndent(pc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// --- Markdown -------------------------------------------------------------

func signalLabel(s string) string {
	switch s {
	case "vl":
		return "logs (vl)"
	case "vt":
		return "traces (vt)"
	default:
		return "all"
	}
}

// Markdown renders docs/proof-coverage.md.
func (pc *ProofCoverage) Markdown() string {
	var b strings.Builder
	v := pc.Verdict
	b.WriteString("# Proof coverage (GENERATED by `make conformance-gen` — do not edit)\n\n")
	b.WriteString("What the conformance registry proves, how, and what is missing. Computed from `tests/conformance/registry/` (rows and features), the upstream inventory and the repository tree; never hand-written, byte-stable (sorted, no timestamps). The same numbers are in `docs/proof-coverage.json` for PR comments. Every claim below is a count over declared rows and links: it says what is *named*, not that a named test passes (CI runs the tests).\n\n")

	b.WriteString("## Verdict\n\n")
	b.WriteString("| Question | Answer |\n|---|--:|\n")
	vrow := func(q string, n any) { fmt.Fprintf(&b, "| %s | %v |\n", q, n) }
	vrow("Upstream items (routes, pipes, filters, stats, TraceQL, flags) in the inventory", v.UpstreamItems)
	vrow("... with at least one registry row", v.UpstreamItemsWithRow)
	vrow("... with no row", v.UpstreamItemsNoRow)
	vrow("... verified (a covering row is executed, nothing worse)", v.UpstreamItemsVerified)
	vrow("... declared only (every covering row pending)", v.UpstreamItemsDeclaredOnly)
	vrow("... with a covering row that links a differential test against hot VL/VT", v.UpstreamItemsDifferentiallyLinked)
	vrow(fmt.Sprintf("Ingest protocol cells (%d verified)", v.IngestCellsVerified), v.IngestCells)
	vrow("Upstream pass rows (vl/vt, not perf cells)", v.UpstreamPassRows)
	vrow("... linking a differential test (hot target, tests/parity or tests/e2e)", v.UpstreamPassRowsDifferential)
	vrow("... linking only another test", v.UpstreamPassRowsOtherTest)
	vrow("... linking no test at all", v.UpstreamPassRowsNoTest)
	vrow("Registry rows / pending rows", fmt.Sprintf("%d / %d", v.RowsTotal, v.RowsPending))
	vrow("... pending and waiting for the HTTP runner or a harness", v.PendingNoRunner)
	vrow("... pending but executed by another CI job or by cited Go tests", v.PendingElsewhere)
	vrow("... pending, reason unclassified", v.PendingUnclassified)
	vrow("Shipped features", v.ShippedFeatures)
	vrow("... citing no registry row", v.ShippedFeaturesNoRows)
	vrow("... citing no perf scenario (bench id or perf cell)", v.ShippedFeaturesNoPerf)
	vrow("... unproven (no row and no perf scenario)", v.ShippedFeaturesUnproven)
	vrow("Shared upstream code items (pins, patches, imported packages)", v.SharedCodeItems)
	vrow("... unguarded (no test or gate names them)", v.SharedCodeUnguarded)
	b.WriteString("\n")

	pc.writeKinds(&b)
	pc.writePassRows(&b)
	pc.writeFeatures(&b)
	pc.writeShared(&b)
	pc.writePending(&b)
	pc.writeTotals(&b)
	pc.writeDerivation(&b)
	return b.String()
}

func (pc *ProofCoverage) writeKinds(b *strings.Builder) {
	b.WriteString("## 1. Upstream surfaces by kind\n\n")
	b.WriteString("Items are counted per surface (a route or flag that VictoriaLogs and VictoriaTraces each register appears once for `vl` and once for `vt`). Status columns follow `UPSTREAM_COVERAGE.md`: the worst covering row decides, and an item whose rows are all pending is *pending*. *Diff-linked* counts the items with a covering row that targets hot and links a test under `tests/parity/` or `tests/e2e/`; it overlaps the status columns. `ingest` is row-based (the ingest parity matrix has no inventory item).\n\n")
	b.WriteString("| Kind | Signal | Items | With a row | Verified | Pending | Differs | Absent/unsupported | No row | Diff-linked |\n|---|---|--:|--:|--:|--:|--:|--:|--:|--:|\n")
	for _, k := range pc.Upstream {
		fmt.Fprintf(b, "| %s | %s | %d | %d | %d | %d | %d | %d | %d | %d |\n", k.Kind, signalLabel(k.Signal), k.Items, k.WithRow, k.Verified, k.Pending, k.Differs, k.AbsentUnsupported, k.NoRow, k.DifferentiallyLinked)
	}
	b.WriteString("\n")
}

func (pc *ProofCoverage) writePassRows(b *strings.Builder) {
	b.WriteString("## 2. Upstream pass rows by test link\n\n")
	b.WriteString("Rows on the `vl` and `vt` surfaces that expect `pass`, excluding perf cells, split by the tests they link: *differential* (targets hot and links a test under `tests/parity/` or `tests/e2e/`), *other test* (links a test that is not differential), *no test*.\n\n")
	b.WriteString("| Signal | Rows | Target hot | Differential | Other test | No test |\n|---|--:|--:|--:|--:|--:|\n")
	for _, s := range pc.PassRows {
		fmt.Fprintf(b, "| %s | %d | %d | %d | %d | %d |\n", signalLabel(s.Signal), s.Rows, s.TargetHot, s.Differential, s.OtherTest, s.NoTest)
	}
	b.WriteString("\n")
	fmt.Fprintf(b, "<details><summary>%d upstream pass rows link no test</summary>\n\n", len(pc.PassRowsNone))
	if len(pc.PassRowsNone) > 0 {
		b.WriteString("| Row | Signal | Kind | Targets hot | Pending |\n|---|---|---|---|---|\n")
		for _, r := range pc.PassRowsNone {
			fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s |\n", r.ID, r.Signal, r.Kind, yesNo(r.TargetHot), yesNo(r.Pending))
		}
		b.WriteString("\n")
	}
	b.WriteString("</details>\n\n")
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

func (pc *ProofCoverage) writeFeatures(b *strings.Builder) {
	f := pc.Features
	b.WriteString("## 3. Shipped features\n\n")
	fmt.Fprintf(b, "%d features: %s. Of the %d shipped:\n\n", f.Total, countList(f.ByStatus), f.Shipped)
	fmt.Fprintf(b, "- no registry row cited: %d\n- no perf scenario cited (a bench id or a perf cell): %d\n- no test cited: %d\n- a cited row targets hot: %d\n\n", f.NoRows, f.NoPerf, f.NoTests, f.HotRows)
	b.WriteString("Proof class, by precedence: **upstream-exact** (a cited upstream row expects pass, targets hot and links a differential test), **lh-extension** (any other cited row), **perf-proven** (no row, but a bench scenario or perf cell), **unproven** (none of these; cited unit tests are in the tests column and do not change the class).\n\n")
	b.WriteString("| Proof class | Shipped features |\n|---|--:|\n")
	for _, c := range []string{ClassUpstreamExact, ClassLHExtension, ClassPerfProven, ClassUnproven} {
		fmt.Fprintf(b, "| %s | %d |\n", c, f.ByClass[c])
	}
	b.WriteString("\n")
	b.WriteString("| Feature | Area | Rows (pending) | Hot rows | Tests | Perf scenario | Proof class |\n|---|---|--:|--:|--:|---|---|\n")
	for _, fp := range f.ShippedAll {
		perf := "none"
		switch {
		case len(fp.Bench) > 0 && fp.PerfCells > 0:
			perf = strings.Join(fp.Bench, ", ") + " + " + fmt.Sprintf("%d perf cells", fp.PerfCells)
		case len(fp.Bench) > 0:
			perf = strings.Join(fp.Bench, ", ")
		case fp.PerfCells > 0:
			perf = fmt.Sprintf("%d perf cells", fp.PerfCells)
		}
		fmt.Fprintf(b, "| `%s` | %s | %d (%d) | %d | %d | %s | %s |\n", fp.ID, fp.Area, fp.Rows, fp.RowsPending, fp.HotRows, fp.Tests, perf, fp.ProofClass)
	}
	b.WriteString("\n")
}

// countList renders {"shipped": 3, "planned": 1} as "3 shipped, 1 planned" in
// the fixed order of the feature statuses, then any other key sorted.
func countList(m map[string]int) string {
	var parts []string
	seen := map[string]bool{}
	for _, k := range []string{string(registry.StatusShipped), string(registry.StatusInProgress), string(registry.StatusPlanned)} {
		if m[k] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", m[k], k))
		}
		seen[k] = true
	}
	var rest []string
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	for _, k := range rest {
		parts = append(parts, fmt.Sprintf("%d %s", m[k], k))
	}
	return strings.Join(parts, ", ")
}

func (pc *ProofCoverage) writePending(b *strings.Builder) {
	b.WriteString("## 5. Pending rows by reason\n\n")
	b.WriteString("A row is *pending* while the registry HTTP runner does not execute it (`tests/conformance/README.md`). The reason is derived from the row's own fields, first match wins; *executed elsewhere* means another CI job or the cited Go tests run the cell, so pending does not mean unchecked.\n\n")
	b.WriteString("| Reason | Rows | Executed elsewhere | Meaning |\n|---|--:|---|---|\n")
	for _, p := range pc.Pending {
		fmt.Fprintf(b, "| `%s` | %d | %s | %s |\n", p.Reason, p.Rows, yesNo(p.ExecutedElsewhere), p.Description)
	}
	b.WriteString("\n")
	for _, p := range pc.Pending {
		if len(p.Refs) > 0 && p.ExecutedElsewhere {
			fmt.Fprintf(b, "`%s` rows cite: %s.\n\n", p.Reason, "`"+strings.Join(p.Refs, "`, `")+"`")
		}
	}
}

func (pc *ProofCoverage) writeTotals(b *strings.Builder) {
	t := pc.Registry
	b.WriteString("## 6. Registry totals and what is not inventoried\n\n")
	fmt.Fprintf(b, "%d rows: %s; expect %s; origin %s. %d perf cells (%d pending), %d other rows.\n\n", t.Rows, mapList(t.BySurface), mapList(t.ByExpect), mapList(t.ByOrigin), t.PerfCells, t.PendingPerf, t.NonPerf)
	b.WriteString("Surfaces with no inventory, so no completeness number can be computed for them:\n\n")
	for _, n := range pc.NotInventory {
		fmt.Fprintf(b, "- %s\n", n)
	}
	b.WriteString("\n")
}

func mapList(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func (pc *ProofCoverage) writeDerivation(b *strings.Builder) {
	b.WriteString("## How this is derived\n\n")
	b.WriteString("- Kinds, statuses: `tests/conformance/inventory.generated.yaml` joined with the rows exactly as `UPSTREAM_COVERAGE.md` joins them.\n")
	b.WriteString("- Test links: each row's `refs.tests`; *differential* means the row targets `hot` and a linked path starts with `tests/parity/` or `tests/e2e/`. A named test is a link, not proof the test checks that exact request.\n")
	b.WriteString("- Features: `tests/conformance/registry/features/`; perf scenario means a `bench:` id or a cited perf-layer row.\n")
	b.WriteString("- Shared code: `go.mod` and `lakehouse-traces/go.mod` (upstream modules), `Makefile` pins, the files under `patches/`, and the import statements of every non-vendored Go file. A *guard* is a Go test file, or a gate script under `scripts/ci/`, that names the item (the patch path or file name, the pin variable, the quoted import path). An item named by more than three files shows *many* instead of a list or a count, so the report does not change each time a test is added. That a guard exists is derived; that it is sufficient is not.\n")
	b.WriteString("- Pending reasons: the row's kind, layers, request and test links (section 5).\n")
}
