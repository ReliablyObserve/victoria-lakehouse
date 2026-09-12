// Package report renders the generated conformance documents.
package report

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

// RowSurfaces returns the inventory surfaces ("vl", "vt") a row's Upstream
// reference applies to. A vl or vt row only ever documents its own binary's
// surface. An lh row (shim or addition) documents Lakehouse-side behavior
// for an upstream route/pipe/etc that can belong to either upstream binary
// (or, for a shared route like /select/logsql/query, both) — since an lh
// row's Upstream field does not itself say which, it is treated as
// covering the item on both surfaces rather than guessing.
func RowSurfaces(r *registry.Row) []string {
	switch r.Surface {
	case registry.SurfaceVL:
		return []string{"vl"}
	case registry.SurfaceVT:
		return []string{"vt"}
	case registry.SurfaceLH:
		return []string{"vl", "vt"}
	default:
		return nil
	}
}

// PresentOnAnySurface reports whether a row's upstream reference is present
// in the inventory (present maps surface-scoped keys, see Inventory.Key) on
// any of the surfaces the row applies to (RowSurfaces). For a vl or vt row
// that is just presence on its own surface; for an lh row, covering both
// surfaces, it is presence on either — an lh row is only "not present" (and
// so a drift candidate: stale, pending-bump, or a spurious absent
// declaration) when the referenced item exists on neither upstream binary.
func PresentOnAnySurface(present map[string]bool, r *registry.Row) bool {
	if r.Upstream == nil || r.Upstream.IsZero() {
		return false
	}
	uk := r.Upstream.Key()
	for _, surface := range RowSurfaces(r) {
		if present[surface+":"+uk] {
			return true
		}
	}
	return false
}

// CoveredKeys maps every inventory key ("<surface>:<kind>:<name>", see
// Inventory.Key) covered by at least one registry row to the ids of the
// covering rows, in registry report order (native, then lh-shim, then
// lh-addition, then by id — see registry.LoadDir). Two sources of coverage:
//
//   - Exact matches: a row's Upstream.Key() ("<kind>:<name>") joined with
//     the surface(s) the row applies to (RowSurfaces).
//   - Route-prefix expansion: an inventory route item whose Name ends in
//     "/" is covered by any row, on the same surface, whose upstream route
//     lies under that prefix (e.g. the inventory item "vl:route:/insert/
//     loki/" is covered by a vl row citing "/insert/loki/api/v1/push").
//
// This is the single source of truth for "is this upstream item covered":
// both the drift check (tests/conformance/drift.go) and the generated
// coverage doc (RenderCoverage, below) call this so they can never
// disagree about which items are covered.
func CoveredKeys(inv *inventory.Inventory, reg *registry.Registry) map[string][]string {
	covered := map[string][]string{}

	prefixRoutes := map[string][]string{} // surface -> prefix route names
	for _, it := range inv.Items {
		if it.Kind == "route" && strings.HasSuffix(it.Name, "/") {
			prefixRoutes[it.Surface] = append(prefixRoutes[it.Surface], it.Name)
		}
	}

	for i := range reg.Rows {
		r := &reg.Rows[i]
		if r.Upstream == nil || r.Upstream.IsZero() {
			continue
		}
		uk := r.Upstream.Key() // "<kind>:<name>"
		for _, surface := range RowSurfaces(r) {
			k := surface + ":" + uk
			covered[k] = append(covered[k], r.ID)

			if route, ok := strings.CutPrefix(uk, "route:"); ok {
				for _, prefix := range prefixRoutes[surface] {
					// Skip the self-match: when the row's own route IS the
					// prefix route, it was already added above under the
					// same key, and adding it again here would duplicate
					// the id.
					if prefix != route && strings.HasPrefix(route, prefix) {
						pk := surface + ":route:" + prefix
						covered[pk] = append(covered[pk], r.ID)
					}
				}
			}
		}
	}

	return covered
}

// expectRank orders Expect values from best (0) to worst (3), for
// aggregating the status of multiple rows that cover the same upstream
// item: the aggregate status is the worst of them, since a single
// unsupported or differing sibling row means the item is not cleanly
// covered even if another row for the same item passes.
func expectRank(e registry.Expect) int {
	switch e {
	case registry.ExpectDiffer:
		return 1
	case registry.ExpectUnsupported:
		return 2
	case registry.ExpectAbsent:
		return 3
	default: // ExpectPass
		return 0
	}
}

// expectIcon renders the icon for a row's Expect value alone (no Pending,
// no Origin — see icon() for the single-row, LH-addition-aware version).
func expectIcon(r *registry.Row) string {
	switch r.Expect {
	case registry.ExpectAbsent:
		return "⛔ absent upstream and on LH"
	case registry.ExpectUnsupported:
		return "⛔ unsupported on cold (documented)"
	case registry.ExpectDiffer:
		return "🔁 differs: " + r.DifferNote
	default:
		return "✅ native, verified"
	}
}

// aggregateStatus renders the coverage status for one upstream item from
// every registry row that covers it: the worst Expect among them (differ <
// unsupported < absent, all worse than pass), so a passing row can never
// hide an unsupported or differing sibling. If every covering row is still
// Pending, the status becomes an execution-state note rather than a
// correctness verdict: when the worst expectation is pass, that note is
// exactly "🟡 declared, not yet executed" (a pending pass has nothing to
// report yet, so there is no verified/differ/unsupported/absent claim to
// make); for a worse worst expectation, the note is appended to that
// expectation's icon/label instead, since a documented differ/unsupported/
// absent declaration is still informative even before it runs.
func aggregateStatus(rows []*registry.Row) string {
	if len(rows) == 0 {
		return "⚪ no row"
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
	if allPending && worst.Expect == registry.ExpectPass {
		return "🟡 declared, not yet executed"
	}
	status := expectIcon(worst)
	if allPending {
		status += " (declared, not yet executed)"
	}
	return status
}

// icon renders the status of a single row shown on its own (the Lakehouse
// additions table, one row per line — there is no other row covering the
// same upstream key to aggregate against, since lh-addition rows have no
// upstream key at all). Unlike aggregateStatus, Pending here fully replaces
// the status (🟡) rather than annotating it: with only one row in view,
// "not yet executed" is a more useful headline than the expect it declares.
func icon(r *registry.Row) string {
	switch {
	case r == nil:
		return "⚪ no row"
	case r.Expect == registry.ExpectAbsent:
		return "⛔ absent upstream and on LH"
	case r.Expect == registry.ExpectUnsupported:
		return "⛔ unsupported on cold (documented)"
	case r.Expect == registry.ExpectDiffer:
		return "🔁 differs: " + r.DifferNote
	case r.Pending:
		return "🟡 declared, not yet executed"
	case r.Origin == registry.OriginLHAddition:
		return "🧩 LH addition"
	default:
		return "✅ native, verified"
	}
}

// RenderCoverage produces UPSTREAM_COVERAGE.md: every upstream item with its
// covering row(s) and aggregate status, then every LH addition.
func RenderCoverage(inv *inventory.Inventory, reg *registry.Registry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Upstream API coverage (GENERATED by `make conformance-gen` — do not edit)\n\n")
	if inv.VLCommitTraces != "" {
		fmt.Fprintf(&b, "Upstream: VictoriaLogs %s (traces module vendors its own VictoriaLogs copy pinned at commit %s) · VictoriaTraces %s. Source of truth: `tests/conformance/registry/rows/` + `tests/conformance/inventory.generated.yaml`.\n\n", inv.VLVersion, inv.VLCommitTraces, inv.VTVersion)
	} else {
		fmt.Fprintf(&b, "Upstream: VictoriaLogs %s · VictoriaTraces %s. Source of truth: `tests/conformance/registry/rows/` + `tests/conformance/inventory.generated.yaml`.\n\n", inv.VLVersion, inv.VTVersion)
	}
	fmt.Fprintf(&b, "Lakehouse mounts the upstream VictoriaLogs/VictoriaTraces handlers and engines; it never re-implements an API that exists upstream. Extensions go through `patches/` and are guarded by `internal/upstreamreuse` — see `patches/README.md`.\n\n")
	fmt.Fprintf(&b, "Items below are counted PER SURFACE: a route, flag, pipe, filter, stats function or TraceQL function that VictoriaLogs and VictoriaTraces each register independently appears once for `vl` and once for `vt` — the totals are not counts of unique names.\n\n")
	fmt.Fprintf(&b, "Legend: ✅ verified · 🟡 declared, not yet executed · 🔁 differs from upstream (documented) · ⛔ absent or unsupported (documented) · ⚪ no registry row · 🧩 Lakehouse addition (no upstream equivalent)\n\n")

	keys := CoveredKeys(inv, reg)
	byKind := map[string][]inventory.Item{}
	for _, it := range inv.Items {
		byKind[it.Kind] = append(byKind[it.Kind], it)
	}
	for _, kind := range []string{"route", "pipe", "filter", "stats", "traceql", "flag"} {
		items := byKind[kind]
		if len(items) == 0 {
			continue
		}
		// Sort by Name, then Surface as a tie-break: the same name can be
		// registered independently by both vl and vt (e.g. route
		// /insert/native, or a shared flag name), and without the
		// tie-break sort.Slice's order between two equal-Name items is
		// unspecified — not just cosmetically nondeterministic, but able
		// to change confgen's byte-stable output run to run.
		sort.Slice(items, func(i, j int) bool {
			if items[i].Name != items[j].Name {
				return items[i].Name < items[j].Name
			}
			return items[i].Surface < items[j].Surface
		})
		covered := 0
		for _, it := range items {
			if len(keys[inv.Key(it)]) > 0 {
				covered++
			}
		}
		fmt.Fprintf(&b, "## Upstream %s (%d)\n\n%s: %d/%d covered by at least one registry row.\n\n| %s | Source | Rows | Status |\n|---|---|---|---|\n", kind, len(items), kind, covered, len(items), kind)
		for _, it := range items {
			ids := keys[inv.Key(it)]
			var rows []*registry.Row
			for _, id := range ids {
				if r := reg.ByID[id]; r != nil {
					rows = append(rows, r)
				}
			}
			status := aggregateStatus(rows)
			if kind == "flag" && !it.Linked {
				if len(rows) == 0 {
					status = "⚪ not linked into LH, no row"
				} else {
					status += " (package not linked into LH)"
				}
			}
			fmt.Fprintf(&b, "| `%s` | `%s` | %s | %s |\n", it.Name, it.Source, strings.Join(ids, ", "), status)
		}
		b.WriteString("\n")
	}

	fmt.Fprintf(&b, "## Lakehouse additions (no upstream equivalent)\n\n| Row | Title | Kind | Status |\n|---|---|---|---|\n")
	for _, r := range reg.Rows {
		if r.Origin == registry.OriginLHAddition {
			fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", r.ID, r.Title, r.Kind, icon(&r))
		}
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "## Rows gated on a later upstream version / absent by design\n\n| Row | Since | Expect | Note |\n|---|---|---|---|\n")
	present := map[string]bool{}
	for _, it := range inv.Items {
		present[inv.Key(it)] = true
	}
	for _, r := range reg.Rows {
		if r.Upstream == nil || r.Upstream.IsZero() {
			continue
		}
		// A row is already shown in the main coverage tables (and so
		// skipped here) if its upstream key is present on any surface it
		// applies to — an lh row covering both surfaces only belongs in
		// this gated section if it is present on neither.
		if PresentOnAnySurface(present, &r) {
			continue
		}
		since := "—"
		if len(r.Since) > 0 {
			parts := make([]string, 0, len(r.Since))
			for _, s := range []string{"vl", "vt"} {
				if v, ok := r.Since[s]; ok {
					parts = append(parts, s+" "+v)
				}
			}
			since = strings.Join(parts, ", ")
		}
		note := r.DifferNote
		if note == "" {
			note = r.Notes
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", r.ID, since, r.Expect, note)
	}

	return b.String()
}
