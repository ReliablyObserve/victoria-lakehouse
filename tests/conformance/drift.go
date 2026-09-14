// Package conformance joins the upstream inventory with the registry (this
// file) and the feature catalog with both of them plus the changelog
// (features.go) — the two halves of the drift gate.
package conformance

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/report"
)

type DriftReport struct {
	Unmapped         []inventory.Item // hard: upstream item with no row (routes, pipes, filters, stats, traceql)
	Stale            []string         // hard: row ids citing an upstream item the inventory lacks (except expect=absent)
	FlagWarnings     []inventory.Item // soft: upstream flags without a row
	PendingBump      []string         // informational: rows whose Since version is newer than inventory
	AbsentButPresent []string         // soft: row ids with expect=absent whose upstream key IS in inventory
	MissingVersion   []string         // hard: surface names where inventory version is empty but rows cite since
}

// CheckDrift compares the upstream inventory against the registry to identify
// unmapped features, stale rows, pending upstream bumps, and absent-but-present rows.
func CheckDrift(inv *inventory.Inventory, reg *registry.Registry) DriftReport {
	var rep DriftReport

	// Build covered upstream keys from registry rows.
	covered := buildCovered(reg, inv)

	// Track which inventory items are present.
	present := map[string]bool{}
	for _, it := range inv.Items {
		k := inv.Key(it)
		present[k] = true

		// Check if this item is covered by a registry row.
		if covered[k] {
			continue
		}

		// Uncovered items are either unmapped features or flag warnings.
		if it.Kind == "flag" {
			rep.FlagWarnings = append(rep.FlagWarnings, it)
		} else {
			rep.Unmapped = append(rep.Unmapped, it)
		}
	}

	// Track missing versions (hard failure when a row cites since for empty-version surface).
	missingVersions := make(map[string]bool)

	// Check for stale rows and absent-but-present rows. Presence is checked
	// per surface (report.PresentOnAnySurface): a vl or vt row only against
	// its own surface, an lh row against either, since an lh row's Upstream
	// reference does not itself say which upstream binary it belongs to.
	for _, r := range reg.Rows {
		if r.Upstream == nil || r.Upstream.IsZero() {
			continue
		}

		presentOnAnySurface := report.PresentOnAnySurface(present, &r)

		if r.Expect == registry.ExpectAbsent {
			// Track rows marked absent but whose upstream is in the inventory.
			if presentOnAnySurface {
				rep.AbsentButPresent = append(rep.AbsentButPresent, r.ID)
			}
			continue
		}

		// Check for missing versions: row cites since for a surface with empty inventory version.
		if hasMissingVersion(&r, inv) {
			surface := getSurfaceForSince(&r, inv)
			if surface != "" {
				missingVersions[surface] = true
			}
			continue
		}

		if !presentOnAnySurface {
			// Check if this is a pending-bump row.
			if isPendingBump(&r, inv) {
				rep.PendingBump = append(rep.PendingBump, r.ID)
			} else {
				rep.Stale = append(rep.Stale, r.ID)
			}
		}
	}

	// Convert missing versions to sorted list.
	for surface := range missingVersions {
		rep.MissingVersion = append(rep.MissingVersion, surface)
	}

	// Sort output for determinism.
	sort.Slice(rep.Unmapped, func(i, j int) bool { return rep.Unmapped[i].Name < rep.Unmapped[j].Name })
	sort.Slice(rep.FlagWarnings, func(i, j int) bool { return rep.FlagWarnings[i].Name < rep.FlagWarnings[j].Name })
	sort.Strings(rep.Stale)
	sort.Strings(rep.PendingBump)
	sort.Strings(rep.AbsentButPresent)
	sort.Strings(rep.MissingVersion)

	return rep
}

// buildCovered builds a map of covered upstream keys from the registry, via
// the same report.CoveredKeys the generated coverage doc uses (exact
// UpstreamKeys matches plus route-prefix expansion), so the drift check and
// the coverage doc can never disagree about what counts as covered.
func buildCovered(reg *registry.Registry, inv *inventory.Inventory) map[string]bool {
	covered := map[string]bool{}
	for key, ids := range report.CoveredKeys(inv, reg) {
		if len(ids) > 0 {
			covered[key] = true
		}
	}
	return covered
}

// inventoryVersion returns the inventory's version string for a "vl"/"vt"
// since-surface name (empty for anything else).
func inventoryVersion(surface string, inv *inventory.Inventory) string {
	switch surface {
	case "vl":
		return inv.VLVersion
	case "vt":
		return inv.VTVersion
	default:
		return ""
	}
}

// sinceSurfaces returns the since-surface names ("vl", "vt") relevant to a
// row: its own surface for a vl or vt row, or both for an lh row — the
// same rule report.RowSurfaces uses for Upstream-key presence, applied here
// to the row's `since` map instead. A row's `since` naming a surface other
// than its own (or, for an lh row, neither vl nor vt) is never consulted.
func sinceSurfaces(r *registry.Row) []string { return report.RowSurfaces(r) }

// hasMissingVersion checks if a row cites a since for one of its own
// surfaces (sinceSurfaces) whose inventory version is empty.
func hasMissingVersion(r *registry.Row, inv *inventory.Inventory) bool {
	return getSurfaceForSince(r, inv) != ""
}

// getSurfaceForSince returns a since-surface of the row (sinceSurfaces)
// that the row cites and whose inventory version is empty, or "" if none.
func getSurfaceForSince(r *registry.Row, inv *inventory.Inventory) string {
	for _, surface := range sinceSurfaces(r) {
		if _, hasSince := r.Since[surface]; !hasSince {
			continue
		}
		if inventoryVersion(surface, inv) == "" {
			return surface
		}
	}
	return ""
}

// isPendingBump checks if a row's Since version, for one of its own
// surfaces (sinceSurfaces), is newer than the inventory's version for that
// surface. Returns false if the inventory version is empty for every
// surface the row cites (that case is a MissingVersion hard failure
// instead, handled separately in CheckDrift).
func isPendingBump(r *registry.Row, inv *inventory.Inventory) bool {
	if len(r.Since) == 0 {
		return false
	}

	for _, surface := range sinceSurfaces(r) {
		sinceVersion, hasSince := r.Since[surface]
		if !hasSince {
			continue
		}

		invVersion := inventoryVersion(surface, inv)
		// Empty inventory version means we can't determine if pending; must be treated as missing.
		if invVersion == "" {
			continue
		}

		// Parse versions: strip leading "v" and compare as dotted integers.
		invVer := strings.TrimPrefix(invVersion, "v")
		sinceVer := strings.TrimPrefix(sinceVersion, "v")

		if compareVersions(sinceVer, invVer) > 0 {
			return true
		}
	}

	return false
}

// compareVersions compares two dotted-integer version strings.
// Pre-release suffixes (e.g., "-rc1") are ignored; only the dotted integers are compared.
// Returns: < 0 if a < b, 0 if a == b, > 0 if a > b.
func compareVersions(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	maxLen := len(aParts)
	if len(bParts) > maxLen {
		maxLen = len(bParts)
	}

	for i := 0; i < maxLen; i++ {
		aVal := 0
		if i < len(aParts) {
			fmt.Sscanf(aParts[i], "%d", &aVal)
		}

		bVal := 0
		if i < len(bParts) {
			fmt.Sscanf(bParts[i], "%d", &bVal)
		}

		if aVal < bVal {
			return -1
		}
		if aVal > bVal {
			return 1
		}
	}

	return 0
}

// HardFailures returns formatted error messages for unmapped items, stale rows, and missing versions.
// The messages include a suggested row stub for unmapped items.
func (d DriftReport) HardFailures() []string {
	var out []string

	for _, surface := range d.MissingVersion {
		out = append(out, fmt.Sprintf("inventory has no %s version — regenerate with make conformance-gen", surface))
	}

	for _, it := range d.Unmapped {
		kind := it.Kind
		out = append(out, fmt.Sprintf("upstream %s %q (%s) has no registry row — add to tests/conformance/registry/rows/ e.g.\n"+
			"  - id: <surface>.%s.<name>.basic\n    origin: native\n    expect: pass\n    upstream: { %s: %s }\n    pending: true",
			kind, it.Name, it.Source, kind, kind, it.Name))
	}

	for _, id := range d.Stale {
		out = append(out, fmt.Sprintf("row %s cites an upstream item the inventory no longer has — delete the row or mark it expect: absent", id))
	}

	return out
}

// Summary returns a one-line summary of the drift report for CI logs.
func (d DriftReport) Summary() string {
	parts := []string{
		fmt.Sprintf("%d unmapped", len(d.Unmapped)),
		fmt.Sprintf("%d stale", len(d.Stale)),
		fmt.Sprintf("%d pending-bump", len(d.PendingBump)),
	}

	flagCount := len(d.FlagWarnings)
	flagWord := "warning"
	if flagCount != 1 {
		flagWord = "warnings"
	}
	parts = append(parts, fmt.Sprintf("%d flag %s", flagCount, flagWord))

	absentCount := len(d.AbsentButPresent)
	if absentCount > 0 {
		parts = append(parts, fmt.Sprintf("%d absent-but-present", absentCount))
	}

	missingCount := len(d.MissingVersion)
	if missingCount > 0 {
		parts = append(parts, fmt.Sprintf("%d missing-version", missingCount))
	}

	return strings.Join(parts, ", ")
}
