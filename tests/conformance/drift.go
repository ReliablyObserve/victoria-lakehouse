// Package conformance joins the upstream inventory with the registry.
package conformance

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

type DriftReport struct {
	Unmapped     []inventory.Item // hard: upstream item with no row (routes, pipes, filters, stats, traceql)
	Stale        []string         // hard: row ids citing an upstream item the inventory lacks (except expect=absent)
	FlagWarnings []inventory.Item // soft: upstream flags without a row
	PendingBump  []string         // informational: rows whose Since version is newer than inventory
}

// CheckDrift compares the upstream inventory against the registry to identify
// unmapped features, stale rows, and pending upstream bumps.
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
		if isCovered(it, covered) {
			continue
		}

		// Uncovered items are either unmapped features or flag warnings.
		if it.Kind == "flag" {
			rep.FlagWarnings = append(rep.FlagWarnings, it)
		} else {
			rep.Unmapped = append(rep.Unmapped, it)
		}
	}

	// Check for stale rows.
	for _, r := range reg.Rows {
		if r.Upstream == nil || r.Upstream.IsZero() || r.Expect == registry.ExpectAbsent {
			continue
		}

		upstreamKey := r.Upstream.Key()
		if !present[upstreamKey] {
			// Check if this is a pending-bump row.
			if isPendingBump(&r, inv) {
				rep.PendingBump = append(rep.PendingBump, r.ID)
			} else {
				rep.Stale = append(rep.Stale, r.ID)
			}
		}
	}

	// Sort output for determinism.
	sort.Slice(rep.Unmapped, func(i, j int) bool { return rep.Unmapped[i].Name < rep.Unmapped[j].Name })
	sort.Strings(rep.Stale)
	sort.Strings(rep.PendingBump)

	return rep
}

// buildCovered builds a map of covered upstream keys by collecting all upstream
// keys from registry rows. For route items, also tracks which routes are covered
// via prefix matching.
func buildCovered(reg *registry.Registry, inv *inventory.Inventory) map[string]bool {
	covered := map[string]bool{}

	for _, r := range reg.Rows {
		if r.Upstream == nil || r.Upstream.IsZero() {
			continue
		}
		upstreamKey := r.Upstream.Key()
		covered[upstreamKey] = true

		// For routes, also mark any inventory routes with this route as a prefix as covered.
		if r.Upstream.Route != "" {
			for _, it := range inv.Items {
				if it.Kind == "route" && strings.HasSuffix(it.Name, "/") {
					if strings.HasPrefix(r.Upstream.Route, it.Name) {
						k := inv.Key(it)
						covered[k] = true
					}
				}
			}
		}
	}

	return covered
}

// isCovered checks if an inventory item is covered by the registry.
func isCovered(it inventory.Item, covered map[string]bool) bool {
	invKey := ""
	if it.Kind == "route" {
		invKey = "route:" + it.Name
	} else if it.Kind == "pipe" {
		invKey = "pipe:" + it.Name
	} else if it.Kind == "filter" {
		invKey = "filter:" + it.Name
	} else if it.Kind == "stats" {
		invKey = "stats:" + it.Name
	} else if it.Kind == "traceql" {
		invKey = "traceql:" + it.Name
	} else if it.Kind == "flag" {
		invKey = "flag:" + it.Name
	}
	return covered[invKey]
}

// isPendingBump checks if a row's Since version is newer than the inventory's
// version for that surface.
func isPendingBump(r *registry.Row, inv *inventory.Inventory) bool {
	if len(r.Since) == 0 {
		return false
	}

	for surface, sinceVersion := range r.Since {
		var invVersion string
		switch surface {
		case "vl":
			invVersion = inv.VLVersion
		case "vt":
			invVersion = inv.VTVersion
		default:
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

// HardFailures returns formatted error messages for unmapped items and stale rows.
// The messages include a suggested row stub for unmapped items.
func (d DriftReport) HardFailures() []string {
	var out []string

	for _, it := range d.Unmapped {
		kind := it.Kind
		fieldName := kind
		if kind == "pipe" || kind == "filter" || kind == "stats" || kind == "traceql" {
			fieldName = kind
		}

		out = append(out, fmt.Sprintf("upstream %s %q (%s) has no registry row — add to tests/conformance/registry/rows/ e.g.\n"+
			"  - id: <surface>.%s.<name>.basic\n    origin: native\n    expect: pass\n    upstream: { %s: %s }\n    pending: true",
			kind, it.Name, it.Source, kind, fieldName, it.Name))
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

	return strings.Join(parts, ", ")
}
