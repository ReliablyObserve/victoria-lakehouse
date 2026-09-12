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
	Unmapped         []inventory.Item // hard: upstream item with no row (routes, pipes, filters, stats, traceql)
	Stale            []string         // hard: row ids citing an upstream item the inventory lacks (except expect=absent)
	FlagWarnings     []inventory.Item // soft: upstream flags without a row
	PendingBump      []string         // informational: rows whose Since version is newer than inventory
	AbsentButPresent []string         // soft: row ids with expect=absent whose upstream key IS in inventory
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

	// Check for stale rows and absent-but-present rows.
	for _, r := range reg.Rows {
		if r.Upstream == nil || r.Upstream.IsZero() {
			continue
		}

		upstreamKey := r.Upstream.Key()

		if r.Expect == registry.ExpectAbsent {
			// Track rows marked absent but whose upstream is in the inventory.
			if present[upstreamKey] {
				rep.AbsentButPresent = append(rep.AbsentButPresent, r.ID)
			}
			continue
		}

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
	sort.Slice(rep.FlagWarnings, func(i, j int) bool { return rep.FlagWarnings[i].Name < rep.FlagWarnings[j].Name })
	sort.Strings(rep.Stale)
	sort.Strings(rep.PendingBump)
	sort.Strings(rep.AbsentButPresent)

	return rep
}

// buildCovered builds a map of covered upstream keys from the registry.
// It includes exact matches from UpstreamKeys and adds route-prefix coverage:
// inventory routes with trailing "/" are covered by any registry route with that prefix.
func buildCovered(reg *registry.Registry, inv *inventory.Inventory) map[string]bool {
	covered := map[string]bool{}

	// Add exact upstream keys from registry.
	upstreamKeys := reg.UpstreamKeys()
	for key := range upstreamKeys {
		covered[key] = true
	}

	// Add route-prefix coverage: mark inventory routes with trailing "/" as covered
	// if any upstream route has that route as a prefix.
	for key := range upstreamKeys {
		// Parse the key to extract the route if it's a route key.
		if !strings.HasPrefix(key, "route:") {
			continue
		}
		route := strings.TrimPrefix(key, "route:")
		for _, it := range inv.Items {
			if it.Kind == "route" && strings.HasSuffix(it.Name, "/") {
				if strings.HasPrefix(route, it.Name) {
					k := inv.Key(it)
					covered[k] = true
				}
			}
		}
	}

	return covered
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

// HardFailures returns formatted error messages for unmapped items and stale rows.
// The messages include a suggested row stub for unmapped items.
func (d DriftReport) HardFailures() []string {
	var out []string

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
		absentWord := "absent-but-present"
		if absentCount != 1 {
			absentWord = "absent-but-present"
		}
		parts = append(parts, fmt.Sprintf("%d %s", absentCount, absentWord))
	}

	return strings.Join(parts, ", ")
}
