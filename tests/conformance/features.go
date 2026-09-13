package conformance

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/report"
)

// FeatureDrift is the feature-catalog half of the conformance gate: it joins
// the catalog with the registry and the changelog and reports everything that
// would let a working Lakehouse feature slip past verification — a Lakehouse
// row nobody claims, a shipped feature nothing checks, a release note with no
// feature behind it.
type FeatureDrift struct {
	// Hard failures.
	RowsWithoutFeature []string                   // lh-addition/lh-shim row ids no feature claims
	RowsInManyFeatures []string                   // "<row id>: <feature>, <feature>"
	UnknownRowRefs     []string                   // "<feature id> -> <row id>" where the row does not exist
	ShippedUnverified  []string                   // shipped features with neither a row nor a test (rule b)
	UnmappedBullets    []registry.ChangelogBullet // `### Added` bullets whose bold lead-in no feature claims
	RowsMustBePending  []string                   // "<feature id> -> <row id>": non-shipped feature citing an executing row

	// Soft, reported so the maintainer can work the list down.
	VerificationGaps []string // shipped features whose only verification is a pending row
}

// CheckFeatures runs every feature-catalog rule:
//
//	(a) every registry row with origin lh-addition or lh-shim belongs to
//	    exactly one feature — a Lakehouse behavior with a row but no feature
//	    is a capability the catalog (and so the generated docs) forgets;
//	(b) every shipped feature has at least one row or test;
//	(c) every referenced row exists (tests and docs are checked on disk by
//	    registry.LoadFeatures);
//	(d) every `### Added` changelog bullet with a bold lead-in is claimed by
//	    exactly one feature (claimed twice is rejected at load time);
//	(e) an in-progress or planned feature may only cite pending rows — a row
//	    that executes today contradicts "not shipped yet".
func CheckFeatures(set *registry.FeatureSet, reg *registry.Registry, bullets []registry.ChangelogBullet) FeatureDrift {
	var d FeatureDrift

	claimedBy := map[string][]string{} // row id -> feature ids
	for i := range set.Features {
		f := &set.Features[i]
		for _, rowID := range f.Rows {
			claimedBy[rowID] = append(claimedBy[rowID], f.ID)
			r := reg.ByID[rowID]
			if r == nil {
				d.UnknownRowRefs = append(d.UnknownRowRefs, fmt.Sprintf("%s -> %s", f.ID, rowID))
				continue
			}
			if f.Status != registry.StatusShipped && !r.Pending {
				d.RowsMustBePending = append(d.RowsMustBePending, fmt.Sprintf("%s -> %s", f.ID, rowID))
			}
		}
		if f.Status == registry.StatusShipped && len(f.Rows) == 0 && len(f.Tests) == 0 {
			d.ShippedUnverified = append(d.ShippedUnverified, f.ID)
		}
	}

	for _, r := range reg.Rows {
		if r.Origin != registry.OriginLHAddition && r.Origin != registry.OriginLHShim {
			continue
		}
		owners := claimedBy[r.ID]
		switch {
		case len(owners) == 0:
			d.RowsWithoutFeature = append(d.RowsWithoutFeature, r.ID)
		case len(owners) > 1:
			sorted := append([]string(nil), owners...)
			sort.Strings(sorted)
			d.RowsInManyFeatures = append(d.RowsInManyFeatures, fmt.Sprintf("%s: %s", r.ID, strings.Join(sorted, ", ")))
		}
	}

	leadIns := set.LeadIns()
	for _, b := range bullets {
		if b.LeadIn == "" {
			// A bullet without a bold lead-in is a sub-detail of the
			// release note (a file touched, a test added), not a feature
			// claim — there is nothing stable to match it on, so it is
			// deliberately out of scope for the gate.
			continue
		}
		if _, ok := leadIns[b.LeadIn]; !ok {
			d.UnmappedBullets = append(d.UnmappedBullets, b)
		}
	}

	d.VerificationGaps = report.VerificationGaps(set, reg)

	sort.Strings(d.RowsWithoutFeature)
	sort.Strings(d.RowsInManyFeatures)
	sort.Strings(d.UnknownRowRefs)
	sort.Strings(d.ShippedUnverified)
	sort.Strings(d.RowsMustBePending)
	return d
}

// HardFailures returns one message per rule violation that must fail CI, each
// naming the fix.
func (d FeatureDrift) HardFailures() []string {
	var out []string
	for _, id := range d.RowsWithoutFeature {
		out = append(out, fmt.Sprintf("registry row %s (a Lakehouse addition/shim) belongs to no feature — add it to a feature's `rows:` in tests/conformance/registry/features/", id))
	}
	for _, msg := range d.RowsInManyFeatures {
		out = append(out, fmt.Sprintf("registry row claimed by more than one feature — %s; a row verifies exactly one feature", msg))
	}
	for _, msg := range d.UnknownRowRefs {
		out = append(out, fmt.Sprintf("feature cites a registry row that does not exist — %s", msg))
	}
	for _, id := range d.ShippedUnverified {
		out = append(out, fmt.Sprintf("feature %s is shipped but cites neither a registry row nor a test — link its real regression test (or add a row), or correct its status", id))
	}
	for _, msg := range d.RowsMustBePending {
		out = append(out, fmt.Sprintf("feature is not shipped but cites a row that executes today — %s; mark the row `pending: true` or the feature `shipped`", msg))
	}
	for _, b := range d.UnmappedBullets {
		out = append(out, fmt.Sprintf("CHANGELOG %s line %d: `### Added` bullet **%s** maps to no feature — add it to a feature's `changelog_bullets:` (or add the feature) in tests/conformance/registry/features/", b.Version, b.Line, b.LeadIn))
	}
	return out
}

// Summary returns a one-line feature-catalog summary for CI logs.
func (d FeatureDrift) Summary() string {
	return strings.Join([]string{
		fmt.Sprintf("%d rows without feature", len(d.RowsWithoutFeature)),
		fmt.Sprintf("%d shipped unverified", len(d.ShippedUnverified)),
		fmt.Sprintf("%d unmapped changelog bullets", len(d.UnmappedBullets)),
		fmt.Sprintf("%d verification gaps", len(d.VerificationGaps)),
	}, ", ")
}
