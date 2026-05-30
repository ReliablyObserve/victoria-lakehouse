package resourcebounds

import (
	"fmt"
	"strings"
)

// ParseScalingPolicy converts an operator-facing string ("fixed",
// "linear", "expbackoff") into the matching ScalingPolicy. Defaults
// to Fixed for the empty string. Surface owners should treat any
// returned error as a fatal startup failure — a typo in the operator
// flag is a misconfiguration, not a soft fallback condition.
func ParseScalingPolicy(s string) (ScalingPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "fixed":
		return Fixed, nil
	case "linear", "lineargrowth", "linear-growth":
		return LinearGrowth, nil
	case "expbackoff", "exponential", "exponential-backoff":
		return ExponentialBackoff, nil
	default:
		return Fixed, fmt.Errorf("unknown scaling policy %q (want one of: fixed, linear, expbackoff)", s)
	}
}

// Resolve picks the live request/limit/policy triple to use given:
//   - the operator-facing request/limit/scaling triple (zero values mean
//     "not set"),
//   - the deprecated single-value alias (used when the triple is
//     entirely zero),
//   - and a built-in default applied when neither was specified.
//
// Returns (request, limit, policy, deprecatedAliasUsed).
//
// Surface owners log a startup warning when deprecatedAliasUsed is
// true; the runtime path uses the returned triple unchanged.
//
// Policy: when scaling is empty, defaults to "fixed". An unparseable
// scaling string returns an error — the caller should fail startup.
func Resolve(requestFlag, limitFlag int64, scaling string, deprecatedAlias int64, builtinDefault int64) (int64, int64, ScalingPolicy, bool, error) {
	policy, err := ParseScalingPolicy(scaling)
	if err != nil {
		return 0, 0, Fixed, false, err
	}

	if requestFlag > 0 || limitFlag > 0 {
		// New-style triple is set. Use the supplied values; if only
		// one of request/limit is supplied, mirror the missing axis
		// from the other (a Request without Limit means "Request =
		// Limit" which collapses to flat behaviour at the request
		// value).
		req := requestFlag
		lim := limitFlag
		if req <= 0 {
			req = lim
		}
		if lim <= 0 {
			lim = req
		}
		return req, lim, policy, false, nil
	}

	if deprecatedAlias > 0 {
		// Legacy single-value flag is set. Behave as flat ceiling
		// at the alias value (request == limit == alias). Surface
		// owner is expected to log a deprecation warning.
		return deprecatedAlias, deprecatedAlias, Fixed, true, nil
	}

	// Nothing set; fall back to the built-in default.
	return builtinDefault, builtinDefault, Fixed, false, nil
}
