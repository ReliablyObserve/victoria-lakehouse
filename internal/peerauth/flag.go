package peerauth

import (
	"fmt"
	"strconv"
)

// TriBool is a boolean command-line flag that also knows whether it was given:
// -flag and -flag=true give true, -flag=false gives false, and a flag that was
// never given (or was set to the empty string) stays unset. It lets a flag
// override a tri-state config value (peer.auth_enabled) only when the operator
// asked to, whether the value came from the command line or, through
// VictoriaMetrics' envflag, from the environment (both end in Set).
//
// It implements flag.Value, flag.Getter and the IsBoolFlag hook of the flag
// package, so "-flag" without a value works as for a plain bool flag.
type TriBool struct {
	given bool
	val   bool
}

// Set implements flag.Value. "" resets to unset.
func (t *TriBool) Set(s string) error {
	if s == "" {
		*t = TriBool{}
		return nil
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return fmt.Errorf("want true or false, got %q", s)
	}
	*t = TriBool{given: true, val: v}
	return nil
}

// String implements flag.Value: "" when unset.
func (t *TriBool) String() string {
	if t == nil || !t.given {
		return ""
	}
	return strconv.FormatBool(t.val)
}

// Get implements flag.Getter: the value, false when unset.
func (t *TriBool) Get() any { return t != nil && t.given && t.val }

// IsBoolFlag lets "-flag" mean "-flag=true".
func (t *TriBool) IsBoolFlag() bool { return true }

// Apply points dst at a copy of the value when the flag was given and leaves
// dst alone otherwise.
func (t *TriBool) Apply(dst **bool) {
	if t == nil || !t.given {
		return
	}
	v := t.val
	*dst = &v
}
