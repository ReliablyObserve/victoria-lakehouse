package main

import (
	"flag"
	"strings"
	"testing"
)

// Every listed flag must exist in this binary (vlselect registers it), or the
// list is stale and the check guards nothing.
func TestVLSelectFlagsNotHonoured_AreRegistered(t *testing.T) {
	for _, name := range vlselectFlagsNotHonoured {
		if flag.Lookup(name) == nil {
			t.Errorf("-%s is not registered in lakehouse-logs; drop it from vlselectFlagsNotHonoured", name)
		}
	}
}

func TestCheckUpstreamFlagsHonoured_DefaultsPass(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Bool("select.disable", false, "")
	fs.Bool("internaldelete.enable", false, "")
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if err := checkUpstreamFlagsHonoured(fs); err != nil {
		t.Fatalf("no flag set: err = %v, want nil", err)
	}
}

func TestCheckUpstreamFlagsHonoured_RefusesEverySetFlag(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	for _, name := range vlselectFlagsNotHonoured {
		fs.String(name, "", "")
	}
	fs.Bool("internaldelete.enable", false, "")
	args := []string{"-internaldelete.enable=true"}
	for _, name := range vlselectFlagsNotHonoured {
		args = append(args, "-"+name+"=1")
	}
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	err := checkUpstreamFlagsHonoured(fs)
	if err == nil {
		t.Fatal("err = nil, want a refusal")
	}
	for _, name := range vlselectFlagsNotHonoured {
		if !strings.Contains(err.Error(), "-"+name) {
			t.Errorf("refusal %q does not name -%s", err, name)
		}
	}
	if strings.Contains(err.Error(), "internaldelete.enable") {
		t.Errorf("refusal names -internaldelete.enable, which this binary honours: %q", err)
	}
}
