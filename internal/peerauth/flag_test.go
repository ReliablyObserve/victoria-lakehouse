package peerauth

import (
	"flag"
	"testing"
)

func TestTriBool(t *testing.T) {
	var tb TriBool
	if tb.String() != "" || tb.Get() != false {
		t.Fatalf("zero value: %q %v, want unset", tb.String(), tb.Get())
	}
	var dst *bool
	tb.Apply(&dst)
	if dst != nil {
		t.Fatal("an unset flag changed the destination")
	}
	for _, tc := range []struct {
		in   string
		want string
		val  bool
	}{{"true", "true", true}, {"false", "false", false}, {"1", "true", true}, {"0", "false", false}} {
		if err := tb.Set(tc.in); err != nil {
			t.Fatal(err)
		}
		if tb.String() != tc.want || tb.Get() != tc.val {
			t.Errorf("Set(%q): %q %v", tc.in, tb.String(), tb.Get())
		}
		dst = nil
		tb.Apply(&dst)
		if dst == nil || *dst != tc.val {
			t.Errorf("Set(%q): Apply gave %v", tc.in, dst)
		}
		if dst == &tb.val {
			t.Error("Apply aliases the flag's own storage")
		}
	}
	if err := tb.Set("maybe"); err == nil {
		t.Error("Set(maybe) was accepted")
	}
	if tb.String() != "0" && tb.String() != "false" {
		t.Errorf("a refused value changed the flag to %q", tb.String())
	}
	if err := tb.Set(""); err != nil || tb.String() != "" {
		t.Errorf("Set(empty) must reset to unset: %v %q", err, tb.String())
	}
	var nilTB *TriBool
	if nilTB.String() != "" || nilTB.Get() != false {
		t.Error("nil TriBool is not unset")
	}
	nilTB.Apply(&dst)
}

// As a flag: "-f" means true, "-f=false" false, absent stays unset.
func TestTriBool_AsFlag(t *testing.T) {
	for args, want := range map[string]string{"": "", "-f": "true", "-f=true": "true", "-f=false": "false"} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		tb := &TriBool{}
		fs.Var(tb, "f", "")
		var in []string
		if args != "" {
			in = []string{args}
		}
		if err := fs.Parse(in); err != nil {
			t.Fatal(err)
		}
		if tb.String() != want {
			t.Errorf("args %q: %q, want %q", args, tb.String(), want)
		}
	}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(discard{})
	fs.Var(&TriBool{}, "f", "")
	if err := fs.Parse([]string{"-f=perhaps"}); err == nil {
		t.Error("an invalid value was accepted")
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
