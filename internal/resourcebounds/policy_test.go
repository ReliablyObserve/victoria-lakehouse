package resourcebounds

import "testing"

func TestParseScalingPolicy(t *testing.T) {
	tests := []struct {
		in      string
		want    ScalingPolicy
		wantErr bool
	}{
		{"", Fixed, false},
		{"fixed", Fixed, false},
		{"FIXED", Fixed, false},
		{"linear", LinearGrowth, false},
		{"lineargrowth", LinearGrowth, false},
		{"expbackoff", ExponentialBackoff, false},
		{"exponential", ExponentialBackoff, false},
		{"bogus", Fixed, true},
	}
	for _, tt := range tests {
		got, err := ParseScalingPolicy(tt.in)
		if tt.wantErr && err == nil {
			t.Errorf("ParseScalingPolicy(%q) want err, got nil", tt.in)
		}
		if !tt.wantErr && err != nil {
			t.Errorf("ParseScalingPolicy(%q) unexpected err: %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("ParseScalingPolicy(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestResolve_NewStyleTriple(t *testing.T) {
	req, lim, pol, dep, err := Resolve(4, 16, "fixed", 0, 8)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if req != 4 || lim != 16 || pol != Fixed || dep {
		t.Errorf("Resolve(4,16,fixed,0,8) = (%d,%d,%v,%v), want (4,16,fixed,false)", req, lim, pol, dep)
	}
}

func TestResolve_OnlyRequestSet(t *testing.T) {
	// Request without Limit: mirror to collapse to flat at Request.
	req, lim, _, _, _ := Resolve(8, 0, "", 0, 16)
	if req != 8 || lim != 8 {
		t.Errorf("Resolve(8,0,) = (%d,%d), want (8,8)", req, lim)
	}
}

func TestResolve_OnlyLimitSet(t *testing.T) {
	req, lim, _, _, _ := Resolve(0, 16, "", 0, 8)
	if req != 16 || lim != 16 {
		t.Errorf("Resolve(0,16,) = (%d,%d), want (16,16)", req, lim)
	}
}

func TestResolve_DeprecatedAlias(t *testing.T) {
	req, lim, pol, dep, err := Resolve(0, 0, "", 32, 8)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if req != 32 || lim != 32 || pol != Fixed || !dep {
		t.Errorf("Resolve(0,0,,32,8) = (%d,%d,%v,%v), want (32,32,fixed,true)", req, lim, pol, dep)
	}
}

func TestResolve_BuiltinDefault(t *testing.T) {
	req, lim, pol, dep, err := Resolve(0, 0, "", 0, 8)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if req != 8 || lim != 8 || pol != Fixed || dep {
		t.Errorf("Resolve(0,0,,0,8) = (%d,%d,%v,%v), want (8,8,fixed,false)", req, lim, pol, dep)
	}
}

func TestResolve_BadScalingPolicy(t *testing.T) {
	_, _, _, _, err := Resolve(4, 16, "bogus", 0, 8)
	if err == nil {
		t.Error("Resolve with bad scaling: want err, got nil")
	}
}

func TestResolve_NewStyleTakesPrecedence(t *testing.T) {
	// When both new triple AND deprecated alias are set, new triple
	// wins and the deprecation flag is NOT set (operator has migrated).
	req, lim, _, dep, _ := Resolve(4, 16, "", 32, 8)
	if req != 4 || lim != 16 || dep {
		t.Errorf("Resolve(4,16,,32,8) = (%d,%d,dep=%v), want (4,16,false)", req, lim, dep)
	}
}
