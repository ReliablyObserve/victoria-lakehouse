package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

const configSurfaceGolden = "testdata/config-surface.json"

// TestConfigSurfaceGolden pins what `lakehouse-logs print-default-config`
// prints. docs/configuration.md and the Helm chart are generated and checked
// from this file by scripts/ci/config_drift_report.py, so changing a default,
// a profile, a flag or the config-file merge rules means regenerating it:
//
//	make config-surface
func TestConfigSurfaceGolden(t *testing.T) {
	got, err := defaultConfigSurface()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CONFIG_SURFACE_UPDATE") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configSurfaceGolden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := testutil.DiffGolden(configSurfaceGolden, got); err != nil {
		t.Fatalf("%v\nregenerate with `make config-surface` and commit the result", err)
	}
}

func TestConfigSurfaceCoversEveryLakehouseFlag(t *testing.T) {
	out, err := defaultConfigSurface()
	if err != nil {
		t.Fatal(err)
	}
	var s config.Surface
	if err := json.Unmarshal(out, &s); err != nil {
		t.Fatal(err)
	}
	if s.Binary != "lakehouse-logs" || s.Mode != config.ModeLogs {
		t.Fatalf("surface names %s/%s, want lakehouse-logs/logs", s.Binary, s.Mode)
	}
	listed := map[string]config.FlagInfo{}
	for _, f := range s.Flags {
		listed[f.Name] = f
	}
	flag.VisitAll(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, "lakehouse.") {
			return
		}
		info, ok := listed[f.Name]
		if !ok {
			t.Errorf("flag -%s missing from the configuration surface", f.Name)
			return
		}
		if f.Name != "lakehouse.config" && len(info.Keys) == 0 {
			t.Errorf("flag -%s writes no config key", f.Name)
		}
	})
	if _, ok := listed["httpListenAddr"]; !ok {
		t.Error("-httpListenAddr missing from the configuration surface")
	}
	if _, ok := listed["loggerLevel"]; ok {
		t.Error("upstream flags must not be part of the lakehouse configuration surface")
	}
}

func TestPrintDefaultConfigSubcommand(t *testing.T) {
	level := flag.Lookup("loggerLevel")
	prev := level.Value.String()
	defer func() { _ = flag.Set("loggerLevel", prev) }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	read := make(chan []byte)
	go func() {
		b, _ := io.ReadAll(r)
		read <- b
	}()
	stdout := os.Stdout
	os.Stdout = w
	runPrintDefaultConfigSubcommand()
	os.Stdout = stdout
	_ = w.Close()
	got := <-read

	want, err := defaultConfigSurface()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("print-default-config printed %d bytes that differ from the surface (%d bytes)", len(got), len(want))
	}
	if level.Value.String() != "ERROR" {
		t.Errorf("print-default-config left loggerLevel at %q, want ERROR", level.Value.String())
	}
}

// TestCompactionEnabledResolution pins which of the code default, a profile,
// the config file and the flags decides compaction.enabled in this binary,
// the way main loads it: config.LoadWithMode, then applyFlags. Documented in
// docs/configuration.md#compaction-on-or-off.
func TestCompactionEnabledResolution(t *testing.T) {
	cases := []struct {
		name        string
		file        string // body under `lakehouse:`; empty means no config file
		role        config.Role
		flagProfile string
		flagEnabled string
		want        bool
	}{
		{name: "code default", want: true},
		{name: "file false under the balanced base is ignored", file: "compaction:\n    enabled: false", want: true},
		{name: "file profile max-cost-savings", file: "profile: max-cost-savings", want: false},
		{name: "file profile dev", file: "profile: dev", want: false},
		{name: "file profile max-durability", file: "profile: max-durability", want: true},
		{name: "file true over a file profile that disables", file: "profile: dev\n  compaction:\n    enabled: true", want: true},
		{name: "signal profile in the file", file: "logs:\n    profile: max-cost-savings", want: false},
		{name: "role profile in the file for this role", file: "logs:\n    insert:\n      profile: dev", role: config.RoleInsert, want: false},
		{name: "role profile in the file for another role", file: "logs:\n    insert:\n      profile: dev", role: config.RoleSelect, want: true},
		{name: "flag profile max-cost-savings keeps compaction on", flagProfile: "max-cost-savings", want: true},
		{name: "flag false does not disable", flagEnabled: "false", want: true},
		{name: "flag true over a file profile that disables", file: "profile: dev", flagEnabled: "true", want: true},
		{name: "chart shape: file profile and file false", file: "profile: max-cost-savings\n  compaction:\n    enabled: false", flagProfile: "max-cost-savings", want: false},
		{name: "chart shape: file profile and the chart's true", file: "profile: max-cost-savings\n  compaction:\n    enabled: true", flagProfile: "max-cost-savings", want: true},
		{name: "chart shape: signal profile only as a flag, file false", file: "compaction:\n    enabled: false", flagProfile: "max-cost-savings", want: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := ""
			if c.file != "" {
				path = filepath.Join(t.TempDir(), "config.yaml")
				body := "lakehouse:\n  " + c.file + "\n"
				if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := config.LoadWithMode(path, config.ModeLogs, c.role)
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range map[string]string{"lakehouse.profile": c.flagProfile, "lakehouse.compaction.enabled": c.flagEnabled} {
				if value == "" {
					continue
				}
				f := flag.Lookup(name)
				if err := flag.Set(name, value); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = flag.Set(name, f.DefValue) }()
			}
			applyFlags(cfg)
			if cfg.Compaction.Enabled != c.want {
				t.Errorf("compaction.enabled = %v, want %v", cfg.Compaction.Enabled, c.want)
			}
		})
	}
}
