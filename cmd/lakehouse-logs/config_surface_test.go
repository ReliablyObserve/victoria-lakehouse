package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"os"
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
