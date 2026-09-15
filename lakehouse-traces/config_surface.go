package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// surfaceBinaryName names this binary in its configuration surface.
const surfaceBinaryName = "lakehouse-traces"

// configSurfaceFlags returns the flags this binary defines — every
// -lakehouse.* flag plus -httpListenAddr — sharing the process flag values,
// without the flags linked in from VictoriaTraces/VictoriaLogs/VictoriaMetrics
// packages.
func configSurfaceFlags() *flag.FlagSet {
	fs := flag.NewFlagSet(surfaceBinaryName, flag.ContinueOnError)
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "lakehouse.") || f.Name == "httpListenAddr" {
			fs.Var(f.Value, f.Name, f.Usage)
		}
	})
	return fs
}

// defaultConfigSurface renders what `print-default-config` prints. The
// golden test pins it in testdata/config-surface.json.
func defaultConfigSurface() ([]byte, error) {
	s, err := config.DescribeSurface(surfaceBinaryName, config.ModeTraces, configSurfaceFlags(), applyFlags)
	if err != nil {
		return nil, err
	}
	return s.JSON()
}

// runPrintDefaultConfigSubcommand prints the configuration surface as JSON:
// every config key with its default and how a config-file value merges,
// every profile as the keys it overrides, and every flag with the key it
// sets. It reads no config file and no flags.
//
// Usage: `lakehouse-traces print-default-config`
func runPrintDefaultConfigSubcommand() {
	// Finding the key behind -lakehouse.tenant.alias probes it with a value
	// the alias parser rejects with a warning; keep stderr quiet.
	_ = flag.Set("loggerLevel", "ERROR")
	out, err := defaultConfigSurface()
	if err != nil {
		fmt.Fprintf(os.Stderr, "print-default-config: %s\n", err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(out)
}
