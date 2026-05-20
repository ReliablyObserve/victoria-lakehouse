package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	tier := flag.String("tier", "small", "Data tier: small, medium, large")
	signal := flag.String("signal", "logs", "Signal: logs, traces, both")
	endpoint := flag.String("endpoint", "http://localhost:9428", "Lakehouse endpoint")
	output := flag.String("output", "benchmarks", "Output directory for baseline files")
	seedOnly := flag.Bool("seed-only", false, "Only seed data, don't run benchmarks")
	runs := flag.Int("runs", 3, "Number of benchmark runs (median used)")
	flag.Parse()

	tierRows := map[string]int{
		"small":  50000,
		"medium": 500000,
		"large":  2500000,
	}
	rows, ok := tierRows[*tier]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown tier: %s\n", *tier)
		os.Exit(1)
	}

	signals := []string{*signal}
	if *signal == "both" {
		signals = []string{"logs", "traces"}
	}

	for _, sig := range signals {
		log.Printf("Seeding %s data: %d rows at %s", sig, rows, *endpoint)
		if err := seedData(seedConfig{endpoint: *endpoint, rows: rows, signal: sig}); err != nil {
			log.Fatalf("Seed failed: %v", err)
		}
		log.Printf("Seed complete for %s", sig)

		if *seedOnly {
			continue
		}

		log.Printf("Running %s benchmarks (%d runs)...", sig, *runs)
		baseline := &Baseline{
			Timestamp: time.Now().UTC(),
			GitSHA:    gitSHA(),
			Tier:      *tier,
			Signal:    sig,
			FileCount: rows,
			Write:     make(map[string]WriteResult),
			Read:      benchmarkQueries(*endpoint, sig, *runs),
		}

		path := baselineFilePath(*output, sig, *tier)
		if err := writeBaseline(path, baseline); err != nil {
			log.Fatalf("Write baseline: %v", err)
		}
		log.Printf("Baseline written to %s", path)
	}
}

func gitSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
