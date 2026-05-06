package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	target := flag.String("target", "http://localhost:9428", "Lakehouse target URL")
	mode := flag.String("mode", "all", "Test mode: latency, throughput, mixed, all, realistic, benchmark, verify, e2e, compare")
	duration := flag.String("duration", "60s", "Test duration")
	iterations := flag.Int("iterations", 100, "Iterations per latency test")
	warmup := flag.Int("warmup", 3, "Warmup iterations per test (realistic mode)")
	output := flag.String("output", "", "JSON output file path")
	e2eDirect := flag.String("e2e-direct", "", "E2E: lakehouse direct URL (no proxy)")
	e2eProxy := flag.String("e2e-proxy", "", "E2E: lakehouse through proxy URL")
	e2eVL := flag.String("e2e-vl", "", "E2E: VictoriaLogs URL for comparison")
	compareVL := flag.String("compare-vl", "", "Compare: VictoriaLogs URL for head-to-head")
	flag.Parse()

	report := &Report{
		Mode:     *mode,
		Duration: *duration,
		Target:   *target,
	}

	switch *mode {
	case "latency":
		report.LatencyBenchmarks = runLatencyBenchmarks(*target, *iterations)
	case "throughput":
		report.ThroughputTests = runThroughputTests(*target, *duration)
	case "mixed":
		report.ThroughputTests = map[string]*ThroughputResult{
			"mixed_workload": runMixedWorkload(*target, *duration),
		}
	case "all":
		report.LatencyBenchmarks = runLatencyBenchmarks(*target, *iterations)
		report.ThroughputTests = runThroughputTests(*target, *duration)
	case "realistic":
		report.RealisticResults = runRealisticBenchmarks(*target, *iterations, *warmup)
		printRealisticSummary(report.RealisticResults)
	case "verify":
		report.VerifyResults = runVerify(*target)
	case "compare":
		cmpCfg := CompareConfig{
			LakehouseURL: *target,
			VLURL:        *compareVL,
			Iterations:   *iterations,
			Warmup:       *warmup,
		}
		if cmpCfg.VLURL == "" {
			fmt.Fprintf(os.Stderr, "compare mode requires --compare-vl flag\n")
			os.Exit(1)
		}
		cmpReport := runCompare(cmpCfg)
		if *output != "" {
			if err := cmpReport.WriteJSON(*output); err != nil {
				fmt.Fprintf(os.Stderr, "failed to write compare report: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Compare report written to %s\n", *output)
		}
		return
	case "e2e":
		e2eCfg := E2EBenchConfig{
			LakehouseDirectURL: *e2eDirect,
			LakehouseProxyURL:  *e2eProxy,
			VLURL:              *e2eVL,
			Iterations:         *iterations,
			Warmup:             *warmup,
		}
		e2eReport := runE2EBench(e2eCfg)
		if *output != "" {
			if err := e2eReport.WriteJSON(*output); err != nil {
				fmt.Fprintf(os.Stderr, "failed to write e2e report: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("E2E report written to %s\n", *output)
		}
		return
	case "benchmark":
		bcfg := BenchmarkConfig{
			Endpoint:  "",
			Bucket:    "obs-archive",
			AccessKey: "minioadmin",
			SecretKey: "minioadmin",
		}
		report.Benchmarks = runBenchmarks(bcfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	report.ComputePass()
	report.PrintSummary()

	if *output != "" {
		if err := report.WriteToFile(*output); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write report: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Report written to %s\n", *output)
	}

	if !report.Pass {
		os.Exit(1)
	}
}
