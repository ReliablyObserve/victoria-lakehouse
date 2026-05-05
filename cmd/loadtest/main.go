package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	target := flag.String("target", "http://localhost:9428", "Lakehouse target URL")
	mode := flag.String("mode", "all", "Test mode: latency, throughput, mixed, all")
	duration := flag.String("duration", "60s", "Test duration")
	iterations := flag.Int("iterations", 100, "Iterations per latency test")
	output := flag.String("output", "", "JSON output file path")
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
