package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type ExtCompareConfig struct {
	LakehouseURL string
	VLURL        string
	Iterations   int
	Warmup       int
}

type ExtScenario struct {
	Name     string
	Category string
	Range    string
	LHURLFn  func(target string) string
	VLURLFn  func(target string) string
}

type ExtResult struct {
	Name       string  `json:"name"`
	Category   string  `json:"category"`
	Range      string  `json:"range"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
	Iterations int     `json:"iterations"`
	HasData    bool    `json:"has_data"`
	AvgBytes   int     `json:"avg_response_bytes,omitempty"`
}

type ExtCompareReport struct {
	Timestamp  string             `json:"timestamp"`
	LHResults  []ExtResult        `json:"lakehouse_s3"`
	VLResults  []ExtResult        `json:"victorialogs_ebs"`
	Comparison []ExtCompareRow    `json:"comparison"`
}

type ExtCompareRow struct {
	Scenario  string  `json:"scenario"`
	Category  string  `json:"category"`
	Range     string  `json:"range"`
	LHP95     float64 `json:"lh_s3_p95_ms"`
	VLP95     float64 `json:"vl_ebs_p95_ms"`
	Ratio     float64 `json:"ratio"`
	Winner    string  `json:"winner"`
	LHHasData bool    `json:"lh_has_data"`
	VLHasData bool    `json:"vl_has_data"`
	LHAvgKB   float64 `json:"lh_avg_kb,omitempty"`
	VLAvgKB   float64 `json:"vl_avg_kb,omitempty"`
}

func buildExtScenarios() []ExtScenario {
	now := time.Now()
	h1 := now.Add(-1 * time.Hour)
	h6 := now.Add(-6 * time.Hour)
	d1 := now.Add(-24 * time.Hour)
	d7 := now.Add(-7 * 24 * time.Hour)

	type tr struct {
		name  string
		start time.Time
	}
	ranges := []tr{
		{"1h", h1}, {"6h", h6}, {"24h", d1}, {"7d", d7},
	}

	var scenarios []ExtScenario

	for _, r := range ranges {
		s, e := r.start, now

		// --- QUERY: wildcard ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("query_wildcard_%s", r.name), Category: "query", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=100", t, s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=100", t, s.UnixNano(), e.UnixNano())
			},
		})

		// --- QUERY: service filter ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("query_service_%s", r.name), Category: "query", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`service.name:="api-gateway"`), s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`service.name:="api-gateway"`), s.UnixNano(), e.UnixNano())
			},
		})

		// --- QUERY: level filter ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("query_level_%s", r.name), Category: "query", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`level:="ERROR"`), s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`level:="ERROR"`), s.UnixNano(), e.UnixNano())
			},
		})

		// --- QUERY: compound filter ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("query_compound_%s", r.name), Category: "query", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=30",
					t, url.QueryEscape(`service.name:="api-gateway" AND level:="ERROR"`), s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=30",
					t, url.QueryEscape(`service.name:="api-gateway" AND level:="ERROR"`), s.UnixNano(), e.UnixNano())
			},
		})

		// --- STATS: count ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("stats_count_%s", r.name), Category: "stats", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d", t, s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape("* | stats count() rows"), s.UnixNano(), e.UnixNano())
			},
		})

		// --- STATS: filtered count ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("stats_count_filtered_%s", r.name), Category: "stats", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape(`service.name:="api-gateway"`), s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape(`service.name:="api-gateway" | stats count() rows`), s.UnixNano(), e.UnixNano())
			},
		})

		// --- STATS: count_uniq (VL pipe, LH field_values cardinality) ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("count_uniq_service_%s", r.name), Category: "cardinality", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=service.name&limit=1000&start=%d&end=%d",
					t, s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape("* | stats count_uniq(service.name) services"), s.UnixNano(), e.UnixNano())
			},
		})

		// --- STATS: group by level (VL pipe, LH field_values) ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("group_by_level_%s", r.name), Category: "aggregation", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=level&limit=100&start=%d&end=%d",
					t, s.UnixNano(), e.UnixNano())
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape("* | stats by(level) count() rows"), s.UnixNano(), e.UnixNano())
			},
		})

		// --- RATE: stats_query_range (count per time bucket = rate) ---
		step := "300s"
		if r.name == "24h" || r.name == "7d" {
			step = "3600s"
		}
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("rate_%s", r.name), Category: "rate", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query_range?query=*&start=%d&end=%d&step=%s",
					t, s.UnixNano(), e.UnixNano(), step)
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query_range?query=%s&start=%d&end=%d&step=%s",
					t, url.QueryEscape("* | stats count() rows"), s.UnixNano(), e.UnixNano(), step)
			},
		})

		// --- RATE: filtered rate ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("rate_error_%s", r.name), Category: "rate", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query_range?query=%s&start=%d&end=%d&step=%s",
					t, url.QueryEscape(`level:="ERROR"`), s.UnixNano(), e.UnixNano(), step)
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query_range?query=%s&start=%d&end=%d&step=%s",
					t, url.QueryEscape(`level:="ERROR" | stats count() rows`), s.UnixNano(), e.UnixNano(), step)
			},
		})

		// --- HITS: histogram ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("hits_%s", r.name), Category: "histogram", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/hits?query=*&start=%d&end=%d&step=%s",
					t, s.UnixNano(), e.UnixNano(), step)
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/hits?query=*&start=%d&end=%d&step=%s",
					t, s.UnixNano(), e.UnixNano(), step)
			},
		})

		// --- HITS: filtered histogram ---
		scenarios = append(scenarios, ExtScenario{
			Name: fmt.Sprintf("hits_filtered_%s", r.name), Category: "histogram", Range: r.name,
			LHURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/hits?query=%s&start=%d&end=%d&step=%s",
					t, url.QueryEscape(`service.name:="api-gateway"`), s.UnixNano(), e.UnixNano(), step)
			},
			VLURLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/hits?query=%s&start=%d&end=%d&step=%s",
					t, url.QueryEscape(`service.name:="api-gateway"`), s.UnixNano(), e.UnixNano(), step)
			},
		})
	}

	// --- METADATA (range-independent but we test once) ---
	scenarios = append(scenarios, ExtScenario{
		Name: "field_names", Category: "metadata", Range: "48h",
		LHURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/field_names?query=*&start=%d&end=%d",
				t, d7.UnixNano(), now.UnixNano())
		},
		VLURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/field_names?query=*&start=%d&end=%d",
				t, d7.UnixNano(), now.UnixNano())
		},
	})
	scenarios = append(scenarios, ExtScenario{
		Name: "field_values_service", Category: "metadata", Range: "48h",
		LHURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=service.name&limit=100&start=%d&end=%d",
				t, d7.UnixNano(), now.UnixNano())
		},
		VLURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=service.name&limit=100&start=%d&end=%d",
				t, d7.UnixNano(), now.UnixNano())
		},
	})
	scenarios = append(scenarios, ExtScenario{
		Name: "field_values_level", Category: "metadata", Range: "48h",
		LHURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=level&limit=100&start=%d&end=%d",
				t, d7.UnixNano(), now.UnixNano())
		},
		VLURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=level&limit=100&start=%d&end=%d",
				t, d7.UnixNano(), now.UnixNano())
		},
	})

	// --- POINT LOOKUP ---
	scenarios = append(scenarios, ExtScenario{
		Name: "bloom_trace_id_miss", Category: "point_lookup", Range: "48h",
		LHURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
				t, url.QueryEscape(`trace_id:="ffffffffffffffffffffffffffffffff"`), d7.UnixNano(), now.UnixNano())
		},
		VLURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
				t, url.QueryEscape(`trace_id:="ffffffffffffffffffffffffffffffff"`), d7.UnixNano(), now.UnixNano())
		},
	})

	// --- STREAMS ---
	scenarios = append(scenarios, ExtScenario{
		Name: "streams_list", Category: "metadata", Range: "48h",
		LHURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/streams?query=*&start=%d&end=%d", t, d1.UnixNano(), now.UnixNano())
		},
		VLURLFn: func(t string) string {
			return fmt.Sprintf("%s/select/logsql/streams?query=*&start=%d&end=%d", t, d1.UnixNano(), now.UnixNano())
		},
	})

	return scenarios
}

func runExtCompare(cfg ExtCompareConfig) ExtCompareReport {
	scenarios := buildExtScenarios()
	report := ExtCompareReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("  VL (EBS) vs VLH (S3) — EXTENDED COMPARISON")
	fmt.Println("  " + fmt.Sprintf("VLH: %s  |  VL: %s  |  Iterations: %d  Warmup: %d",
		cfg.LakehouseURL, cfg.VLURL, cfg.Iterations, cfg.Warmup))
	fmt.Println(strings.Repeat("=", 100))

	client := &http.Client{Timeout: 60 * time.Second}

	// Data verification preamble
	fmt.Println("\n  [DATA VERIFICATION]")
	now := time.Now()
	for _, label := range []string{"1h", "4h", "24h", "48h"} {
		var dur time.Duration
		switch label {
		case "1h":
			dur = 1 * time.Hour
		case "4h":
			dur = 4 * time.Hour
		case "24h":
			dur = 24 * time.Hour
		case "48h":
			dur = 48 * time.Hour
		}
		s := now.Add(-dur)
		lhBody, _ := httpGetBody(client, fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d",
			cfg.LakehouseURL, s.UnixNano(), now.UnixNano()))
		vlBody, _ := httpGetBody(client, fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
			cfg.VLURL, url.QueryEscape("* | stats count() rows"), s.UnixNano(), now.UnixNano()))
		lhCount := parseStatsCount(lhBody)
		vlCount := parseStatsCount(vlBody)
		ratio := "-"
		if lhCount > 0 {
			ratio = fmt.Sprintf("%.1fx", float64(vlCount)/float64(lhCount))
		}
		fmt.Printf("    %4s:  LH=%6d rows  VL=%6d rows  (VL/LH=%s)\n", label, lhCount, vlCount, ratio)
	}
	fmt.Println()

	fmt.Printf("\n  %-35s %12s %12s %8s %10s %6s %6s\n", "Scenario", "VLH S3 p95", "VL EBS p95", "Ratio", "Winner", "LH KB", "VL KB")
	fmt.Println("  " + strings.Repeat("-", 96))

	prevCategory := ""
	for _, sc := range scenarios {
		if sc.Category != prevCategory {
			fmt.Printf("\n  [%s]\n", sc.Category)
			prevCategory = sc.Category
		}

		// Warmup both
		for i := 0; i < cfg.Warmup; i++ {
			doRequest(client, sc.LHURLFn(cfg.LakehouseURL))
			doRequest(client, sc.VLURLFn(cfg.VLURL))
		}

		// Measure LH
		var lhLats []float64
		var lhTotalBytes int64
		for i := 0; i < cfg.Iterations; i++ {
			m := measureRequestFull(client, sc.LHURLFn(cfg.LakehouseURL))
			if m.latencyMs >= 0 {
				lhLats = append(lhLats, m.latencyMs)
				lhTotalBytes += m.bodyBytes
			}
		}

		// Measure VL
		var vlLats []float64
		var vlTotalBytes int64
		for i := 0; i < cfg.Iterations; i++ {
			m := measureRequestFull(client, sc.VLURLFn(cfg.VLURL))
			if m.latencyMs >= 0 {
				vlLats = append(vlLats, m.latencyMs)
				vlTotalBytes += m.bodyBytes
			}
		}

		lhR := buildExtResult(sc, lhLats)
		vlR := buildExtResult(sc, vlLats)
		if len(lhLats) > 0 {
			lhR.HasData = lhTotalBytes/int64(len(lhLats)) > 10
			lhR.AvgBytes = int(lhTotalBytes / int64(len(lhLats)))
		}
		if len(vlLats) > 0 {
			vlR.HasData = vlTotalBytes/int64(len(vlLats)) > 10
			vlR.AvgBytes = int(vlTotalBytes / int64(len(vlLats)))
		}
		report.LHResults = append(report.LHResults, lhR)
		report.VLResults = append(report.VLResults, vlR)

		ratio := 0.0
		winner := "-"
		if vlR.P95Ms > 0 && lhR.P95Ms > 0 {
			ratio = lhR.P95Ms / vlR.P95Ms
			if !lhR.HasData && vlR.HasData {
				winner = "VLH*EMPTY"
			} else if lhR.HasData && !vlR.HasData {
				winner = "VL*EMPTY"
			} else if !lhR.HasData && !vlR.HasData {
				winner = "BOTH*EMPTY"
			} else if ratio < 0.8 {
				winner = "VLH"
			} else if ratio > 1.2 {
				winner = "VL"
			} else {
				winner = "~tie"
			}
		}

		lhKB := float64(lhR.AvgBytes) / 1024.0
		vlKB := float64(vlR.AvgBytes) / 1024.0

		report.Comparison = append(report.Comparison, ExtCompareRow{
			Scenario: sc.Name, Category: sc.Category, Range: sc.Range,
			LHP95: lhR.P95Ms, VLP95: vlR.P95Ms, Ratio: ratio, Winner: winner,
			LHHasData: lhR.HasData, VLHasData: vlR.HasData,
			LHAvgKB: lhKB, VLAvgKB: vlKB,
		})

		dataFlag := ""
		if !lhR.HasData || !vlR.HasData {
			dataFlag = " ⚠"
		}
		fmt.Printf("  %-35s %11.1fms %11.1fms %7.1fx %10s %5.0fKB %5.0fKB%s\n",
			sc.Name, lhR.P95Ms, vlR.P95Ms, ratio, winner, lhKB, vlKB, dataFlag)
	}

	// Summary
	printExtSummary(report)
	return report
}

func doRequest(client *http.Client, u string) {
	resp, err := client.Get(u)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

type measurement struct {
	latencyMs float64
	bodyBytes int64
}

func measureRequestFull(client *http.Client, u string) measurement {
	start := time.Now()
	resp, err := client.Get(u)
	elapsed := time.Since(start)
	if err != nil {
		return measurement{latencyMs: -1}
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		return measurement{latencyMs: -1}
	}
	return measurement{
		latencyMs: float64(elapsed.Microseconds()) / 1000.0,
		bodyBytes: n,
	}
}

func buildExtResult(sc ExtScenario, latencies []float64) ExtResult {
	if len(latencies) == 0 {
		return ExtResult{Name: sc.Name, Category: sc.Category, Range: sc.Range}
	}
	sort.Float64s(latencies)
	return ExtResult{
		Name:       sc.Name,
		Category:   sc.Category,
		Range:      sc.Range,
		P50Ms:      percentile(latencies, 0.50),
		P95Ms:      percentile(latencies, 0.95),
		P99Ms:      percentile(latencies, 0.99),
		Iterations: len(latencies),
	}
}

func printExtSummary(report ExtCompareReport) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("  SUMMARY BY CATEGORY (only scenarios where BOTH systems returned data)")
	fmt.Println(strings.Repeat("=", 100))

	type catStats struct {
		lhWins, vlWins, ties, empty int
		lhTotal, vlTotal            float64
		validCount                  int
	}
	cats := map[string]*catStats{}
	catOrder := []string{}

	for _, row := range report.Comparison {
		cs, ok := cats[row.Category]
		if !ok {
			cs = &catStats{}
			cats[row.Category] = cs
			catOrder = append(catOrder, row.Category)
		}
		if !row.LHHasData || !row.VLHasData {
			cs.empty++
			continue
		}
		cs.validCount++
		cs.lhTotal += row.LHP95
		cs.vlTotal += row.VLP95
		switch row.Winner {
		case "VLH":
			cs.lhWins++
		case "VL":
			cs.vlWins++
		default:
			cs.ties++
		}
	}

	fmt.Printf("\n  %-15s %8s %8s %8s %8s %12s %12s\n", "Category", "VLH wins", "VL wins", "Ties", "Empty", "VLH avg p95", "VL avg p95")
	fmt.Println("  " + strings.Repeat("-", 80))

	totalLHW, totalVLW, totalT, totalEmpty := 0, 0, 0, 0
	for _, cat := range catOrder {
		cs := cats[cat]
		avgLH, avgVL := 0.0, 0.0
		if cs.validCount > 0 {
			avgLH = cs.lhTotal / float64(cs.validCount)
			avgVL = cs.vlTotal / float64(cs.validCount)
		}
		fmt.Printf("  %-15s %8d %8d %8d %8d %11.1fms %11.1fms\n",
			cat, cs.lhWins, cs.vlWins, cs.ties, cs.empty, avgLH, avgVL)
		totalLHW += cs.lhWins
		totalVLW += cs.vlWins
		totalT += cs.ties
		totalEmpty += cs.empty
	}
	fmt.Println("  " + strings.Repeat("-", 80))
	fmt.Printf("  %-15s %8d %8d %8d %8d\n", "TOTAL", totalLHW, totalVLW, totalT, totalEmpty)

	if totalEmpty > 0 {
		fmt.Printf("\n  WARNING: %d scenarios had one or both systems return empty data.\n", totalEmpty)
		fmt.Println("  These are EXCLUDED from win/loss tallies to avoid misleading results.")
		fmt.Println("  (A fast response to empty data is not a performance win.)")
	}
}

func (r *ExtCompareReport) WriteJSON(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
