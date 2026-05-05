package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func runThroughputTests(target string, durationStr string) map[string]*ThroughputResult {
	dur, _ := time.ParseDuration(durationStr)
	if dur == 0 {
		dur = 60 * time.Second
	}

	results := make(map[string]*ThroughputResult)
	results["max_insert_rate"] = benchInsertThroughput(target, dur)
	results["max_query_qps"] = benchQueryThroughput(target, dur)
	return results
}

func benchInsertThroughput(target string, dur time.Duration) *ThroughputResult {
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	var bestRate float64
	var bestConc int

	for _, conc := range concurrencyLevels {
		rate := measureInsertRate(target, conc, dur/time.Duration(len(concurrencyLevels)))
		if rate > bestRate {
			bestRate = rate
			bestConc = conc
		}
		if rate < bestRate*0.8 {
			break
		}
	}

	return &ThroughputResult{
		MaxRate:        bestRate,
		ConcurrencyMax: bestConc,
		Unit:           "rows/s",
	}
}

func measureInsertRate(target string, concurrency int, dur time.Duration) float64 {
	var totalRows atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	line := `{"_time":"2026-05-02T10:00:00Z","_msg":"load test","service.name":"loadtest","level":"info"}` + "\n"
	batch := ""
	for i := 0; i < 100; i++ {
		batch += line
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Post(
					target+"/insert/jsonline",
					"application/x-ndjson",
					bytes.NewReader([]byte(batch)),
				)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 300 {
					totalRows.Add(100)
				}
			}
		}()
	}
	wg.Wait()

	elapsed := dur.Seconds()
	return float64(totalRows.Load()) / elapsed
}

func benchQueryThroughput(target string, dur time.Duration) *ThroughputResult {
	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	var bestQPS float64
	var bestConc int

	for _, conc := range concurrencyLevels {
		qps := measureQueryQPS(target, conc, dur/time.Duration(len(concurrencyLevels)))
		if qps > bestQPS {
			bestQPS = qps
			bestConc = conc
		}
		if qps < bestQPS*0.8 {
			break
		}
	}

	return &ThroughputResult{
		MaxRate:        bestQPS,
		ConcurrencyMax: bestConc,
		Unit:           "qps",
	}
}

func measureQueryQPS(target string, concurrency int, dur time.Duration) float64 {
	var totalQueries atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	queryURL := fmt.Sprintf("%s/select/logsql/query?query=*&limit=10", target)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Get(queryURL)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				totalQueries.Add(1)
			}
		}()
	}
	wg.Wait()

	elapsed := dur.Seconds()
	return float64(totalQueries.Load()) / elapsed
}

func runMixedWorkload(target string, durationStr string) *ThroughputResult {
	dur, _ := time.ParseDuration(durationStr)
	if dur == 0 {
		dur = 60 * time.Second
	}

	var totalOps atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	line := `{"_time":"2026-05-02T10:00:00Z","_msg":"mixed test","service.name":"loadtest"}` + "\n"
	batch := ""
	for i := 0; i < 50; i++ {
		batch += line
	}

	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Post(target+"/insert/jsonline", "application/x-ndjson", bytes.NewReader([]byte(batch)))
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				totalOps.Add(1)
			}
		}()
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			queryURL := fmt.Sprintf("%s/select/logsql/query?query=*&limit=10", target)
			for time.Now().Before(deadline) {
				resp, err := client.Get(queryURL)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				totalOps.Add(1)
			}
		}()
	}
	wg.Wait()

	return &ThroughputResult{
		MaxRate:        float64(totalOps.Load()) / dur.Seconds(),
		ConcurrencyMax: 10,
		Unit:           "ops/s",
	}
}
