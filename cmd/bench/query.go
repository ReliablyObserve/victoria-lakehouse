package main

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"time"
)

type queryDef struct {
	endpoint string
	filter   string
	urlFn    func(base string) string
}

func logsQueries() []queryDef {
	return []queryDef{
		{
			endpoint: "/select/logsql/hits",
			filter:   "*",
			urlFn:    func(base string) string { return base + "/select/logsql/hits?query=*&step=60s" },
		},
		{
			endpoint: "/select/logsql/hits",
			filter:   `trace_id:="0000000000000001"`,
			urlFn: func(base string) string {
				return base + "/select/logsql/hits?query=" + url.QueryEscape(`trace_id:="0000000000000001"`) + "&step=60s"
			},
		},
		{
			endpoint: "/select/logsql/query",
			filter:   `service.name:="api-gateway"`,
			urlFn: func(base string) string {
				return base + "/select/logsql/query?query=" + url.QueryEscape(`service.name:="api-gateway"`) + "&limit=100"
			},
		},
		{
			endpoint: "/select/logsql/field_names",
			filter:   "none",
			urlFn:    func(base string) string { return base + "/select/logsql/field_names" },
		},
		{
			endpoint: "/select/logsql/field_values",
			filter:   "service.name",
			urlFn: func(base string) string {
				return base + "/select/logsql/field_values?field=" + url.QueryEscape("service.name")
			},
		},
		{
			endpoint: "/select/logsql/stats_query_range",
			filter:   "* | count() by (service.name)",
			urlFn: func(base string) string {
				return base + "/select/logsql/stats_query_range?query=" + url.QueryEscape("* | count() by (service.name)") + "&step=60s"
			},
		},
	}
}

func benchmarkQueries(endpoint, signal string, runs int) []ReadResult {
	queries := logsQueries()
	var results []ReadResult
	client := &http.Client{Timeout: 60 * time.Second}
	for _, q := range queries {
		log.Printf("  Benchmarking %s %s...", q.endpoint, q.filter)
		cold := benchmarkSingleQuery(client, q.urlFn(endpoint), runs)
		warm := benchmarkSingleQuery(client, q.urlFn(endpoint), runs)
		hot := benchmarkSingleQuery(client, q.urlFn(endpoint), runs)
		results = append(results, ReadResult{
			Endpoint: q.endpoint,
			Filter:   q.filter,
			ColdMs:   cold,
			WarmMs:   warm,
			HotMs:    hot,
		})
	}
	return results
}

func benchmarkSingleQuery(client *http.Client, rawURL string, runs int) float64 {
	var latencies []float64
	for i := 0; i < runs; i++ {
		start := time.Now()
		resp, err := client.Get(rawURL)
		elapsed := time.Since(start)
		if err != nil {
			log.Printf("    query error: %v", err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		latencies = append(latencies, float64(elapsed.Milliseconds()))
	}
	if len(latencies) == 0 {
		return 0
	}
	sort.Float64s(latencies)
	return latencies[len(latencies)/2]
}
