package main

import (
	"encoding/json"
	mrand "math/rand"
	"os"
	"sort"
)

// The manifest is the writer-side truth of a datagen run: it is computed from the rows
// the generator built, before any of them is sent, so it does not depend on how (or
// whether) Lakehouse stored them. The reader matrix compares Lakehouse's own answers and
// every external engine's answers to it, which is what makes a defect that Lakehouse
// reads back identically (a dropped map key, truncated timestamps, rows filed under the
// wrong tenant) visible.

type manifestSignal struct {
	Count       int            `json:"count"`
	ByService   map[string]int `json:"by_service"`
	Errors      int            `json:"errors"`       // severity_text = ERROR (logs) / status.code = 2 (traces)
	FieldFilter int            `json:"field_filter"` // the same, restricted to service.name = api-gateway
	MapColumn   string         `json:"map_column"`
	MapKeys     map[string]int `json:"map_keys"`   // rows carrying each key of the map column
	MapFilter   int            `json:"map_filter"` // rows with format=nginx (logs) / rpc.system=grpc (traces)
	TsMin       int64          `json:"ts_min"`
	TsMax       int64          `json:"ts_max"`
	Timestamps  []int64        `json:"timestamps"` // every row's timestamp_unix_nano, ascending
	// TraceCounts is the rows per trace ID: spans per trace for traces, correlated log rows for logs.
	TraceCounts map[string]int `json:"trace_counts"`
}

type manifestFile struct {
	Name    string         `json:"name"`
	Account string         `json:"account_id"`
	Project string         `json:"project_id"`
	OrgID   string         `json:"org_id,omitempty"`
	Seed    int64          `json:"seed"`
	Logs    manifestSignal `json:"logs"`
	Traces  manifestSignal `json:"traces"`
}

func newSignal(mapColumn string) manifestSignal {
	return manifestSignal{ByService: map[string]int{}, MapColumn: mapColumn, MapKeys: map[string]int{}, TraceCounts: map[string]int{}}
}

func (m *manifestSignal) finish() {
	sort.Slice(m.Timestamps, func(i, j int) bool { return m.Timestamps[i] < m.Timestamps[j] })
	if len(m.Timestamps) > 0 {
		m.TsMin, m.TsMax = m.Timestamps[0], m.Timestamps[len(m.Timestamps)-1]
	}
}

func buildManifest(name, account, project, org string, seed int64, logs []logRow, spans []traceRow) manifestFile {
	f := manifestFile{Name: name, Account: account, Project: project, OrgID: org, Seed: seed,
		Logs: newSignal("log.attributes"), Traces: newSignal("span.attributes")}
	for _, r := range logs {
		m := &f.Logs
		m.Count++
		m.ByService[r.ServiceName]++
		if r.SeverityText == "ERROR" {
			m.Errors++
			if r.ServiceName == "api-gateway" {
				m.FieldFilter++
			}
		}
		for k, v := range r.LogAttrs {
			m.MapKeys[k]++
			if k == "format" && v == "nginx" {
				m.MapFilter++
			}
		}
		m.Timestamps = append(m.Timestamps, r.TimestampUnixNano)
		if r.TraceID != "" {
			m.TraceCounts[r.TraceID]++
		}
	}
	for _, r := range spans {
		m := &f.Traces
		m.Count++
		m.ByService[r.ServiceName]++
		if r.StatusCode == 2 {
			m.Errors++
			if r.ServiceName == "api-gateway" {
				m.FieldFilter++
			}
		}
		for k, v := range r.SpanAttrs {
			m.MapKeys[k]++
			if k == "rpc.system" && v == "grpc" {
				m.MapFilter++
			}
		}
		m.Timestamps = append(m.Timestamps, r.TimestampUnixNano)
		m.TraceCounts[r.TraceID]++
	}
	f.Logs.finish()
	f.Traces.finish()
	return f
}

func writeManifest(path, name, account, project, org string, seed int64, logs []logRow, spans []traceRow) error {
	b, err := json.Marshal(buildManifest(name, account, project, org, seed, logs, spans))
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644) // #nosec G306 -- test artefact
}

func newSeededRand(seed int64) *mrand.Rand {
	return mrand.New(mrand.NewSource(seed)) // #nosec G404 -- synthetic test data
}
