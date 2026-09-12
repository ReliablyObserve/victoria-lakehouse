// Package inventory extracts the upstream VictoriaLogs / VictoriaTraces feature
// surface (HTTP routes, LogsQL pipes/filters/stats, TraceQL functions, flags)
// from the vendored sources so the registry can be checked for drift.
package inventory

type Item struct {
	Kind   string `yaml:"kind"`             // route | pipe | filter | stats | traceql | flag
	Name   string `yaml:"name"`             // e.g. /select/logsql/query, coalesce, search.maxTraces. For prefix registrations the name is the exact HasPrefix literal, including a trailing slash (/insert/loki/); compound HasPrefix(...) && HasSuffix(...) registrations keep the prefix literal only.
	Source string `yaml:"source"`           // upstream file path relative to the deps dir
	Linked bool   `yaml:"linked,omitempty"` // flags only: package is linked into an LH binary
}

type Inventory struct {
	VLVersion string `yaml:"vl_version"`
	VTVersion string `yaml:"vt_version"`
	Items     []Item `yaml:"items"`
}

func (inv *Inventory) Key(i Item) string { return i.Kind + ":" + i.Name }
