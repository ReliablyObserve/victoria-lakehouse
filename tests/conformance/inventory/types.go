// Package inventory extracts the upstream VictoriaLogs / VictoriaTraces feature
// surface (HTTP routes, LogsQL pipes/filters/stats, TraceQL functions, flags)
// from the vendored sources so the registry can be checked for drift.
package inventory

type Item struct {
	Kind string `yaml:"kind"` // route | pipe | filter | stats | traceql | flag
	// Surface is which upstream binary this item comes from: "vl"
	// (VictoriaLogs) or "vt" (VictoriaTraces). Both binaries can register a
	// route (or, less often, a flag) under the same kind+name independently
	// — e.g. VL and VT each have their own /insert/native — so Surface is
	// part of the item's identity: it is never collapsed across surfaces.
	Surface string `yaml:"surface"`
	Name    string `yaml:"name"`             // e.g. /select/logsql/query, coalesce, search.maxTraces. For prefix registrations the name is the exact HasPrefix literal, including a trailing slash (/insert/loki/); compound HasPrefix(...) && HasSuffix(...) registrations keep the prefix literal only.
	Source  string `yaml:"source"`           // upstream file path relative to the deps dir
	Linked  bool   `yaml:"linked,omitempty"` // flags only: package is linked into an LH binary
}

type Inventory struct {
	VLVersion string `yaml:"vl_version"`
	VTVersion string `yaml:"vt_version"`
	// VLCommitTraces is the VL_COMMIT_TRACES pin (the traces module's own,
	// separate VictoriaLogs vendor commit); see Dirs.VLCommitTraces.
	VLCommitTraces string `yaml:"vl_commit_traces,omitempty"`
	// Protocol records the /internal/select/* and /internal/delete/* peer
	// protocol versions each module expects, read from that module's own
	// vendored VictoriaLogs tree. They are what the registry's
	// {{proto.internal_select}} / {{proto.internal_delete}} placeholders
	// resolve to, and they are NOT in lockstep across the two pins — so a
	// bump that moves either one shows up here as a diff instead of as a
	// runtime "unexpected protocol version" rejection between peers.
	Protocol Protocols `yaml:"protocol,omitempty"`
	Items    []Item    `yaml:"items"`
}

// Key returns "<surface>:<kind>:<name>", the identity used to join with
// registry rows. Surface is part of the key (not just Kind+Name) so the
// same kind+name registered independently by both VL and VT — e.g.
// route:/insert/native — is tracked as two distinct items, one per surface.
func (inv *Inventory) Key(i Item) string { return i.Surface + ":" + i.Kind + ":" + i.Name }
