// Package ingestmatrix is the case table of the ingest parity matrix: every
// write protocol the pinned VictoriaLogs and VictoriaTraces accept, with the
// request builders that send the same payload to hot VL/VT and to Lakehouse.
//
// The table is shared by two consumers:
//
//   - tests/e2e/ingest_matrix_test.go (build tag e2e) sends every case to the hot
//     binary and to the Lakehouse binary, compares the ingest answers, reads the
//     rows back from Lakehouse's unflushed buffer and again after the flush to
//     Parquet, and checks the ingest counters;
//   - tests/conformance/ingest_matrix_test.go is the drift gate: it derives the
//     ingest routes and listener flags from the vendored upstream trees and fails
//     when upstream gains or loses a protocol and this table does not follow, and
//     when a case has no registry row (or a row has no case).
//
// Payloads are built with upstream's own libraries wherever upstream exports
// them (logstorage.InsertRow for the native protocol, the OTLP proto types the
// upstream parsers decode); the rest are the wire formats the upstream parsers
// document, as plain text.
package ingestmatrix

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Signal is the telemetry signal a case belongs to; it decides which binary
// pair (VL/lakehouse-logs or VT/lakehouse-traces) the case runs against.
type Signal string

const (
	Logs   Signal = "logs"
	Traces Signal = "traces"
)

// Form is the tenant form a case is sent in.
type Form string

const (
	// Numeric is the upstream form: AccountID/ProjectID request headers.
	Numeric Form = "numeric"
	// Alias is the Lakehouse string form: X-Scope-OrgID naming a configured alias.
	// Hot VL/VT has no aliases, so the reference request carries the numeric
	// tenant the alias resolves to.
	Alias Form = "alias"
)

// Transport is how a case reaches the binary.
type Transport string

const (
	HTTP Transport = "http"
	TCP  Transport = "tcp"
	UDP  Transport = "udp"
	GRPC Transport = "grpc"
)

// Tenant is one tenant in both of its spellings.
type Tenant struct {
	Account, Project uint32
	// OrgID is the string alias; empty for a purely numeric tenant.
	OrgID string
}

// The two tenants the matrix writes to. NumericTenant is also the tenant the
// syslog listeners are pinned to (-syslog.tenantID.tcp/udp in the e2e compose
// file): upstream syslog has no per-message tenant. AliasTenant is the
// "acme-corp" alias of the e2e stack (-lakehouse.tenant.alias=acme-corp:1001:0).
var (
	NumericTenant = Tenant{Account: 4401, Project: 1}
	AliasTenant   = Tenant{Account: 1001, Project: 0, OrgID: "acme-corp"}
)

// TenantFor returns the tenant a form writes to.
func TenantFor(f Form) Tenant {
	if f == Alias {
		return AliasTenant
	}
	return NumericTenant
}

// Params is everything a payload builder needs.
type Params struct {
	Case   string
	Form   Form
	Tenant Tenant
	// Marker is the unique token the case's rows carry (logs: a single word in
	// _msg; traces: the trace id), so a query finds exactly this case's rows.
	Marker string
	// Base is the second-truncated time the rows are stamped around.
	Base time.Time
}

// RowTime is the timestamp of row i (1-based). Rows are one second apart and
// end at Base, so they are always inside the last minute.
func (p Params) RowTime(i, rows int) time.Time {
	return p.Base.Add(-time.Duration(rows-i) * time.Second)
}

// NewParams returns the parameters of one case run in one form. runID makes the
// marker unique per test run so reruns on a long-lived stack never see old rows.
func NewParams(c Case, f Form, runID string, base time.Time) Params {
	seed := "ingm" + runID + strings.ReplaceAll(c.ID, "_", "") + string(f)
	marker := seed
	if c.Signal == Traces {
		marker = TraceID(seed)
	}
	return Params{Case: c.ID, Form: f, Tenant: TenantFor(f), Marker: marker, Base: base.Truncate(time.Second)}
}

// ReadQuery returns the LogsQL query that finds exactly the rows of a run.
func (c Case) ReadQuery(p Params) string {
	if c.Read.Query != "" {
		return strings.ReplaceAll(c.Read.Query, "%MARKER%", p.Marker)
	}
	return p.Marker
}

// Level is the severity of row i; the three rows cover three levels so
// field-value parity is not trivially a single value.
func Level(i int) string {
	return [...]string{"INFO", "WARN", "ERROR"}[(i-1)%3]
}

// Msg is the log message of row i.
func (p Params) Msg(i int) string {
	return fmt.Sprintf("%s row %d request handled", p.Marker, i)
}

// Request is one HTTP request of a case. The harness adds the tenant headers
// (see Case.NoTenantHeaders) and the transport.
type Request struct {
	Method  string
	Path    string
	Query   string
	Header  map[string]string
	Body    []byte
	Rows    int // rows this request carries
	Comment string
}

// Case is one protocol of the matrix.
type Case struct {
	ID     string
	Signal Signal
	Title  string
	// Transport the data goes over. HTTP cases build Requests; TCP/UDP cases build
	// Lines; GRPC cases build a trace export in the harness (see GRPCExport).
	Transport Transport
	// Forms the case is run in. A case with a single form says why in FormNote.
	Forms    []Form
	FormNote string
	// Rows is the number of rows (logs) or spans (traces) one run writes.
	Rows int
	// Routes are the upstream inventory route names the case sends data to (or
	// probes), exactly as the inventory spells them. Flags are the upstream
	// listener flags the case exercises. The drift gate joins on both.
	Routes []string
	Flags  []string
	// NoTenantHeaders: the tenant travels inside the payload, not in headers.
	NoTenantHeaders bool
	// Counter is the upstream rows-ingested series that must move on hot and on
	// Lakehouse (a lower bound: other writers on a shared stack may add to it).
	Counter string
	// Build returns the HTTP requests (Transport HTTP).
	Build func(p Params) []Request
	// Lines returns the raw lines (Transport TCP/UDP): one syslog message each.
	Lines func(p Params) [][]byte
	// Rejected: upstream refuses this payload (for example OTLP/JSON logs, which
	// VictoriaLogs does not accept). The case then proves that Lakehouse refuses it
	// with the same status and body and that neither side stored a row; Rows is 0.
	Rejected bool
	// NormalizeResponse removes time-dependent parts from an ingest response body
	// before it is compared (nil: compare bytes as they are).
	NormalizeResponse func(body []byte) []byte
	// Gaps are the ids (see Gaps) of known, issue-tracked divergences from hot that
	// this cell exhibits. The e2e test applies exactly these, requires the cell to
	// observe each of them, and the registry row is expect: differ with the issues in
	// its differ_note. Deleting an id here is how a fixed gap is closed.
	Gaps []string
	// Read describes how the rows are read back; zero value means the LogsQL
	// word search for the marker in _msg.
	Read ReadSpec
}

// ReadSpec describes the read-back query of a case.
type ReadSpec struct {
	// Query is the LogsQL query ("" = the marker as a word filter on _msg).
	Query string
}

// Gap is a known divergence of Lakehouse from hot VL/VT with a tracking issue.
type Gap struct {
	ID    string
	Issue string // full issue URL
	Title string
	// AfterFlush: the divergence exists only on the Parquet read, not on the buffer read.
	AfterFlush bool
}

// Gaps lists the known divergences the matrix carries. Every entry has an issue;
// a divergence without one is a failure of the matrix, not a gap.
func Gaps() []Gap {
	return []Gap{
		{ID: "cold-read-adds-severity-number", Issue: "https://github.com/ReliablyObserve/victoria-lakehouse/issues/274", AfterFlush: true,
			Title: "after the flush Lakehouse adds severity_number=\"0\" to rows VictoriaLogs stores without it"},
		{ID: "cold-read-renames-severity-text-to-level", Issue: "https://github.com/ReliablyObserve/victoria-lakehouse/issues/331", AfterFlush: true,
			Title: "after the flush an OTLP log row comes back with level in place of severity_text"},
		{ID: "traces-default-msg-value", Issue: "https://github.com/ReliablyObserve/victoria-lakehouse/issues/332",
			Title: "spans are stored with VictoriaLogs' default _msg text instead of VictoriaTraces' \"-\""},
	}
}

// GapByID returns the gap with the given id.
func GapByID(id string) (Gap, bool) {
	for _, g := range Gaps() {
		if g.ID == id {
			return g, true
		}
	}
	return Gap{}, false
}

// RouteGap is an upstream ingest route that Lakehouse answers differently from the
// hot binary, observed rather than excluded: the matrix sends the request to both,
// requires the documented pair of statuses and fails when Lakehouse starts
// answering like hot (the gap is closed and the entry must go).
type RouteGap struct {
	ID     string
	Signal Signal
	Route  string // inventory route name
	Method string
	Path   string // request path with query
	// HotStatus and LHStatus are the statuses the two sides answer today.
	HotStatus, LHStatus int
	Issue               string
	Title               string
}

// RouteGaps lists the observed route gaps.
func RouteGaps() []RouteGap {
	return []RouteGap{
		{ID: "internal_insert", Signal: Traces, Route: "/internal/insert", Method: "POST", Path: "/internal/insert?version=v1",
			HotStatus: 200, LHStatus: 404, Issue: "https://github.com/ReliablyObserve/victoria-lakehouse/issues/334",
			Title: "/internal/insert: storage-node ingest used by a vtinsert tier (not mounted in lakehouse-traces)"},
	}
}

// RouteGapRowID is the registry row id of a route gap, e.g. "vt.ingest.internal_insert.numeric".
func RouteGapRowID(g RouteGap) string {
	return fmt.Sprintf("%s.ingest.%s.%s", surface(g.Signal), g.ID, Numeric)
}

// Probe is a non-data ingest route (readiness, compatibility stub, health):
// the matrix sends the same GET to hot and to Lakehouse and compares the
// status and body.
type Probe struct {
	Signal Signal
	Route  string // inventory route name
	Method string
	Path   string
	Header map[string]string
}

// Exclusion is an upstream ingest route or listener the matrix deliberately
// does not send data to, with the reason. The drift gate requires a reason and
// deletes the entry when upstream stops having the route.
type Exclusion struct {
	Signal Signal
	Name   string // "route:/internal/insert" or "flag:syslog.listenAddr.unix"
	Reason string
}

// AllCases returns the case table: logs first, then traces, each in a stable order.
func AllCases() []Case {
	out := append([]Case(nil), logsCases()...)
	out = append(out, tracesCases()...)
	return out
}

// CasesFor returns the cases of one signal.
func CasesFor(s Signal) []Case {
	var out []Case
	for _, c := range AllCases() {
		if c.Signal == s {
			out = append(out, c)
		}
	}
	return out
}

// RowID returns the registry row id of a case in a form, e.g.
// "vl.ingest.jsonline.numeric".
func RowID(c Case, f Form) string {
	return fmt.Sprintf("%s.ingest.%s.%s", surface(c.Signal), c.ID, f)
}

// RowIDs returns every registry row id the table requires, sorted.
func RowIDs() []string {
	var ids []string
	for _, c := range AllCases() {
		for _, f := range c.Forms {
			ids = append(ids, RowID(c, f))
		}
	}
	for _, g := range RouteGaps() {
		ids = append(ids, RouteGapRowID(g))
	}
	sort.Strings(ids)
	return ids
}

func surface(s Signal) string {
	if s == Traces {
		return "vt"
	}
	return "vl"
}

// Surface is the registry surface ("vl" or "vt") of a signal.
func Surface(s Signal) string { return surface(s) }

// Exercised returns the union of routes and flags the cases and probes send to,
// as "route:<name>" / "flag:<name>" keys, for one signal.
func Exercised(s Signal) map[string]string {
	m := map[string]string{}
	for _, c := range CasesFor(s) {
		for _, r := range c.Routes {
			m["route:"+r] = c.ID
		}
		for _, f := range c.Flags {
			m["flag:"+f] = c.ID
		}
	}
	for _, g := range RouteGaps() {
		if g.Signal == s {
			m["route:"+g.Route] = "route gap " + g.ID
		}
	}
	for _, p := range Probes() {
		if p.Signal == s {
			m["route:"+p.Route] = "probe"
		}
	}
	return m
}
