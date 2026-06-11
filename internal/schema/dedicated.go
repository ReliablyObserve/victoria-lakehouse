package schema

import (
	"encoding/json"
	"sort"
)

// init folds the Tier-1 strict OTel columns into the two signal profiles so
// every registry-driven read path (projection, filter pushdown, the columnar
// reader, parquetRowToFields) picks them up automatically. Runs before any
// NewRegistry call (package init ordering), so byInternal/byParquet are built
// with the dedicated columns already present. Tier-2 slots are NOT registered
// here — their column->name binding is per-file (footer KV), resolved at read
// time, not via this static registry.
func init() {
	LogsProfile.Promoted = append(LogsProfile.Promoted, logDedicatedColumns...)
	TracesProfile.Promoted = append(TracesProfile.Promoted, traceDedicatedColumns...)
}

// This file is the single source of truth for the Tier-1 strict OTel dedicated
// columns and the Tier-2 custom config-driven slots. The promoted FieldMappings
// here are appended into LogsProfile/TracesProfile (registry.go) so every
// registry-driven path — projection, filter pushdown, the columnar reader,
// parquetRowToFields — picks them up automatically. The bloom set is derived
// from the HasBloom flag (Tier 1) plus the operator's per-attribute bloom
// choice (Tier 2). Naming: every dedicated column emits under its BARE OTel
// field name (InternalName == ParquetColumn) for both signals, identical to how
// service.name/k8s.pod.name already surface — that is the VL/VT compatibility
// invariant (the read path resolves the same field name whether the value lives
// in a map or a column).

// logDedicatedColumns are the strict Tier-1 OTel columns promoted out of the
// logs maps. Bloom HIGH-card id-like (container.id, service.instance.id) +
// service.version/exception.type by class; dict-only (no bloom) for the
// low-card resource descriptors and for exception.message (large text, rarely
// equality-matched).
var logDedicatedColumns = []FieldMapping{
	{ParquetColumn: "container.id", InternalName: "container.id", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes", HasBloom: true},
	{ParquetColumn: "service.instance.id", InternalName: "service.instance.id", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes", HasBloom: true},
	{ParquetColumn: "service.version", InternalName: "service.version", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes", HasBloom: true},
	{ParquetColumn: "exception.type", InternalName: "exception.type", Type: TypeString, Origin: OriginPromoted, MapColumn: "log.attributes", HasBloom: true},
	{ParquetColumn: "exception.message", InternalName: "exception.message", Type: TypeString, Origin: OriginPromoted, MapColumn: "log.attributes"},
	{ParquetColumn: "k8s.cluster.name", InternalName: "k8s.cluster.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "telemetry.sdk.name", InternalName: "telemetry.sdk.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "telemetry.sdk.language", InternalName: "telemetry.sdk.language", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "telemetry.sdk.version", InternalName: "telemetry.sdk.version", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "cloud.account.id", InternalName: "cloud.account.id", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "cloud.provider", InternalName: "cloud.provider", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "os.type", InternalName: "os.type", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "host.arch", InternalName: "host.arch", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "process.runtime.name", InternalName: "process.runtime.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "process.runtime.version", InternalName: "process.runtime.version", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
}

// traceDedicatedColumns are the strict Tier-1 OTel columns promoted out of the
// traces maps. High-card per-request identifiers (url.full, client.address,
// network.peer.address, container.id, service.instance.id) -> plain + bloom;
// selective lookup keys (server.address, db.collection.name, db.operation.name,
// rpc.method, messaging.destination.name, code.function.name, exception.type)
// -> dict + bloom; low-card resource descriptors -> dict, no bloom.
// db.query.text -> dict, NEVER bloom (huge unique SQL).
var traceDedicatedColumns = []FieldMapping{
	{ParquetColumn: "url.full", InternalName: "span_attr:url.full", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "client.address", InternalName: "span_attr:client.address", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "server.address", InternalName: "span_attr:server.address", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "network.peer.address", InternalName: "span_attr:network.peer.address", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "db.collection.name", InternalName: "span_attr:db.collection.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "db.operation.name", InternalName: "span_attr:db.operation.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "rpc.method", InternalName: "span_attr:rpc.method", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "messaging.destination.name", InternalName: "span_attr:messaging.destination.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "code.function.name", InternalName: "span_attr:code.function.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "exception.type", InternalName: "span_attr:exception.type", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes", HasBloom: true},
	{ParquetColumn: "container.id", InternalName: "resource_attr:container.id", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes", HasBloom: true},
	{ParquetColumn: "service.instance.id", InternalName: "resource_attr:service.instance.id", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes", HasBloom: true},
	{ParquetColumn: "k8s.cluster.name", InternalName: "resource_attr:k8s.cluster.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "telemetry.sdk.name", InternalName: "resource_attr:telemetry.sdk.name", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "cloud.account.id", InternalName: "resource_attr:cloud.account.id", Type: TypeString, Origin: OriginPromoted, MapColumn: "resource.attributes"},
	{ParquetColumn: "db.query.text", InternalName: "span_attr:db.query.text", Type: TypeString, Origin: OriginPromoted, MapColumn: "span.attributes"},
}

// LogDedicatedColumns returns the Tier-1 strict OTel log columns (a copy, so
// callers can't mutate the registry source of truth).
func LogDedicatedColumns() []FieldMapping {
	out := make([]FieldMapping, len(logDedicatedColumns))
	copy(out, logDedicatedColumns)
	return out
}

// TraceDedicatedColumns returns the Tier-1 strict OTel trace columns (a copy).
func TraceDedicatedColumns() []FieldMapping {
	out := make([]FieldMapping, len(traceDedicatedColumns))
	copy(out, traceDedicatedColumns)
	return out
}

// LogBloomColumns is the strict set of log columns that get a Parquet bloom
// filter, derived from the Tier-1 HasBloom flags plus the always-on legacy
// blooms (service.name, trace_id). extraSlotBlooms carries the Tier-2 slot
// columns the operator marked bloom:true (already resolved to ded_sNN names).
// Returned sorted + de-duplicated for deterministic writer output.
func LogBloomColumns(extraSlotBlooms ...string) []string {
	return bloomColumnsFrom(LogsProfile.Promoted, []string{"service.name", "trace_id"}, extraSlotBlooms)
}

// TraceBloomColumns is the strict bloom set for traces (Tier-1 HasBloom flags +
// legacy service.name/trace_id + Tier-2 slot blooms).
func TraceBloomColumns(extraSlotBlooms ...string) []string {
	return bloomColumnsFrom(TracesProfile.Promoted, []string{"service.name", "trace_id"}, extraSlotBlooms)
}

func bloomColumnsFrom(promoted []FieldMapping, base, extra []string) []string {
	set := make(map[string]bool)
	for _, c := range base {
		set[c] = true
	}
	for _, m := range promoted {
		if m.HasBloom {
			set[m.ParquetColumn] = true
		}
	}
	for _, c := range extra {
		if c != "" {
			set[c] = true
		}
	}
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// SlotMapping is the per-file {slotColumn -> configuredAttrName} association,
// e.g. {"ded_s01":"tenant_id"}. Serialized to the Parquet footer KV under
// DedicatedSlotsMetaKey and rehydrated by the read paths.
type SlotMapping map[string]string

// MarshalSlotMapping serializes a slot mapping to canonical JSON for the footer
// KV. Returns nil for an empty mapping so the writer can skip the KV entirely.
func MarshalSlotMapping(m SlotMapping) []byte {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

// UnmarshalSlotMapping parses the footer-KV JSON back into a SlotMapping.
// Returns nil on empty/garbage so the read path falls back to bare slot names
// (which simply won't match user queries — safe, never panics).
func UnmarshalSlotMapping(data []byte) SlotMapping {
	if len(data) == 0 {
		return nil
	}
	var m SlotMapping
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// SlotAttr is one operator-configured custom attribute promotion (Tier 2),
// decoupled from the config package to avoid an import cycle.
type SlotAttr struct {
	Name  string
	Bloom bool
}

// SlotResolver binds operator-configured custom attribute names to the fixed
// spare slot columns (ded_s01..ded_sNN) deterministically, in config order, up
// to DedicatedSlotCount. It is the runtime half of Tier 2: ingest routes a
// configured key to its slot; the writer records the binding in the footer KV;
// the read path remaps the slot column back to the configured name. A nil
// *SlotResolver is valid and inert (no custom promotions) — all methods are
// nil-safe so callers need no guards.
type SlotResolver struct {
	nameToSlot map[string]string // attr name  -> ded_sNN
	slotToName map[string]string // ded_sNN    -> attr name
	bloomSlots []string          // ded_sNN columns that requested a bloom
}

// NewSlotResolver assigns up to DedicatedSlotCount attributes to slots in the
// given order. Empty names and duplicates are skipped; excess beyond the slot
// count is dropped (callers should warn). Returns nil for an empty input so the
// common no-custom-attributes case allocates nothing.
func NewSlotResolver(attrs []SlotAttr) *SlotResolver {
	r := &SlotResolver{
		nameToSlot: make(map[string]string),
		slotToName: make(map[string]string),
	}
	next := 0
	for _, a := range attrs {
		if a.Name == "" || r.nameToSlot[a.Name] != "" {
			continue
		}
		if next >= len(DedicatedSlotColumns) {
			break
		}
		slot := DedicatedSlotColumns[next]
		next++
		r.nameToSlot[a.Name] = slot
		r.slotToName[slot] = a.Name
		if a.Bloom {
			r.bloomSlots = append(r.bloomSlots, slot)
		}
	}
	if next == 0 {
		return nil
	}
	return r
}

// SlotForName returns the slot column a configured attribute routes to.
func (r *SlotResolver) SlotForName(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	s, ok := r.nameToSlot[name]
	return s, ok
}

// NameForSlot returns the configured attribute name bound to a slot column.
func (r *SlotResolver) NameForSlot(slot string) (string, bool) {
	if r == nil {
		return "", false
	}
	n, ok := r.slotToName[slot]
	return n, ok
}

// BloomSlots returns the slot columns the operator marked bloom:true.
func (r *SlotResolver) BloomSlots() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.bloomSlots...)
}

// Mapping returns the slot→name binding for the Parquet footer KV.
func (r *SlotResolver) Mapping() SlotMapping {
	if r == nil || len(r.slotToName) == 0 {
		return nil
	}
	m := make(SlotMapping, len(r.slotToName))
	for slot, name := range r.slotToName {
		m[slot] = name
	}
	return m
}

// SetLogSlot writes value into the LogRow spare slot column named slot
// (ded_s01..ded_sNN). Unknown slot names are ignored. Used by ingest after the
// SlotResolver maps a configured custom attribute to its slot.
func SetLogSlot(row *LogRow, slot, value string) {
	switch slot {
	case "ded_s01":
		row.DedS01 = value
	case "ded_s02":
		row.DedS02 = value
	case "ded_s03":
		row.DedS03 = value
	case "ded_s04":
		row.DedS04 = value
	case "ded_s05":
		row.DedS05 = value
	case "ded_s06":
		row.DedS06 = value
	case "ded_s07":
		row.DedS07 = value
	case "ded_s08":
		row.DedS08 = value
	}
}

// LogSlotValue reads the value of a LogRow spare slot column (for read-path
// remap). Returns "" for unknown/empty slots.
func LogSlotValue(row *LogRow, slot string) string {
	switch slot {
	case "ded_s01":
		return row.DedS01
	case "ded_s02":
		return row.DedS02
	case "ded_s03":
		return row.DedS03
	case "ded_s04":
		return row.DedS04
	case "ded_s05":
		return row.DedS05
	case "ded_s06":
		return row.DedS06
	case "ded_s07":
		return row.DedS07
	case "ded_s08":
		return row.DedS08
	}
	return ""
}

// SetTraceSlot / TraceSlotValue mirror the LogRow helpers for TraceRow.
func SetTraceSlot(row *TraceRow, slot, value string) {
	switch slot {
	case "ded_s01":
		row.DedS01 = value
	case "ded_s02":
		row.DedS02 = value
	case "ded_s03":
		row.DedS03 = value
	case "ded_s04":
		row.DedS04 = value
	case "ded_s05":
		row.DedS05 = value
	case "ded_s06":
		row.DedS06 = value
	case "ded_s07":
		row.DedS07 = value
	case "ded_s08":
		row.DedS08 = value
	}
}

func TraceSlotValue(row *TraceRow, slot string) string {
	switch slot {
	case "ded_s01":
		return row.DedS01
	case "ded_s02":
		return row.DedS02
	case "ded_s03":
		return row.DedS03
	case "ded_s04":
		return row.DedS04
	case "ded_s05":
		return row.DedS05
	case "ded_s06":
		return row.DedS06
	case "ded_s07":
		return row.DedS07
	case "ded_s08":
		return row.DedS08
	}
	return ""
}
