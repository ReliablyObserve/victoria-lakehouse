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

// LogDedicatedResourceKeys / LogDedicatedLogKeys partition the Tier-1 log
// columns by their source map so the emission paths can skip the promoted key
// in exactly the right map (resource.attributes vs log.attributes) and avoid
// double-emitting it from the catch-all map.
func LogDedicatedResourceKeys() map[string]bool {
	return dedicatedKeysFor(logDedicatedColumns, "resource.attributes")
}
func LogDedicatedLogKeys() map[string]bool {
	return dedicatedKeysFor(logDedicatedColumns, "log.attributes")
}

// TraceDedicatedResourceKeys / TraceDedicatedSpanKeys partition the Tier-1 trace
// columns by source map (resource.attributes vs span.attributes).
func TraceDedicatedResourceKeys() map[string]bool {
	return dedicatedKeysFor(traceDedicatedColumns, "resource.attributes")
}
func TraceDedicatedSpanKeys() map[string]bool {
	return dedicatedKeysFor(traceDedicatedColumns, "span.attributes")
}

func dedicatedKeysFor(cols []FieldMapping, mapCol string) map[string]bool {
	out := make(map[string]bool)
	for _, m := range cols {
		if m.MapColumn == mapCol {
			out[m.ParquetColumn] = true
		}
	}
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
