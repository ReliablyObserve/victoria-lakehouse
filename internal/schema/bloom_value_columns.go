package schema

// This file is the SINGLE source of truth for which promoted columns get their
// distinct per-file VALUES extracted — the feed for the partition bloom index
// (_bloom.bin), which prunes files at query time. It must equal the schema's
// HasBloom set (LogBloomColumns()/TraceBloomColumns()) EXCEPT for the documented
// high-card exclusions. TestBloomValueColumns_MatchSchema enforces that: a HasBloom
// column can't silently lack a value accessor (the bug where only trace_id +
// service.name were extracted, so the other ~17 bloomed columns had Parquet
// row-group blooms but no _bloom.bin entry and could not prune at the file level).
//
// EXCLUSION — span_id: unique per span, so its per-file distinct set is the whole
// column; a _bloom.bin entry costs ~1.25 B/span (~1.25 TB/PB), the single largest
// line item, for a span-only point lookup that is rare (trace assembly fetches by
// trace_id). Its Parquet row-group bloom still covers that lookup; its distinct
// COUNT still survives restart via the tap-fed catalog HLL. url.full is the next
// highest-card bloom (can approach unique-per-request) — if its _bloom.bin cost is
// not justified by url point lookups, move it to bloomValueExclusions (one line).
//
// Slot (Tier-2) blooms (ded_sNN) are dynamic — resolved per file and bloomed by the
// writer directly — so this STATIC SoT covers Tier-1 + legacy promoted columns only;
// the drift test compares against the no-extra-slots base set.

// LogBloomValueColumn pairs a bloom column name with an accessor for its value.
type LogBloomValueColumn struct {
	Name string
	Get  func(*LogRow) string
}

// TraceBloomValueColumn is the traces counterpart.
type TraceBloomValueColumn struct {
	Name string
	Get  func(*TraceRow) string
}

// LogBloomValueColumns is the bloom-value extraction set for logs — every HasBloom
// promoted column except the documented exclusions (span_id).
var LogBloomValueColumns = []LogBloomValueColumn{
	{"trace_id", func(r *LogRow) string { return r.TraceID }},
	{"container.id", func(r *LogRow) string { return r.ContainerID }},
	{"service.instance.id", func(r *LogRow) string { return r.ServiceInstanceID }},
	{"service.name", func(r *LogRow) string { return r.ServiceName }},
	{"service.version", func(r *LogRow) string { return r.ServiceVersion }},
	{"host.name", func(r *LogRow) string { return r.HostName }},
	{"k8s.pod.name", func(r *LogRow) string { return r.K8sPodName }},
	{"k8s.node.name", func(r *LogRow) string { return r.K8sNodeName }},
	{"exception.type", func(r *LogRow) string { return r.ExceptionType }},
}

// TraceBloomValueColumns is the bloom-value extraction set for traces — every
// HasBloom promoted column except the documented exclusions (span_id).
var TraceBloomValueColumns = []TraceBloomValueColumn{
	{"trace_id", func(r *TraceRow) string { return r.TraceID }},
	{"span.name", func(r *TraceRow) string { return r.SpanName }},
	{"service.name", func(r *TraceRow) string { return r.ServiceName }},
	{"service.instance.id", func(r *TraceRow) string { return r.ServiceInstanceID }},
	{"container.id", func(r *TraceRow) string { return r.ContainerID }},
	{"host.name", func(r *TraceRow) string { return r.HostName }},
	{"k8s.pod.name", func(r *TraceRow) string { return r.K8sPodName }},
	{"k8s.node.name", func(r *TraceRow) string { return r.K8sNodeName }},
	{"url.full", func(r *TraceRow) string { return r.URLFull }},
	{"client.address", func(r *TraceRow) string { return r.ClientAddress }},
	{"server.address", func(r *TraceRow) string { return r.ServerAddress }},
	{"network.peer.address", func(r *TraceRow) string { return r.NetworkPeerAddress }},
	{"db.collection.name", func(r *TraceRow) string { return r.DBCollectionName }},
	{"db.operation.name", func(r *TraceRow) string { return r.DBOperationName }},
	{"rpc.method", func(r *TraceRow) string { return r.RPCMethod }},
	{"messaging.destination.name", func(r *TraceRow) string { return r.MessagingDestination }},
	{"code.function.name", func(r *TraceRow) string { return r.CodeFunctionName }},
	{"exception.type", func(r *TraceRow) string { return r.ExceptionType }},
}
