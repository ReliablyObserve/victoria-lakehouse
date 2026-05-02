package schema

import (
	"strings"
)

type FieldOrigin int

const (
	OriginPromoted     FieldOrigin = iota // top-level Parquet column
	OriginResourceMap                     // inside resource.attributes MAP
	OriginLogAttrMap                      // inside log.attributes MAP
	OriginSpanAttrMap                     // inside span.attributes MAP
	OriginScopeAttrMap                    // inside scope.attributes MAP
)

type FieldMapping struct {
	ParquetColumn string
	InternalName  string
	Origin        FieldOrigin
	MapColumn     string // parent MAP column name when Origin != OriginPromoted
	MapKey        string // key inside the MAP
	HasBloom      bool
}

type Profile struct {
	Promoted     []FieldMapping
	MapColumns   []string // MAP column names to scan for unknown fields
	StreamFields []string // fields that define a stream identity
}

type Registry struct {
	profile    Profile
	byInternal map[string]*FieldMapping
	byParquet  map[string]*FieldMapping
}

var LogsProfile = Profile{
	Promoted: []FieldMapping{
		{ParquetColumn: "timestamp_unix_nano", InternalName: "_time", Origin: OriginPromoted},
		{ParquetColumn: "body", InternalName: "_msg", Origin: OriginPromoted},
		{ParquetColumn: "severity_text", InternalName: "level", Origin: OriginPromoted},
		{ParquetColumn: "severity_number", InternalName: "severity_number", Origin: OriginPromoted},
		{ParquetColumn: "service.name", InternalName: "service.name", Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "k8s.namespace.name", InternalName: "k8s.namespace.name", Origin: OriginPromoted},
		{ParquetColumn: "k8s.pod.name", InternalName: "k8s.pod.name", Origin: OriginPromoted},
		{ParquetColumn: "trace_id", InternalName: "trace_id", Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span_id", InternalName: "span_id", Origin: OriginPromoted},
		{ParquetColumn: "_stream", InternalName: "_stream", Origin: OriginPromoted},
		{ParquetColumn: "_stream_id", InternalName: "_stream_id", Origin: OriginPromoted},
		{ParquetColumn: "scope.name", InternalName: "scope.name", Origin: OriginPromoted},
	},
	MapColumns:   []string{"resource.attributes", "log.attributes"},
	StreamFields: []string{"service.name", "k8s.namespace.name", "k8s.pod.name"},
}

var TracesProfile = Profile{
	Promoted: []FieldMapping{
		{ParquetColumn: "timestamp_unix_nano", InternalName: "_time", Origin: OriginPromoted},
		{ParquetColumn: "start_time_unix_nano", InternalName: "start_time_unix_nano", Origin: OriginPromoted},
		{ParquetColumn: "trace_id", InternalName: "trace_id", Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span_id", InternalName: "span_id", Origin: OriginPromoted},
		{ParquetColumn: "parent_span_id", InternalName: "parent_span_id", Origin: OriginPromoted},
		{ParquetColumn: "span.name", InternalName: "name", Origin: OriginPromoted},
		{ParquetColumn: "span.kind", InternalName: "kind", Origin: OriginPromoted},
		{ParquetColumn: "status.code", InternalName: "status_code", Origin: OriginPromoted},
		{ParquetColumn: "status.message", InternalName: "status_message", Origin: OriginPromoted},
		{ParquetColumn: "duration_ns", InternalName: "duration", Origin: OriginPromoted},
		{ParquetColumn: "service.name", InternalName: "resource_attr:service.name", Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "scope.name", InternalName: "scope_attr:otel.library.name", Origin: OriginPromoted},
	},
	MapColumns:   []string{"resource.attributes", "span.attributes", "scope.attributes"},
	StreamFields: []string{"resource_attr:service.name", "name"},
}

func NewRegistry(profile Profile) *Registry {
	r := &Registry{
		profile:    profile,
		byInternal: make(map[string]*FieldMapping, len(profile.Promoted)),
		byParquet:  make(map[string]*FieldMapping, len(profile.Promoted)),
	}
	for i := range profile.Promoted {
		m := &profile.Promoted[i]
		r.byInternal[m.InternalName] = m
		r.byParquet[m.ParquetColumn] = m
	}
	return r
}

func (r *Registry) ResolveToParquet(internalName string) *FieldMapping {
	if m, ok := r.byInternal[internalName]; ok {
		return m
	}

	// VT prefix convention: resource_attr:X -> resource.attributes MAP
	if key, ok := strings.CutPrefix(internalName, "resource_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "resource.attributes",
			InternalName:  internalName,
			Origin:        OriginResourceMap,
			MapColumn:     "resource.attributes",
			MapKey:        key,
		}
	}
	if key, ok := strings.CutPrefix(internalName, "span_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "span.attributes",
			InternalName:  internalName,
			Origin:        OriginSpanAttrMap,
			MapColumn:     "span.attributes",
			MapKey:        key,
		}
	}
	if key, ok := strings.CutPrefix(internalName, "scope_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "scope.attributes",
			InternalName:  internalName,
			Origin:        OriginScopeAttrMap,
			MapColumn:     "scope.attributes",
			MapKey:        key,
		}
	}
	if key, ok := strings.CutPrefix(internalName, "log_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "log.attributes",
			InternalName:  internalName,
			Origin:        OriginLogAttrMap,
			MapColumn:     "log.attributes",
			MapKey:        key,
		}
	}

	// VL dotted convention: try resource.attributes, then log.attributes
	for _, mapCol := range r.profile.MapColumns {
		origin := mapColumnToOrigin(mapCol)
		return &FieldMapping{
			ParquetColumn: mapCol,
			InternalName:  internalName,
			Origin:        origin,
			MapColumn:     mapCol,
			MapKey:        internalName,
		}
	}

	return nil
}

func (r *Registry) ResolveFromParquet(parquetColumn string) *FieldMapping {
	if m, ok := r.byParquet[parquetColumn]; ok {
		return m
	}
	return nil
}

func (r *Registry) PromotedColumns() []FieldMapping {
	return r.profile.Promoted
}

func (r *Registry) MapColumns() []string {
	return r.profile.MapColumns
}

func (r *Registry) StreamFields() []string {
	return r.profile.StreamFields
}

func (r *Registry) TimestampColumn() string {
	return "timestamp_unix_nano"
}

func mapColumnToOrigin(col string) FieldOrigin {
	switch col {
	case "resource.attributes":
		return OriginResourceMap
	case "log.attributes":
		return OriginLogAttrMap
	case "span.attributes":
		return OriginSpanAttrMap
	case "scope.attributes":
		return OriginScopeAttrMap
	default:
		return OriginResourceMap
	}
}
