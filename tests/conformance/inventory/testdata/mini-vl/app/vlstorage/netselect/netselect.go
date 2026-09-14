package netselect

// Trimmed copy of VictoriaLogs' protocol-version block (logs pin shape).
const (
	// FieldNamesProtocolVersion is the version of the protocol used for /internal/select/field_names HTTP endpoint.
	FieldNamesProtocolVersion = "v5"

	// QueryProtocolVersion is the version of the protocol used for /internal/select/query HTTP endpoint.
	QueryProtocolVersion = "v5"

	// StreamIDsProtocolVersion is the version of the protocol used for /internal/select/stream_ids HTTP endpoint.
	StreamIDsProtocolVersion = "v5"

	// DeleteRunTaskProtocolVersion is the version of the protocol used for /internal/delete/run_task HTTP endpoint.
	DeleteRunTaskProtocolVersion = "v2"

	// DeleteStopTaskProtocolVersion is the version of the protocol used for /internal/delete/stop_task HTTP endpoint.
	DeleteStopTaskProtocolVersion = "v2"

	// DeleteActiveTasksProtocolVersion is the version of the protocol used for /internal/delete/active_tasks endpoint.
	DeleteActiveTasksProtocolVersion = "v2"
)
