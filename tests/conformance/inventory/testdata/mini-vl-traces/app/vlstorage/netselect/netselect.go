package netselect

// Trimmed copy of VictoriaLogs' protocol-version block (traces pin shape: same select version, older delete version).
const (
	// FieldNamesProtocolVersion is the version of the protocol used for /internal/select/field_names HTTP endpoint.
	FieldNamesProtocolVersion = "v5"

	// QueryProtocolVersion is the version of the protocol used for /internal/select/query HTTP endpoint.
	QueryProtocolVersion = "v5"

	// StreamIDsProtocolVersion is the version of the protocol used for /internal/select/stream_ids HTTP endpoint.
	StreamIDsProtocolVersion = "v5"

	// DeleteRunTaskProtocolVersion is the version of the protocol used for /internal/delete/run_task HTTP endpoint.
	DeleteRunTaskProtocolVersion = "v1"

	// DeleteStopTaskProtocolVersion is the version of the protocol used for /internal/delete/stop_task HTTP endpoint.
	DeleteStopTaskProtocolVersion = "v1"

	// DeleteActiveTasksProtocolVersion is the version of the protocol used for /internal/delete/active_tasks endpoint.
	DeleteActiveTasksProtocolVersion = "v1"
)
