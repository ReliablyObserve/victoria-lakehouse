package main

import "flag"

// upstream-copy: VictoriaTraces app/vtselect/main.go — the -internalselect.disable
// flag, verbatim (internal_select_test.go fails when the vendored source stops
// matching). VictoriaTraces registers it in vtselect, which this binary cannot
// link (see internal_delete.go), so it is registered here. buffer.Gate reads it
// by name for /internal/select/* (mountInternalProtocol) and for
// /internal/buffer/query, as the logs binary does with the copy VictoriaLogs'
// vlselect registers.
var _ = flag.Bool("internalselect.disable", false, "Whether to disable /internal/select/* HTTP endpoints")
