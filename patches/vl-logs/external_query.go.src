package logstorage

// RunQueryExternal executes a query with full pipe processing for external
// storage backends. The searchFn provides raw filtered rows; pipes from
// qctx.Query are then applied to produce the final output.
//
// This reuses VL's native runPipes machinery — no reimplementation needed.
func RunQueryExternal(qctx *QueryContext, searchFn func(writeBlock WriteDataBlockFunc) error, writeBlock WriteDataBlockFunc) error {
	writeBlockResult := writeBlock.newBlockResultWriter()

	q := qctx.Query
	concurrency := q.GetConcurrency()

	search := func(stopCh <-chan struct{}, writeBlockToPipes writeBlockResultFunc) error {
		wb := writeBlockToPipes.newDataBlockWriter()
		return searchFn(wb)
	}

	return runPipes(qctx, q.pipes, search, writeBlockResult, concurrency)
}

// QueryHasPipes returns true if the query has pipe operators attached.
func QueryHasPipes(q *Query) bool {
	return len(q.pipes) > 0
}
