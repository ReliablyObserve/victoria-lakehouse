package ingestmatrix

// MixedNativeRequest uses the same upstream InsertRow builders as the matrix,
// but puts multiple encoded tenants in a single native request. Native trace
// IDs are strings, so these controls deliberately need not be OTLP hex IDs.
func MixedNativeRequest(sig Signal, params []Params) Request {
	request := Request{Method: "POST", Path: "/insert/multitenant/native", Query: "version=v1", Header: map[string]string{"Content-Type": "application/octet-stream"}}
	for _, p := range params {
		if sig == Traces {
			request.Body = append(request.Body, traceNativeRows(p, p.Tenant)...)
		} else {
			request.Body = append(request.Body, nativeRows(p, p.Tenant)...)
		}
		request.Rows += 3
	}
	return request
}
