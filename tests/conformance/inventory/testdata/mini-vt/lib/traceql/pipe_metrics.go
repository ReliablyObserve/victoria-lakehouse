package traceql

func getMeter(name string) interface{} {
	switch name {
	case "rate":
		return nil
	case "count_over_time":
		return nil
	case "histogram_over_time":
		return nil
	}
	return nil
}
