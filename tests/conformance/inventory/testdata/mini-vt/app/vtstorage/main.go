package main

import (
	"flag"
)

func safeRetentionDuration(name, defaultVal, description string) *string {
	return flag.String(name, defaultVal, description)
}

func init() {
	_ = safeRetentionDuration("retentionPeriod", "7d", "retention period")
	_ = flag.Lookup("retentionPeriod")
}

func registerRoutes(httpServer interface{}) {
	// Internal routes
	switch path := ""; path {
	case "/internal/force_merge":
		_ = httpServer
	}
}
