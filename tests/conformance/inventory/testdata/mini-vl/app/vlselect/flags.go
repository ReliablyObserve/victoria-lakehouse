package main

import (
	"flag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
)

var (
	maxConcurrentRequests = flag.Int("search.maxConcurrentRequests", 10, "max concurrent requests")
	maxQueueDuration      = flagutil.NewDuration("search.maxQueueDuration", 0, "max queue duration")
)
