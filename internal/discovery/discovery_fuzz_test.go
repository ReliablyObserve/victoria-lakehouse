package discovery

import (
	"testing"
)

func FuzzSplitHostPort(f *testing.F) {
	f.Add("host:9428")
	f.Add("host")
	f.Add("10.0.0.1:9428")
	f.Add("[::1]:9428")
	f.Add("vlstorage.monitoring.svc.cluster.local:9428")
	f.Add("vlstorage.monitoring.svc.cluster.local")
	f.Add(":9428")
	f.Add("")
	f.Add("host:")
	f.Add("::")
	f.Add("[::1]")
	f.Add("host:0")
	f.Add("host:65535")
	f.Add("host:99999")
	f.Add("a:b:c")

	f.Fuzz(func(t *testing.T, addr string) {
		host, port := splitHostPort(addr)
		_ = host
		_ = port
	})
}
