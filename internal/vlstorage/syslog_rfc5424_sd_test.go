package vlstorage

import (
	"flag"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/syslog"
)

// A syslog RFC5424 message whose structured data ends right after `name=`
// ("[id k=") made VictoriaLogs v1.52.0 index past the end of the message and
// panic, killing the process from the listener goroutine (VictoriaLogs #1786).
// The logs binary serves upstream's syslog listener, so a single datagram or
// line like that took the node down. v1.53.0 refuses the malformed structured
// data instead.
//
// The test runs upstream's own TCP listener into the Lakehouse insert path: the
// malformed line must not crash the process, and the well-formed line after it,
// on the same connection, must reach the buffer.
func TestSyslogRFC5424_IncompleteStructuredDataDoesNotCrashTheListener(t *testing.T) {
	SetCardinalityGate(nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	if err := flag.Set("syslog.listenAddr.tcp", addr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set("syslog.listenAddr.tcp", "") })

	buf := &recordingBuffer{}
	SetInsertStorage(buf, t.TempDir())
	syslog.MustInit()
	t.Cleanup(syslog.MustStop)

	var conn net.Conn
	for deadline := time.Now().Add(5 * time.Second); ; {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("syslog listener never came up on %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer func() { _ = conn.Close() }()

	lines := []string{
		// structured data ends immediately after `iut=`
		"<165>1 2026-10-06T10:00:00Z host app 1 ID47 [exampleSDID@32473 iut=",
		"<165>1 2026-10-06T10:00:01Z host app 1 ID48 - after-the-malformed-line",
	}
	if _, err := conn.Write([]byte(strings.Join(lines, "\n") + "\n")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, m := range buf.got() {
			if strings.Contains(m, "after-the-malformed-line") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the well-formed line after the malformed one never reached the buffer; got %v", buf.got())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
