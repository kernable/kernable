package kernable

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// HTTP/2 keeps headers in a table both sides share, so a header damaged on the way (the grant)
// stays wrong there for every later request: a refused grant is not tried again on that
// connection, the next call opens a new one.
func TestARefusedGrantIsNotTriedAgainOnTheSameConnection(t *testing.T) {
	k := newTestKernel(t, []string{"counters.*"}, kernelOptions{})
	k.start()
	proxy := filepath.Join(k.dir, "proxy.sock")
	l, err := net.Listen("unix", proxy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	var accepted atomic.Int64
	go func() {
		for {
			in, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			out, err := net.Dial("unix", strings.TrimPrefix(k.url, "unix:"))
			if err != nil {
				in.Close()
				continue
			}
			go func() { io.Copy(out, in); out.Close() }()
			go func() { io.Copy(in, out); in.Close() }()
		}
	}()
	// Hex, but no Grant: the kernel refuses it as unreadable.
	grant := filepath.Join(k.dir, "damaged.grant")
	if err := os.WriteFile(grant, []byte("ff00ff00"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New(Options{URL: "unix:" + proxy, Key: k.appKey, Grant: grant})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	for i := range 3 {
		_, err := c.Submit(context.Background(), Command{Domain: "counters", AggregateType: "Counter", AggregateID: "c" + string(rune('0'+i)), Type: "Increment"})
		if err == nil || !strings.Contains(err.Error(), "grant") {
			t.Fatalf("call %d: %v", i, err)
		}
		if n := accepted.Load(); n != int64(i+1) {
			t.Fatalf("call %d: %d connections", i, n)
		}
	}
}
