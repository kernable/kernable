package kernable

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testKernel runs the real kernel binary (cargo build first) in a temp dir: KERNABLE_TEST_KERNEL,
// else target/debug/kernable of the repository.
type testKernel struct {
	t      *testing.T
	dir    string
	url    string
	appKey string
	store  string
	flags  []string
	cmd    *exec.Cmd
}

func kernelBinary(t *testing.T) string {
	if b := os.Getenv("KERNABLE_TEST_KERNEL"); b != "" {
		return b
	}
	b, err := filepath.Abs("../../target/debug/kernable")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b); err != nil {
		t.Skipf("no kernel at %s: cargo build first", b)
	}
	return b
}

type kernelOptions struct {
	processes bool
	extra     []string
}

// newTestKernel prepares a kernel whose app key may run commandTypes and do everything else.
func newTestKernel(t *testing.T, commandTypes []string, o kernelOptions) *testKernel {
	t.Helper()
	// Short: Unix socket paths are limited to ~104 bytes.
	dir, err := os.MkdirTemp("/tmp", "kn-go-")
	if err != nil {
		t.Fatal(err)
	}
	k := &testKernel{t: t, dir: dir, url: "unix:" + filepath.Join(dir, "kernable.sock"), appKey: filepath.Join(dir, "app.key"), store: "sqlite:" + filepath.Join(dir, "k.db")}
	for _, name := range []string{"root", "kernel", "app"} {
		k.cli("keys", "new", name)
	}
	args := []string{"grant", "root.key", "app.pub", "run=" + strings.Join(commandTypes, ",")}
	for _, a := range []string{"submit", "subscribe", "listen", "query", "answer", "check", "vault_erase", "manage", "vault_read", "data_read", "data_write", "cache"} {
		args = append(args, a+"=*")
	}
	if o.processes {
		args = append(args, "process=*")
	}
	k.cli(args...)
	k.flags = append([]string{"start", "--root", "root.pub", "--store", k.store, "--checkpoint-interval-ms", "50", "--instance-key", "kernel.key", "--listen", k.url}, o.extra...)
	t.Cleanup(func() {
		k.stop()
		os.RemoveAll(dir)
	})
	return k
}

func (k *testKernel) cli(args ...string) string {
	k.t.Helper()
	cmd := exec.Command(kernelBinary(k.t), args...)
	cmd.Dir = k.dir
	out, err := cmd.Output()
	if err != nil {
		k.t.Fatalf("kernable %v: %v", args, err)
	}
	return string(out)
}

func (k *testKernel) start() {
	k.t.Helper()
	k.cmd = exec.Command(kernelBinary(k.t), k.flags...)
	k.cmd.Dir = k.dir
	if os.Getenv("KERNABLE_TEST_LOG") != "" {
		k.cmd.Stderr = os.Stderr
	}
	if err := k.cmd.Start(); err != nil {
		k.t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if c, err := net.Dial("unix", strings.TrimPrefix(k.url, "unix:")); err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	k.t.Fatal("kernel did not start")
}

func (k *testKernel) stop() {
	if k.cmd != nil && k.cmd.Process != nil {
		k.cmd.Process.Kill()
		k.cmd.Wait()
		k.cmd = nil
	}
}

func (k *testKernel) verify() string {
	return k.cli("verify", k.store, filepath.Join(k.dir, "kernel.pub"))
}

func (k *testKernel) client(o ...Options) *Client {
	k.t.Helper()
	opts := Options{}
	if len(o) > 0 {
		opts = o[0]
	}
	opts.URL, opts.Key = k.url, k.appKey
	c, err := New(opts)
	if err != nil {
		k.t.Fatal(err)
	}
	k.t.Cleanup(c.Close)
	return c
}

// running runs f until the test ends.
func running(t *testing.T, f func(ctx context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := f(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("run: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func init() {
	if os.Getenv("KERNABLE_LOG") == "" {
		os.Setenv("KERNABLE_LOG", "warn")
	}
	_ = fmt.Sprint
}
