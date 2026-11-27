package kernable

import "testing"

// A kernel on this machine is retried within two seconds after it went away (a restart is over
// in seconds); one elsewhere backs off to 30 s.
func TestLocalKernelsAreRetriedSoon(t *testing.T) {
	for _, url := range []string{"unix:/srv/run/kernel.sock", "http://127.0.0.1:7233", "127.0.0.1:7233",
		"http://localhost:7233", "http://[::1]:7233", "http://127.0.0.1:7233,http://127.0.0.1:7235"} {
		if !isLocal(url) {
			t.Errorf("%s: not taken as local", url)
		}
	}
	for _, url := range []string{"https://kernel.example.com:7233", "http://10.0.0.5:7233",
		"http://127.0.0.1:7233,https://b.example:7233", "http://localhost.example.com:7233", "http://"} {
		if isLocal(url) {
			t.Errorf("%s: taken as local", url)
		}
	}
}
