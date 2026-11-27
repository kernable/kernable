package kernable

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type level int

const (
	levelOff level = iota
	levelError
	levelWarn
	levelInfo
	levelDebug
)

// pairRetry is the longest wait before trying a kernel of a pair again that was not the primary.
const pairRetry = time.Second

// localRetry is the longest wait before trying a kernel on this machine again: a restart is over
// in seconds.
const localRetry = 2 * time.Second

// isLocal reports whether every address in url (comma-separated for a pair) is on this machine:
// unix:, or a loopback host (127.0.0.0/8, localhost, ::1).
func isLocal(url string) bool {
	for _, a := range strings.Split(url, ",") {
		a = strings.TrimSpace(a)
		if a == "" || strings.HasPrefix(a, "unix:") {
			continue
		}
		rest := a
		if i := strings.Index(a, "://"); i >= 0 {
			rest = a[i+3:]
		}
		var host string
		if strings.HasPrefix(rest, "[") {
			host, _, _ = strings.Cut(rest[1:], "]")
		} else {
			parts := strings.FieldsFunc(rest, func(r rune) bool { return r == ':' || r == '/' })
			if len(parts) == 0 {
				return false
			}
			host = parts[0]
		}
		if host != "localhost" && host != "::1" && !strings.HasPrefix(host, "127.") {
			return false
		}
	}
	return true
}

// logger prints connection and activity lines of workers, listeners and subscriptions on stderr,
// up to the level of KERNABLE_LOG (off, error, warn, info (default) or debug):
// `12:04:11  listener carts  <line>`.
type logger struct {
	level level
	color bool
	// `<url> · key <name> (<key id>…)`: the kernel and the key this process connects with.
	peer string
	// local: every address of the kernel is on this machine; reconnecting waits at most localRetry.
	local bool
	mu    sync.Mutex
}

func newLogger(url, keyPath string, key *Key) *logger {
	l := &logger{level: levelInfo, local: isLocal(url)}
	switch strings.ToLower(os.Getenv("KERNABLE_LOG")) {
	case "off":
		l.level = levelOff
	case "error":
		l.level = levelError
	case "warn":
		l.level = levelWarn
	case "debug":
		l.level = levelDebug
	}
	if fi, err := os.Stderr.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 && os.Getenv("NO_COLOR") == "" {
		l.color = true
	}
	name := strings.TrimSuffix(filepath.Base(keyPath), ".key")
	l.peer = fmt.Sprintf("%s · key %s (%s…)", url, name, key.ID[:12])
	return l
}

func (l *logger) enabled(lv level) bool { return lv <= l.level }

// style is text in an ANSI style (1 bold, 2 dim, 31 red, 33 yellow) on a terminal.
func (l *logger) style(code int, text string) string {
	if !l.color {
		return text
	}
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", code, text)
}

// print writes `12:04:11  listener carts  <line>`, warnings in yellow and errors in red.
func (l *logger) print(lv level, what, line string) {
	if !l.enabled(lv) {
		return
	}
	switch lv {
	case levelError:
		line = l.style(31, line)
	case levelWarn:
		line = l.style(33, line)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(os.Stderr, "%s  %s  %s\n", l.style(2, time.Now().Format("15:04:05")), l.style(1, what), line)
}

// refusals are codes a retry does not change.
var refusals = map[codes.Code]bool{
	codes.InvalidArgument: true, codes.PermissionDenied: true, codes.FailedPrecondition: true,
	codes.Unimplemented: true, codes.Unauthenticated: true,
}

// switchingRoles is whether reason says the kernel is one of a pair that is not the primary
// right now.
func switchingRoles(reason string) bool {
	return strings.HasPrefix(reason, "not_primary") || strings.Contains(reason, "lost its lease at the witness") ||
		strings.Contains(reason, "hands its role over")
}

// backoff is the pause before try attempt (from 1): base, doubled each time up to 30 s, ±20%.
func backoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt && d < 30*time.Second; i++ {
		d *= 2
	}
	d = min(d, 30*time.Second)
	return time.Duration(float64(d) * (0.8 + rand.Float64()*0.4)).Round(time.Millisecond)
}

// reconnect logs the end of a session that was not stopped and waits before reconnecting. A
// refusal that will not go away by itself (no grant, an invalid registration) is an error, not a
// warning. It returns early when ctx ends.
func (l *logger) reconnect(ctx context.Context, what string, err error, base time.Duration, attempt int) {
	reason := err.Error()
	code := codes.Unknown
	if s, ok := status.FromError(err); ok {
		reason, code = s.Message(), s.Code()
	}
	delay := backoff(base, attempt)
	if switchingRoles(reason) {
		delay = min(delay, pairRetry)
	} else if l.local {
		delay = min(delay, localRetry)
	}
	if refusals[code] {
		l.print(levelError, what, fmt.Sprintf("refused (%s), reconnecting in %g s", reason, delay.Seconds()))
	} else {
		l.print(levelWarn, what, fmt.Sprintf("connection lost (%s), reconnecting in %g s", reason, delay.Seconds()))
	}
	sleep(ctx, delay)
}

// storeFailed logs that a store could not be opened, and waits before the next try.
func (l *logger) storeFailed(ctx context.Context, what string, err error, base time.Duration, attempt int) {
	delay := backoff(base, attempt)
	l.print(levelError, what, fmt.Sprintf("store: %v; trying again in %g s", err, delay.Seconds()))
	sleep(ctx, delay)
}

// sleep waits d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
