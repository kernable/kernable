package kernable

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// DefaultTimeout is how long a request waits, for the connection and the answer, unless told
// otherwise: above the kernel's own submit and settle timeouts.
const DefaultTimeout = 60 * time.Second

const (
	// Room for the largest file a kernel takes (--max-data-bytes, at most 128 MiB).
	maxDataMessage = 129 << 20
	// How long a request waits for one address of several before it tries the next.
	perAddress = time.Second
	// The first pause before an idempotent call asks a kernel that answered UNAVAILABLE again;
	// it doubles up to the next.
	retryPause    = 50 * time.Millisecond
	maxRetryPause = 200 * time.Millisecond
	// How long an idempotent request whose connection dropped under way is tried again: long
	// enough for a kernel handing its role over, short enough that a crash reaches the caller.
	droppedRetry = 5 * time.Second
	// How long a request waits for its tenant's move to end.
	moveWait = 30 * time.Second
)

// TLSFiles are PEM files for mutual TLS: the CA of the kernel's certificate, and this
// process's own certificate and key.
type TLSFiles struct{ CA, Cert, Key string }

// target is one kernel address with its connection.
type target struct {
	conn *grpc.ClientConn
	// The connection for files: larger messages.
	data *grpc.ClientConn
}

// clients are the clients of one kernel, or of several (a pair): requests go to the current
// one, and turn to the next on UNAVAILABLE or when it does not connect.
type clients struct {
	targets []*target
	current atomic.Int64
	// The newest position a command of this client committed at, on these kernels: its queries
	// wait for it, on whichever copy answers (read your writes).
	seenPos atomic.Uint64
	grant   string
	timeout time.Duration
}

func (c *clients) seen() uint64 { return c.seenPos.Load() }

func (c *clients) saw(position uint64) {
	for {
		old := c.seenPos.Load()
		if position <= old || c.seenPos.CompareAndSwap(old, position) {
			return
		}
	}
}

// next turns to the next address, unless another call did so since `from`.
func (c *clients) next(from int64) {
	if len(c.targets) > 1 {
		c.current.CompareAndSwap(from, (from+1)%int64(len(c.targets)))
	}
}

func (c *clients) close() {
	for _, t := range c.targets {
		t.conn.Close()
		t.data.Close()
	}
}

// outgoing adds the grant to a call's metadata.
func (c *clients) outgoing(ctx context.Context) context.Context {
	if c.grant == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "kernable-grant", c.grant)
}

// addresses splits a comma-separated list of kernel addresses: `http://host:port`,
// `host:port` or `unix:/path`.
func addresses(url string) []string {
	var out []string
	for _, u := range strings.Split(url, ",") {
		u = strings.TrimSpace(u)
		u = strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://")
		u = strings.TrimSuffix(u, "/")
		if u != "" {
			out = append(out, u)
		}
	}
	return out
}

// dial connects to the kernels at url (comma-separated for a pair), lazily: a request waits
// for the connection.
func dial(url string, files *TLSFiles, grant string, timeout time.Duration) (*clients, error) {
	urls := addresses(url)
	if len(urls) == 0 {
		return nil, errors.New("no kernel address")
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	// Pings on every connection, also idle ones: a connection the network dropped without a word
	// ends, and the workers and listeners on it reconnect. A client of several kernels pings as
	// often as gRPC lets it (every 10 s) and waits 2 s for the answer, so a kernel the network
	// cut off is left for the other one.
	ka := keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}
	if len(urls) > 1 {
		ka = keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 2 * time.Second, PermitWithoutStream: true}
	}
	creds := insecure.NewCredentials()
	if files != nil {
		var err error
		if creds, err = tlsCredentials(*files); err != nil {
			return nil, err
		}
	}
	c := &clients{grant: grant, timeout: timeout}
	for _, u := range urls {
		t := strings.TrimPrefix(u, "unix:")
		if strings.HasPrefix(u, "unix:") {
			t = "unix:" + t
		} else {
			t = "passthrough:///" + u
		}
		opts := []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithKeepaliveParams(ka)}
		conn, err := grpc.NewClient(t, opts...)
		if err != nil {
			return nil, err
		}
		data, err := grpc.NewClient(t, append(opts, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxDataMessage), grpc.MaxCallSendMsgSize(maxDataMessage)))...)
		if err != nil {
			return nil, err
		}
		c.targets = append(c.targets, &target{conn: conn, data: data})
	}
	return c, nil
}

func tlsCredentials(f TLSFiles) (credentials.TransportCredentials, error) {
	ca, err := os.ReadFile(f.CA)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("%s holds no certificate", f.CA)
	}
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}), nil
}

// ready waits until conn is connected, or until `until`.
func ready(ctx context.Context, conn *grpc.ClientConn, until time.Time) bool {
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	for {
		s := conn.GetState()
		switch s {
		case connectivity.Ready:
			return true
		case connectivity.Idle:
			conn.Connect()
		case connectivity.Shutdown:
			return false
		}
		if !conn.WaitForStateChange(ctx, s) {
			return false
		}
	}
}

var droppedText = regexp.MustCompile(`(?i)connection dropped|socket closed|ECONNRESET|EPIPE|RST_STREAM|GOAWAY|connection reset|transport is closing|error reading from server|EOF`)

// connectionDropped is whether err is the client's own UNAVAILABLE for a connection that went
// away under the request, not a kernel's answer.
func connectionDropped(err error) bool { return droppedText.MatchString(status.Convert(err).Message()) }

// moving is whether a kernel refused a request because its tenant is moving to another partition.
func moving(err error) bool {
	s := status.Convert(err)
	return s.Code() == codes.Unavailable && strings.HasPrefix(s.Message(), "moving")
}

// wrongPartition is whether a kernel refused a request as one for a tenant of another partition.
func wrongPartition(err error) bool {
	s := status.Convert(err)
	return s.Code() == codes.FailedPrecondition && strings.HasPrefix(s.Message(), "wrong_partition")
}

// unary runs one call with the grant and the protocol version: it waits for the connection,
// then for the answer, until the deadline; with several addresses, it turns to the next one when
// one does not connect within a second or answers UNAVAILABLE. An idempotent call (a submit, by
// its command ID; a cancel; a query; a data or cache read; the partition map) also asks a single
// address again after UNAVAILABLE, with backoff until the deadline; any other may have run, so
// the error is final.
func unary[R any](ctx context.Context, c *clients, timeout time.Duration, idempotent, data bool, call func(context.Context, *grpc.ClientConn) (R, error)) (R, error) {
	var zero R
	if timeout <= 0 {
		timeout = c.timeout
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	pause := retryPause
	var droppedAt time.Time
	var droppedErr error
	several := len(c.targets) > 1
	for {
		at := c.current.Load()
		t := c.targets[at]
		conn := t.conn
		if data {
			conn = t.data
		}
		until := deadline
		if droppedErr != nil && droppedAt.Add(droppedRetry).Before(until) {
			until = droppedAt.Add(droppedRetry)
		}
		wait := until
		if several && time.Now().Add(perAddress).Before(wait) {
			wait = time.Now().Add(perAddress)
		}
		if !ready(ctx, conn, wait) {
			if ctx.Err() != nil {
				return zero, ctx.Err()
			}
			if droppedErr != nil && !time.Now().Before(until) && until.Before(deadline) {
				return zero, droppedErr
			}
			// The next attempt, of this request or the next one, tries the next address.
			c.next(at)
			if !time.Now().Before(deadline) {
				return zero, status.Errorf(codes.DeadlineExceeded, "no connection to the kernel within %d ms", timeout.Milliseconds())
			}
			continue
		}
		callCtx, cancel := context.WithDeadline(c.outgoing(ctx), deadline)
		r, err := call(callCtx, conn)
		cancel()
		if err == nil {
			return r, nil
		}
		if status.Code(err) != codes.Unavailable || !time.Now().Before(deadline) {
			return zero, err
		}
		switch {
		case several:
			c.next(at)
		case idempotent && !moving(err) && time.Now().Add(pause).Before(deadline):
			if connectionDropped(err) {
				if droppedErr == nil {
					droppedAt, droppedErr = time.Now(), err
				}
				if time.Since(droppedAt) >= droppedRetry {
					return zero, err
				}
			}
			select {
			case <-time.After(pause):
			case <-ctx.Done():
				return zero, ctx.Err()
			}
			pause = min(pause*2, maxRetryPause)
		default:
			return zero, err
		}
	}
}

// stream opens a stream on the current address; one that ends with UNAVAILABLE turns the
// clients to the next. `failed` reports the stream's end.
func (c *clients) stream() (conn *grpc.ClientConn, failed func(error)) {
	at := c.current.Load()
	return c.targets[at].conn, func(err error) {
		if status.Code(err) == codes.Unavailable {
			c.next(at)
		}
	}
}

func (c *clients) submit(ctx context.Context, req *pb.SubmitRequest, timeout time.Duration) (*pb.CommandResult, error) {
	req.Protocol = Protocol
	return unary(ctx, c, timeout, true, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.CommandResult, error) {
		return pb.NewCommandServiceClient(conn).Submit(ctx, req)
	})
}

func (c *clients) cancel(ctx context.Context, req *pb.CancelRequest, timeout time.Duration) (*pb.CancelResult, error) {
	req.Protocol = Protocol
	return unary(ctx, c, timeout, true, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.CancelResult, error) {
		return pb.NewCommandServiceClient(conn).Cancel(ctx, req)
	})
}

func (c *clients) query(ctx context.Context, req *pb.QueryRequest, timeout time.Duration) (*pb.QueryResult, error) {
	req.Protocol = Protocol
	return unary(ctx, c, timeout, true, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.QueryResult, error) {
		return pb.NewQueryServiceClient(conn).Query(ctx, req)
	})
}

// dataCall runs a file request: reads are safe to repeat; a write is one-shot (the kernel's
// replay guard), so never.
func (c *clients) dataCall(ctx context.Context, op string, req *pb.DataRequest) (*pb.DataResult, error) {
	req.Protocol = Protocol
	return unary(ctx, c, 0, op == "get", true, func(ctx context.Context, conn *grpc.ClientConn) (*pb.DataResult, error) {
		s := pb.NewDataServiceClient(conn)
		switch op {
		case "put":
			return s.PutData(ctx, req)
		case "get":
			return s.GetData(ctx, req)
		default:
			return s.DeleteData(ctx, req)
		}
	})
}

func (c *clients) listData(ctx context.Context, req *pb.DataRequest) (*pb.DataList, error) {
	req.Protocol = Protocol
	return unary(ctx, c, 0, true, true, func(ctx context.Context, conn *grpc.ClientConn) (*pb.DataList, error) {
		return pb.NewDataServiceClient(conn).ListData(ctx, req)
	})
}

func (c *clients) cacheCall(ctx context.Context, op string, req *pb.CacheRequest) (*pb.CacheResult, error) {
	req.Protocol = Protocol
	return unary(ctx, c, 0, op == "get", false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.CacheResult, error) {
		s := pb.NewCacheServiceClient(conn)
		switch op {
		case "set":
			return s.SetCache(ctx, req)
		case "get":
			return s.GetCache(ctx, req)
		default:
			return s.DeleteCache(ctx, req)
		}
	})
}

// apply sends a membership; the same one again is fine (the kernel takes a retried deploy).
func (c *clients) apply(ctx context.Context, req *pb.ApplyRequest) error {
	req.Protocol = Protocol
	_, err := unary(ctx, c, 0, true, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.ApplyResult, error) {
		return pb.NewMembershipServiceClient(conn).Apply(ctx, req)
	})
	return err
}

func (c *clients) registerProcess(ctx context.Context, req *pb.ProcessRegistration) (*pb.ProcessRegistered, error) {
	req.Protocol = Protocol
	return unary(ctx, c, 0, false, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.ProcessRegistered, error) {
		return pb.NewProcessServiceClient(conn).Register(ctx, req)
	})
}

func (c *clients) processAct(ctx context.Context, req *pb.ProcessActing) (*pb.ProcessActed, error) {
	req.Protocol = Protocol
	return unary(ctx, c, 0, false, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.ProcessActed, error) {
		return pb.NewProcessServiceClient(conn).Act(ctx, req)
	})
}

// partitionMap is the partition map the kernel holds; nil if it serves every tenant.
func (c *clients) partitionMap(ctx context.Context, timeout time.Duration) (*pb.PartitionMap, error) {
	m, err := unary(ctx, c, timeout, true, false, func(ctx context.Context, conn *grpc.ClientConn) (*pb.PartitionMap, error) {
		return pb.NewPartitionServiceClient(conn).Map(ctx, &pb.MapRequest{Protocol: Protocol})
	})
	if code := status.Code(err); code == codes.NotFound || code == codes.Unimplemented {
		return nil, nil
	}
	return m, err
}

// partitioned are the clients of every partition, found by the map the first kernel holds: a
// request goes to its tenant's partition; refused there as wrong_partition (the map changed),
// the map is read again and the request sent anew. A kernel without partitions holds no map:
// everything goes to the first kernel.
type partitioned struct {
	entry *clients
	make  func(url string) (*clients, error)

	mu     sync.Mutex
	loaded bool
	pmap   *pb.PartitionMap
	parts  map[string]*clients
}

func newPartitioned(url string, make func(string) (*clients, error)) (*partitioned, error) {
	entry, err := make(url)
	if err != nil {
		return nil, err
	}
	return &partitioned{entry: entry, make: make, parts: map[string]*clients{}}, nil
}

// current is the map, read once and again after `refresh`; nil without partitions.
func (p *partitioned) current(ctx context.Context, refresh bool, timeout time.Duration) (*pb.PartitionMap, error) {
	p.mu.Lock()
	if refresh {
		p.loaded = false
	}
	if p.loaded {
		m := p.pmap
		p.mu.Unlock()
		return m, nil
	}
	p.mu.Unlock()
	m, err := p.entry.partitionMap(ctx, timeout)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.pmap, p.loaded = m, true
	p.mu.Unlock()
	return m, nil
}

func partitionOf(m *pb.PartitionMap, tenant string) string {
	for _, t := range m.Tenants {
		if t.Tenant == tenant {
			return t.Partition
		}
	}
	return m.Default
}

func (p *partitioned) clientsOf(m *pb.PartitionMap, name string) (*clients, error) {
	var partition *pb.Partition
	for _, x := range m.Partitions {
		if x.Name == name {
			partition = x
		}
	}
	if partition == nil {
		return nil, fmt.Errorf("the partition map names no partition %s", name)
	}
	// `relay:<relay>/<copy>` is how partitions at other sites reach this one: not a client's way.
	var own []string
	for _, a := range partition.Addresses {
		if !strings.HasPrefix(a, "relay:") {
			own = append(own, a)
		}
	}
	if len(own) == 0 {
		return nil, fmt.Errorf("partition %s is reached only through a relay: a client needs one of its own addresses", name)
	}
	url := strings.Join(own, ",")
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.parts[url]; ok {
		return c, nil
	}
	c, err := p.make(url)
	if err != nil {
		return nil, err
	}
	p.parts[url] = c
	return c, nil
}

func (p *partitioned) forTenant(ctx context.Context, tenant string) (*clients, error) {
	m, err := p.current(ctx, false, 0)
	if err != nil || m == nil {
		return p.entry, err
	}
	return p.clientsOf(m, partitionOf(m, tenant))
}

// control is the clients of the control partition, which takes memberships and processes.
func (p *partitioned) control(ctx context.Context) (*clients, error) {
	m, err := p.current(ctx, false, 0)
	if err != nil || m == nil {
		return p.entry, err
	}
	return p.clientsOf(m, m.Control)
}

type partition struct {
	name    string
	clients *clients
}

// all is every partition with its clients; one, named "", without partitions.
func (p *partitioned) all(ctx context.Context, timeout time.Duration) ([]partition, error) {
	m, err := p.current(ctx, false, timeout)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return []partition{{"", p.entry}}, nil
	}
	var out []partition
	for _, x := range m.Partitions {
		c, err := p.clientsOf(m, x.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, partition{x.Name, c})
	}
	return out, nil
}

// route runs call on tenant's partition, sent anew after reading the map again on
// wrong_partition, and while the tenant moves (refused before it took effect) for up to 30 s.
func route[R any](ctx context.Context, p *partitioned, tenant string, call func(*clients) (R, error)) (R, error) {
	deadline := time.Now().Add(moveWait)
	pause := 50 * time.Millisecond
	for attempt := 0; ; {
		var zero R
		c, err := p.forTenant(ctx, tenant)
		if err != nil {
			return zero, err
		}
		r, err := call(c)
		if err == nil {
			return r, nil
		}
		if moving(err) && time.Now().Add(pause).Before(deadline) {
			select {
			case <-time.After(pause):
			case <-ctx.Done():
				return zero, ctx.Err()
			}
			pause = min(pause*2, time.Second)
			continue
		}
		if attempt >= 3 || !wrongPartition(err) {
			return zero, err
		}
		attempt++
		if _, err := p.current(ctx, true, 0); err != nil {
			return zero, err
		}
	}
}

func (p *partitioned) close() {
	p.entry.close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.parts {
		c.close()
	}
}

// local is whether url (every address of a comma-separated list) is a Unix socket or a loopback
// address: plain text never leaves the machine.
func local(url string) bool {
	for _, u := range addresses(url) {
		if strings.HasPrefix(u, "unix:") {
			continue
		}
		host, _, err := net.SplitHostPort(u)
		if err != nil {
			host = u
		}
		host = strings.Trim(host, "[]")
		if host == "localhost" {
			continue
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return false
		}
	}
	return true
}
