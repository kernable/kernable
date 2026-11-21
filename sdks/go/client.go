package kernable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// Options say how a Client connects: kernel address, key, grant and defaults for its requests.
// Every option has a default, most from an environment variable, so New(Options{}) works with
// the files `kernable start --dev` creates.
type Options struct {
	// Kernel address: http://host:port (host:port works too) or, on Unix, unix:/path/to.sock.
	// For a pair of kernels with a standby, both, comma-separated. Default: KERNABLE_URL, else
	// http://127.0.0.1:7233.
	URL string
	// Path to this process's .key file. Default: KERNABLE_KEY, else app.key.
	Key string
	// Path to the key's grant (`kernable grant`), sent with every call. Default:
	// KERNABLE_GRANT, else the key file's path with .grant for .key, if it exists.
	Grant string
	// Default tenant for requests. Default: default.
	Tenant string
	// The service this process's workers and listeners belong to. Default: KERNABLE_SERVICE.
	Service string
	// This process's code version, e.g. a build ID: 1 to 128 letters, digits, `-`, `_` and
	// `.`. Default: KERNABLE_DEPLOYMENT.
	Deployment string
	// Default request context for requests; a request's own context replaces it as a whole.
	Context RequestContext
	// Mutual TLS for host:port URLs. Default: KERNABLE_TLS_CA, KERNABLE_TLS_CERT and
	// KERNABLE_TLS_KEY, if set.
	TLS *TLSFiles
	// Allows plain text to a kernel that is not on this machine. Default: KERNABLE_INSECURE=1.
	Insecure bool
	// How long a request waits in all, for the connection and its answer. Default 60 s.
	Timeout time.Duration
}

// Client is a client of one kernel (or a pair, or partitions): it submits commands, asks
// queries, and runs workers and listeners over its connection. Create one per process with
// New; it connects on the first call. Every request is signed with the key; the kernel checks it
// against the key's grant.
//
// Calls the kernel refuses itself (not signed, not granted, no read model for a query, the
// kernel unreachable) return the gRPC error, whose message says why. Business rejections come
// differently: Submit returns them in its Outcome, Query returns a *Rejected.
type Client struct {
	// Files of a domain kept by the kernel; see Data.
	Data *Data
	// The shared cache of a domain; see Cache.
	Cache      *Cache
	parts      *partitioned
	key        *Key
	log        *logger
	tenant     string
	context    RequestContext
	service    string
	deployment string
	mu         sync.RWMutex
	domains    map[string]*Domain
}

// New reads the key, grant and TLS files and returns a client; it connects on the first call. A
// missing key file, an explicitly named grant file that is missing or a missing TLS file is an
// error.
func New(o Options) (*Client, error) {
	keyPath := firstOf(o.Key, os.Getenv("KERNABLE_KEY"), "app.key")
	grantPath := firstOf(o.Grant, os.Getenv("KERNABLE_GRANT"), strings.TrimSuffix(keyPath, ".key")+".grant")
	explicit := o.Grant != "" || os.Getenv("KERNABLE_GRANT") != ""
	url := firstOf(o.URL, os.Getenv("KERNABLE_URL"), "http://127.0.0.1:7233")
	files := o.TLS
	if files == nil {
		var err error
		if files, err = tlsFromEnv(); err != nil {
			return nil, err
		}
	}
	if files == nil && !o.Insecure && os.Getenv("KERNABLE_INSECURE") != "1" && !local(url) {
		return nil, fmt.Errorf("%s is not on this machine: connect with TLS (Options.TLS), or set Options.Insecure to send in plain text", url)
	}
	grant := ""
	if b, err := os.ReadFile(grantPath); err == nil {
		grant = strings.TrimSpace(string(b))
	} else if explicit {
		return nil, fmt.Errorf("grant %s: %w", grantPath, err)
	}
	key, err := KeyFromFile(keyPath)
	if err != nil {
		return nil, err
	}
	deployment := firstOf(o.Deployment, os.Getenv("KERNABLE_DEPLOYMENT"))
	if deployment != "" && !deployments.MatchString(deployment) {
		return nil, fmt.Errorf("a deployment has 1 to 128 letters, digits, `-`, `_` and `.`, not %q", deployment)
	}
	parts, err := newPartitioned(url, func(u string) (*clients, error) { return dial(u, files, grant, o.Timeout) })
	if err != nil {
		return nil, err
	}
	c := &Client{
		parts: parts, key: key, log: newLogger(url, keyPath, key), tenant: firstOf(o.Tenant, "default"),
		context: o.Context, service: firstOf(o.Service, os.Getenv("KERNABLE_SERVICE")), deployment: deployment,
		domains: map[string]*Domain{},
	}
	c.Data = &Data{c}
	c.Cache = &Cache{c}
	warnIfShared(c.log, keyPath)
	return c, nil
}

func firstOf(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// tlsFromEnv is mutual TLS from KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY: all or none.
func tlsFromEnv() (*TLSFiles, error) {
	ca, cert, key := os.Getenv("KERNABLE_TLS_CA"), os.Getenv("KERNABLE_TLS_CERT"), os.Getenv("KERNABLE_TLS_KEY")
	if ca == "" && cert == "" && key == "" {
		return nil, nil
	}
	if ca == "" || cert == "" || key == "" {
		return nil, errors.New("set all of KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY, or none")
	}
	return &TLSFiles{CA: ca, Cert: cert, Key: key}, nil
}

// warnIfShared warns if other users can read the key file: whoever reads it can sign as this
// process.
func warnIfShared(log *logger, key string) {
	fi, err := os.Stat(key)
	if err != nil {
		return
	}
	if mode := fi.Mode().Perm(); mode&0o077 != 0 {
		log.print(levelWarn, "key", fmt.Sprintf("%s can be read by other users (mode %o): chmod 600 %s", key, mode, key))
	}
}

// KeyID is the ID of this client's key.
func (c *Client) KeyID() string { return c.key.ID }

// Use seals the vault fields of these domains' commands when submitting them, and sends the
// targets their queries declare; without it, those fields go into the signed log in clear.
func (c *Client) Use(domains ...*Domain) *Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range domains {
		c.domains[d.Name] = d
	}
	return c
}

func (c *Client) domain(name string) *Domain {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domains[name]
}

// AnyAggregate is any *Aggregate[S].
type AnyAggregate interface{ aggregateDef }

// Worker is a worker for these aggregates: Worker(cart).Run(ctx) executes their commands until
// ctx ends. Start as many workers as you like; the kernel spreads commands over them, one at a
// time per aggregate.
func (c *Client) Worker(aggregates ...AnyAggregate) *Worker {
	defs := make([]aggregateDef, len(aggregates))
	for i, a := range aggregates {
		defs[i] = a
	}
	return newWorker(c, defs, c.service)
}

// Listener runs a listener defined with Domain.ReadModel or Domain.Reactor over this
// connection: Listener(carts).Run(ctx).
func (c *Client) Listener(l *Listener) *ListenerHandle { return newListenerHandle(c, l, c.service) }

// Close closes the connection; only needed in a long-running process that discards the client.
func (c *Client) Close() { c.parts.close() }

// partitions are the partitions of the client, read again (backing off like a reconnect) while
// no kernel answers; nil once ctx ends.
func (c *Client) partitions(ctx context.Context, what string, reconnect time.Duration) []partition {
	for attempt := 1; ; attempt++ {
		parts, err := c.parts.all(ctx, 500*time.Millisecond)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return parts
		}
		c.log.reconnect(ctx, what, err, reconnect, attempt)
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Command is a command for Client.Submit and Client.Enqueue: the aggregate it goes to (domain,
// aggregate type and ID), the command type and its data.
type Command struct {
	// The domain of the aggregate and the command type: shop.
	Domain string
	// The aggregate type within the domain: cart.
	AggregateType string
	// Which aggregate: the stream the command's events go to.
	AggregateID string
	// The command type within the domain: AddItem.
	Type string
	// The command's data, sent as JSON; fields declared with Domain.Command go to the vault.
	Data any
	// Default: the client's tenant.
	Tenant string
	// The command ID and idempotency key. Default: a new UUID.
	ID string
	// Replaces the client's default context; inside a listener, ctx.Follow(). Its CorrelationID
	// defaults to the command ID.
	Context *RequestContext
	// Committed returns once the events are stored, instead of once every listener that counts
	// processed them (the default).
	Committed bool
	// How long to wait for the connection and the outcome; default Options.Timeout.
	Timeout time.Duration
	// Enqueue only: a scheduled command, run at this time and not before.
	At time.Time
	// Enqueue only, with At: if it cannot run by this time, it is rejected expired.
	Until time.Time
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// Submit runs a command and returns its outcome: committed with its events, or rejected with a
// code and message (a rejection is not an error). By default it returns once the command
// settled, so a query right after it sees the events. It returns a *NotSettled if the command
// committed but did not settle in time, a *NotDue for a scheduled command that is not due yet.
// Idempotent on the command's ID.
func (c *Client) Submit(ctx context.Context, cmd Command) (Outcome, error) {
	if !cmd.At.IsZero() {
		return Outcome{}, errors.New("a scheduled command (At) has no outcome yet: use Enqueue")
	}
	wait := pb.Wait_WAIT_SETTLED
	if cmd.Committed {
		wait = pb.Wait_WAIT_COMMITTED
	}
	res, err := c.send(ctx, cmd, wait)
	if err != nil {
		return Outcome{}, err
	}
	switch x := res.Outcome.(type) {
	case *pb.CommandResult_Rejected:
		return rejectedOutcome(x.Rejected), nil
	case *pb.CommandResult_Accepted:
		return Outcome{}, &NotDue{CommandID: res.CommandId}
	case *pb.CommandResult_Committed:
		// Events come back with references only: the submitter is no vault reader.
		events, err := storedAll(x.Committed.Events)
		if err != nil {
			return Outcome{}, err
		}
		outcome := Outcome{Committed: true, Events: events}
		if wait == pb.Wait_WAIT_SETTLED && !x.Committed.Settled {
			return outcome, &NotSettled{Outcome: outcome}
		}
		return outcome, nil
	}
	return Outcome{}, errors.New("the kernel answered without an outcome")
}

// Enqueue stores the command in the kernel's queue and returns its ID without waiting for it to
// run. Submit it again with the same ID to get the outcome. With At, a scheduled command: it runs
// at that time.
func (c *Client) Enqueue(ctx context.Context, cmd Command) (string, error) {
	res, err := c.send(ctx, cmd, pb.Wait_WAIT_ACCEPTED)
	if err != nil {
		return "", err
	}
	return res.CommandId, nil
}

func (c *Client) send(ctx context.Context, cmd Command, wait pb.Wait) (*pb.CommandResult, error) {
	if !cmd.Until.IsZero() && cmd.At.IsZero() {
		return nil, errors.New("Until needs At")
	}
	id := cmd.ID
	if id == "" {
		id = newUUID()
	}
	var vault *vaultFields
	if d := c.domain(cmd.Domain); d != nil {
		if s, ok := d.commands[cmd.Type]; ok {
			vault = &s.vault
		}
	}
	sealed, err := seal(cmd.Data, vault, cmd.AggregateID)
	if err != nil {
		return nil, err
	}
	rc := c.context
	if cmd.Context != nil {
		rc = *cmd.Context
	}
	if rc.CorrelationID == "" {
		rc.CorrelationID = id
	}
	w := &pb.Command{
		CommandId: id, TenantId: firstOf(cmd.Tenant, c.tenant), Domain: cmd.Domain, AggregateType: cmd.AggregateType,
		AggregateId: cmd.AggregateID, CommandType: cmd.Type, Payload: sealed.payload, Context: rc.wire(), VaultRefs: sealed.refs,
	}
	if !cmd.At.IsZero() {
		w.DeliverAt = cmd.At.UnixMilli()
	}
	if !cmd.Until.IsZero() {
		w.DeliverUntil = cmd.Until.UnixMilli()
	}
	w.ClientSig = c.key.sign(commandMessage(w))
	listener := handlingListener(ctx)
	return route(ctx, c.parts, w.TenantId, func(cl *clients) (*pb.CommandResult, error) {
		res, err := cl.submit(ctx, &pb.SubmitRequest{Command: w, Wait: wait, Listener: listener, Vault: sealed.values}, cmd.Timeout)
		if err != nil {
			return nil, err
		}
		if x, ok := res.Outcome.(*pb.CommandResult_Committed); ok {
			pos := x.Committed.Position
			if n := len(x.Committed.Events); n > 0 {
				pos = max(pos, x.Committed.Events[n-1].GlobalPosition)
			}
			cl.saw(pos)
		}
		return res, nil
	})
}

// CancelOutcome is what came of Client.Cancel.
type CancelOutcome struct {
	Cancelled bool
	// Not cancelled: too_late (due, running or done already), not_found, or a middleware's code.
	Code    string
	Message string
}

// CancelOptions are the tenant and context of Client.Cancel.
type CancelOptions struct {
	Tenant  string
	Context *RequestContext
	Timeout time.Duration
}

// Cancel cancels a scheduled command (Enqueue with At) before it is due: it completes as
// rejected cancelled. Needs submit for the command's type; middlewares check it as a cancel of
// that type.
func (c *Client) Cancel(ctx context.Context, commandID string, options ...CancelOptions) (CancelOutcome, error) {
	var o CancelOptions
	if len(options) > 0 {
		o = options[0]
	}
	rc := c.context
	if o.Context != nil {
		rc = *o.Context
	}
	req := &pb.CancelRequest{CommandId: commandID, TenantId: firstOf(o.Tenant, c.tenant), Context: rc.wire(), IssuedAt: issuedNow()}
	req.ClientSig = c.key.sign(cancelMessage(req))
	res, err := route(ctx, c.parts, req.TenantId, func(cl *clients) (*pb.CancelResult, error) { return cl.cancel(ctx, req, o.Timeout) })
	if err != nil {
		return CancelOutcome{}, err
	}
	if r, ok := res.Outcome.(*pb.CancelResult_Rejected); ok && r.Rejected != nil {
		return CancelOutcome{Code: r.Rejected.Code, Message: r.Rejected.Message}, nil
	}
	return CancelOutcome{Cancelled: true}, nil
}

// Reindex has these aggregates store the unique keys and facts their current state yields, e.g.
// after declaring a key on an aggregate type with existing aggregates: each runs the built-in
// reindex command of its type, which produces no events. It returns the outcome per ID, once
// committed.
func (c *Client) Reindex(ctx context.Context, domain, aggregateType string, aggregateIDs []string, template ...Command) (map[string]Outcome, error) {
	base := Command{}
	if len(template) > 0 {
		base = template[0]
	}
	outcomes := map[string]Outcome{}
	seen := map[string]bool{}
	var ids []string
	for _, id := range aggregateIDs {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	// A few at a time, so a long list does not open thousands of calls at once.
	for i := 0; i < len(ids); i += 16 {
		batch := ids[i:min(i+16, len(ids))]
		var wg sync.WaitGroup
		var mu sync.Mutex
		var first error
		for _, id := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cmd := base
				cmd.Domain, cmd.AggregateType, cmd.AggregateID, cmd.Type, cmd.Data, cmd.Committed, cmd.ID = domain, aggregateType, id, ReindexType(aggregateType), map[string]any{}, true, ""
				o, err := c.Submit(ctx, cmd)
				mu.Lock()
				defer mu.Unlock()
				if err != nil && first == nil {
					first = err
				}
				outcomes[id] = o
			}()
		}
		wg.Wait()
		if first != nil {
			return outcomes, first
		}
	}
	return outcomes, nil
}

// Query is a query for Client.Query: which query type to ask, with which data. The kernel
// routes it to the read model that answers domain.Type.
type Query struct {
	// The domain of the query type: shop.
	Domain string
	// The query type within the domain: GetCart.
	Type string
	// The query's data, sent as JSON.
	Data any
	// Default: the client's tenant.
	Tenant string
	// Replaces the client's default context.
	Context *RequestContext
	// Answer only after the read model has processed this global position. Default 0.
	MinPosition uint64
	// How long to wait for the connection and the answer; default Options.Timeout.
	Timeout time.Duration
	// For a read model across tenants with partitions: answer only once the read model has
	// processed every partition's events up to the query.
	AllPartitions bool
}

// Query asks the read model that answers q.Domain.q.Type and returns its answer as JSON.
// Queries always go through the kernel, also within one process, so grants and middleware apply.
// It returns a *Rejected if the read model or a middleware rejects it.
func (c *Client) Query(ctx context.Context, q Query) (json.RawMessage, error) {
	payload, err := encode(q.Data)
	if err != nil {
		return nil, err
	}
	rc := c.context
	if q.Context != nil {
		rc = *q.Context
	}
	req := &pb.QueryRequest{
		QueryId: newUUID(), TenantId: firstOf(q.Tenant, c.tenant), Domain: q.Domain, QueryType: q.Type, Payload: payload,
		Context: rc.wire(), MinPosition: q.MinPosition, IssuedAt: issuedNow(),
		// Not signed: read models of this deployment answer first.
		Deployment: c.deployment,
	}
	if d := c.domain(q.Domain); d != nil {
		if s, ok := d.queries[q.Type]; ok && s.target != nil {
			req.Target = targetOf(s.target, q.Domain, payload)
		}
	}
	listener := handlingListener(ctx)
	if q.AllPartitions {
		// A wait-only query to every partition at once: each returns once the read model has
		// processed that partition's head.
		wait := &pb.QueryRequest{QueryId: req.QueryId, TenantId: req.TenantId, Domain: req.Domain, QueryType: req.QueryType,
			Context: req.Context, IssuedAt: req.IssuedAt, WaitOnly: true, Target: req.Target, Deployment: req.Deployment, Listener: listener}
		wait.ClientSig = c.key.sign(queryMessage(wait))
		parts, err := c.parts.all(ctx, q.Timeout)
		if err != nil {
			return nil, err
		}
		errs := make(chan error, len(parts))
		for _, p := range parts {
			go func() { _, err := p.clients.query(ctx, wait, q.Timeout); errs <- err }()
		}
		for range parts {
			if err := <-errs; err != nil {
				return nil, err
			}
		}
		req.IssuedAt = issuedNow()
	}
	res, err := route(ctx, c.parts, req.TenantId, func(cl *clients) (*pb.QueryResult, error) {
		asked := proto.Clone(req).(*pb.QueryRequest)
		// Read your writes, on whichever copy answers: the query waits for this client's own
		// newest command. Not from a handler, which must not wait behind its own listener.
		if listener == "" {
			asked.MinPosition = max(asked.MinPosition, cl.seen())
		}
		asked.Listener = listener
		asked.ClientSig = c.key.sign(queryMessage(asked))
		return cl.query(ctx, asked, q.Timeout)
	})
	if err != nil {
		return nil, err
	}
	switch x := res.Outcome.(type) {
	case *pb.QueryResult_Rejected:
		r := x.Rejected
		if r == nil {
			r = &pb.Rejected{}
		}
		return nil, &Rejected{Code: r.Code, Message: r.Message}
	case *pb.QueryResult_Response:
		// Compact, whatever the language of the read model that answered.
		var b bytes.Buffer
		if err := json.Compact(&b, orEmpty(x.Response)); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}
	return nil, errors.New("the kernel answered without an outcome")
}

// QueryAs asks a query like Client.Query and decodes the answer into T.
func QueryAs[T any](ctx context.Context, c *Client, q Query) (T, error) {
	var out T
	raw, err := c.Query(ctx, q)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}

// targetOf is the wire target of a query: the declared aggregate type, qualified, and the ID
// from the data.
func targetOf(t *queryTarget, domain string, payload []byte) *pb.Target {
	data, _ := decodeAny(payload)
	obj, _ := data.(map[string]any)
	id, _ := obj[t.field].(string)
	if id == "" {
		return nil // no ID: no target, so a rule that needs one refuses
	}
	typ := t.aggregateType
	if !strings.Contains(typ, ".") {
		typ = domain + "." + typ
	}
	return &pb.Target{AggregateType: typ, AggregateId: id}
}

// ProcessAct sets as whom the process name (qualified: shop.fulfilment) acts: the sender
// tenant, identity and session of request go with every command and query the kernel's engine
// sends for it from now on. Idempotent: false if the process acted so already.
func (c *Client) ProcessAct(ctx context.Context, name string, request RequestContext) (bool, error) {
	req := &pb.ProcessActing{Name: name, Context: request.wire(), IssuedAt: issuedNow()}
	req.Sig = c.key.sign(processActMessage(req.Name, req.Context, req.IssuedAt))
	control, err := c.parts.control(ctx)
	if err != nil {
		return false, err
	}
	r, err := control.processAct(ctx, req)
	if err != nil {
		return false, err
	}
	return r.Changed, nil
}

// ProcessVersion is a process as the kernel registered it.
type ProcessVersion struct {
	// Qualified: shop.fulfilment.
	Name    string
	Version uint32
	// Whether this registration made a new version.
	New bool
}

// RegisterProcess registers a business process with the kernel. A model equal to the newest
// version registers nothing (New false); a different one becomes the next version. Client.App
// registers the processes of its domains itself.
func (c *Client) RegisterProcess(ctx context.Context, p *ProcessBuilder) (ProcessVersion, error) {
	model, err := p.Model()
	if err != nil {
		return ProcessVersion{}, err
	}
	b, err := json.Marshal(model)
	if err != nil {
		return ProcessVersion{}, err
	}
	issued := issuedNow()
	req := &pb.ProcessRegistration{Model: b, IssuedAt: issued, Sig: c.key.sign(processMessage(b, issued))}
	control, err := c.parts.control(ctx)
	if err != nil {
		return ProcessVersion{}, err
	}
	r, err := control.registerProcess(ctx, req)
	if err != nil {
		return ProcessVersion{}, err
	}
	return ProcessVersion{Name: r.Name, Version: r.Version, New: r.New}, nil
}

// Switch says when a membership switches its service to its deployment while another one
// serves: "caught-up" (default), "now" or "never".
type Switch string

const (
	CaughtUp Switch = "caught-up"
	Now      Switch = "now"
	Never    Switch = "never"
)

// Manifest is what a service consists of, for Client.Apply: its membership, which tells the
// kernel what to wait for.
type Manifest struct {
	// Default: this process's service.
	Service string
	// The aggregates the service runs; the kernel waits for a worker of each.
	Workers []AnyAggregate
	// The listeners the kernel waits for.
	Listeners []*Listener
	// The service's listeners that may check requests.
	Middlewares []*Listener
	// Free metadata, e.g. version.
	Metadata map[string]string
	// The service's business processes, qualified (shop.fulfilment).
	Processes []string
	// The deployment that serves the service. Default: this process's.
	Deployment string
	// When it switches (default CaughtUp).
	Switch Switch
}

// WaitsForSwitch is whether the kernel refused a membership because it would switch its service
// to a deployment that has not caught up yet, back to one that served before, or at all with
// Never: applying it again may succeed later.
func WaitsForSwitch(err error) bool {
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.FailedPrecondition {
		return false
	}
	m := s.Message()
	return strings.HasPrefix(m, "not_caught_up") || strings.HasPrefix(m, "not_serving") || strings.HasPrefix(m, "served_before")
}

// Apply replaces the membership of a service (default: this process's). Apply it once per
// deploy, not from every process: the newest apply wins. The key needs manage for the service.
func (c *Client) Apply(ctx context.Context, m Manifest) error {
	service := firstOf(m.Service, c.service)
	if service == "" {
		return errors.New("apply needs a service: set Service in the manifest or the options")
	}
	deployment := firstOf(m.Deployment, c.deployment)
	if deployment != "" && !deployments.MatchString(deployment) {
		return fmt.Errorf("a deployment has 1 to 128 letters, digits, `-`, `_` and `.`, not %q", deployment)
	}
	ms := &pb.Membership{Service: service, IssuedAt: issuedNow(), Processes: m.Processes, Deployment: deployment}
	for _, a := range m.Workers {
		ms.Workers = append(ms.Workers, a.domain().Name+"."+a.typeName())
	}
	for _, l := range m.Listeners {
		ms.Listeners = append(ms.Listeners, l.Name)
	}
	for _, l := range m.Middlewares {
		ms.Middlewares = append(ms.Middlewares, l.Name)
	}
	for _, k := range sortedKeys(m.Metadata) {
		ms.Metadata = append(ms.Metadata, &pb.Attribute{Key: k, Value: m.Metadata[k]})
	}
	sw := m.Switch
	if sw == "" {
		sw = CaughtUp
	}
	control, err := c.parts.control(ctx)
	if err != nil {
		return err
	}
	return control.apply(ctx, &pb.ApplyRequest{
		Membership: ms, Sig: c.key.sign(membershipMessage(ms)),
		Force: sw == Now, NoSwitch: sw == Never, NoRollback: sw == CaughtUp,
	})
}

// Subscription says what Client.Subscribe streams.
type Subscription struct {
	// Global position of the last event already handled; 0 from the beginning.
	After uint64
	// Qualified event types (orders.OrderPlaced) or domains (orders.*); empty: all the key may read.
	Types []string
	// The first pause before reconnecting (default 1 s).
	Reconnect time.Duration
}

// Subscribe streams committed events after s.After (backlog first, then live) to handler, one at
// a time, until ctx ends; it reconnects and resumes after the last handled event; if the handler
// fails, that event is delivered again. Unlike a listener, a subscription is not registered: the
// kernel keeps no position, nobody waits for it, and events arrive as stored (not upcast, vault
// fields null). With partitions, one stream per partition, each from the start, in order within a
// partition. The key needs subscribe for the types.
func (c *Client) Subscribe(ctx context.Context, s Subscription, handler func(ctx context.Context, event StoredEvent) error) error {
	if s.Reconnect <= 0 {
		s.Reconnect = time.Second
	}
	parts := c.partitions(ctx, "subscription", s.Reconnect)
	if parts == nil {
		return nil
	}
	if len(parts) == 1 && parts[0].name == "" {
		c.subscribe(ctx, parts[0].clients, s, "", handler)
		return nil
	}
	// The handler takes one event at a time, of whichever partition.
	var turn sync.Mutex
	inTurn := func(ctx context.Context, e StoredEvent) error {
		turn.Lock()
		defer turn.Unlock()
		return handler(ctx, e)
	}
	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := s
			sub.After = 0
			c.subscribe(ctx, p.clients, sub, p.name, inTurn)
		}()
	}
	wg.Wait()
	return nil
}

func (c *Client) subscribe(ctx context.Context, cl *clients, s Subscription, part string, handler func(context.Context, StoredEvent) error) {
	what := "subscription"
	if part != "" {
		what += " " + part
	}
	position := s.After
	attempt := 0
	for ctx.Err() == nil {
		from := position
		err := func() error {
			sctx, cancel := context.WithCancel(ctx)
			defer cancel()
			issued := issuedNow()
			req := &pb.SubscribeRequest{AfterPosition: position, EventTypes: s.Types, IssuedAt: issued, Protocol: Protocol,
				ClientSig: c.key.sign(subscribeMessage(position, s.Types, issued))}
			conn, failed := cl.stream()
			stream, err := pb.NewEventServiceClient(conn).Subscribe(cl.outgoing(sctx), req)
			if err != nil {
				failed(err)
				return err
			}
			for {
				e, err := stream.Recv()
				if err != nil {
					failed(err)
					return err
				}
				stored, err := storedOf(e, nil)
				if err != nil {
					return err
				}
				if err := handler(ctx, stored); err != nil {
					return err
				}
				position = e.GlobalPosition
			}
		}()
		if ctx.Err() != nil {
			return
		}
		if position > from {
			attempt = 1
		} else {
			attempt++
		}
		c.log.reconnect(ctx, what, err, s.Reconnect, attempt)
	}
}

// DataOptions say which tenant's file or value, and who asks.
type DataOptions struct {
	// Default: the client's tenant.
	Tenant string
	// Who asks; signed, and what middlewares checking the request decide on.
	Context *RequestContext
	// Put: the content type stored with the file (default application/octet-stream).
	ContentType string
	// List: the Next of the page before.
	After string
	// Cache set: how long until the value expires; default until deleted or the kernel restarts.
	TTL time.Duration
}

// Data is the files of a domain kept by the kernel under (tenant, domain, key), at most 16 MiB
// each (the kernel's --max-data-bytes). Reading needs a grant of data_read for the domain,
// putting and deleting one of data_write. Files are not part of the signed event log and are not
// erased with the vault. Each request passes the middleware chain first, as <domain>.PutData,
// .GetData, .DeleteData or .ListData; a refusal is a PERMISSION_DENIED error.
type Data struct{ c *Client }

// File is a file as Data.Get returns it.
type File struct {
	Data        []byte
	ContentType string
	// SHA-256 of the data, hex.
	SHA256 string
}

// FileEntry is a file as Data.List lists it.
type FileEntry struct {
	Key         string
	ContentType string
	Size        uint64
}

func (d *Data) request(op, domain, key string, o DataOptions, data []byte, contentType string) *pb.DataRequest {
	rc := d.c.context
	if o.Context != nil {
		rc = *o.Context
	}
	r := &pb.DataRequest{TenantId: firstOf(o.Tenant, d.c.tenant), Domain: domain, Key: key, Data: data, ContentType: contentType,
		IssuedAt: issuedNow(), Context: rc.wire(), After: o.After}
	r.ClientSig = d.c.key.sign(dataMessage(op, r))
	return r
}

func dataOptions(options []DataOptions) DataOptions {
	if len(options) > 0 {
		return options[0]
	}
	return DataOptions{}
}

// Put stores data under key, replacing a file there, and returns its SHA-256 (hex) as the kernel
// computed it: for an event to name.
func (d *Data) Put(ctx context.Context, domain, key string, data []byte, options ...DataOptions) (string, error) {
	o := dataOptions(options)
	req := d.request("put", domain, key, o, data, firstOf(o.ContentType, "application/octet-stream"))
	res, err := route(ctx, d.c.parts, req.TenantId, func(cl *clients) (*pb.DataResult, error) { return cl.dataCall(ctx, "put", req) })
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(res.Sha256), nil
}

// Get returns the file under key, or nil if there is none.
func (d *Data) Get(ctx context.Context, domain, key string, options ...DataOptions) (*File, error) {
	req := d.request("get", domain, key, dataOptions(options), nil, "")
	res, err := route(ctx, d.c.parts, req.TenantId, func(cl *clients) (*pb.DataResult, error) { return cl.dataCall(ctx, "get", req) })
	if err != nil || !res.Found {
		return nil, err
	}
	return &File{Data: res.Data, ContentType: res.ContentType, SHA256: hex.EncodeToString(res.Sha256)}, nil
}

// Delete deletes the file under key and returns whether there was one.
func (d *Data) Delete(ctx context.Context, domain, key string, options ...DataOptions) (bool, error) {
	req := d.request("delete", domain, key, dataOptions(options), nil, "")
	res, err := route(ctx, d.c.parts, req.TenantId, func(cl *clients) (*pb.DataResult, error) { return cl.dataCall(ctx, "delete", req) })
	if err != nil {
		return false, err
	}
	return res.Found, nil
}

// List lists the files whose keys start with prefix (needs data_read), in the order of their
// keys' bytes, up to 1,000 after options.After; next, if not empty, is the After of the next page.
func (d *Data) List(ctx context.Context, domain, prefix string, options ...DataOptions) (files []FileEntry, next string, err error) {
	req := d.request("list", domain, prefix, dataOptions(options), nil, "")
	res, err := route(ctx, d.c.parts, req.TenantId, func(cl *clients) (*pb.DataList, error) { return cl.listData(ctx, req) })
	if err != nil {
		return nil, "", err
	}
	for _, f := range res.Files {
		files = append(files, FileEntry{Key: f.Key, ContentType: f.ContentType, Size: f.Size})
	}
	return files, res.Next, nil
}

// Cache is the shared cache of a domain: values of at most 1 MiB under (tenant, domain, key)
// that may expire. Needs a grant of cache for the domain. The cache lives in the kernel's memory:
// a restart empties it. Each request passes the middleware chain first, as <domain>.SetCache,
// .GetCache or .DeleteCache.
type Cache struct{ c *Client }

func (k *Cache) request(op, domain, key string, o DataOptions, value []byte) *pb.CacheRequest {
	rc := k.c.context
	if o.Context != nil {
		rc = *o.Context
	}
	r := &pb.CacheRequest{TenantId: firstOf(o.Tenant, k.c.tenant), Domain: domain, Key: key, Value: value,
		TtlMs: uint64(o.TTL.Milliseconds()), IssuedAt: issuedNow(), Context: rc.wire()}
	r.ClientSig = k.c.key.sign(cacheMessage(op, r))
	return r
}

// Set stores value under key, replacing a value there.
func (k *Cache) Set(ctx context.Context, domain, key string, value []byte, options ...DataOptions) error {
	req := k.request("set", domain, key, dataOptions(options), value)
	_, err := route(ctx, k.c.parts, req.TenantId, func(cl *clients) (*pb.CacheResult, error) { return cl.cacheCall(ctx, "set", req) })
	return err
}

// Get returns the live value under key, or nil if there is none or it expired.
func (k *Cache) Get(ctx context.Context, domain, key string, options ...DataOptions) ([]byte, error) {
	req := k.request("get", domain, key, dataOptions(options), nil)
	res, err := route(ctx, k.c.parts, req.TenantId, func(cl *clients) (*pb.CacheResult, error) { return cl.cacheCall(ctx, "get", req) })
	if err != nil || !res.Found {
		return nil, err
	}
	return res.Value, nil
}

// Delete deletes the value under key and returns whether there was a live one.
func (k *Cache) Delete(ctx context.Context, domain, key string, options ...DataOptions) (bool, error) {
	req := k.request("delete", domain, key, dataOptions(options), nil)
	res, err := route(ctx, k.c.parts, req.TenantId, func(cl *clients) (*pb.CacheResult, error) { return cl.cacheCall(ctx, "delete", req) })
	if err != nil {
		return false, err
	}
	return res.Found, nil
}

func sortedKeys(m map[string]string) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
