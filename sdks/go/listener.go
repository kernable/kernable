package kernable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// parkAfter is how often a reactor's handler may fail on one event before it parks it.
const parkAfter = 5

var (
	qualifiedOrDomain = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*\.([^.]+|\*)$`)
	qualifiedType     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*\.[^.*]+$`)
)

// ListenerOptions say how a listener runs: its place in the order, where it starts, whether it
// is live, and where it keeps its state. Domain.ReadModel and Domain.Reactor set the defaults;
// the ListenerOption functions change them.
type ListenerOptions struct {
	// Listeners with a lower order process each event first; equal orders run in parallel.
	// Defaults: 0 for read models, 10 for reactors. Negative orders are reserved for the kits.
	Order int32
	// Position to start after on the first connect: 0 rebuilds from the beginning (default for
	// read models); nil, the kernel resumes after the last acknowledged event, and a new
	// listener starts at the newest event (default for reactors).
	Start *uint64
	// Not registered with the kernel: no stored position, always starts at the newest event,
	// nobody waits for it, and it is forgotten when it disconnects. Cannot be a middleware.
	Live bool
	// Runs this process's listener as one instance of a group under the listener's name.
	// Default: KERNABLE_INSTANCE.
	Instance string
	// Whether the instances take turns (default for reactors), or every instance gets every
	// event (default for read models).
	Single *bool
	// How long to wait before reconnecting after the stream ends (default 1 s).
	Reconnect time.Duration
	// Keeps the listener's state in a store (sqlite:<path> or memory:) instead of memory.
	Store string
	// Version of the stored data's format (default "1"); a change clears the store.
	Schema string
	// How often a handler may fail on one event before the listener parks it and moves on
	// (default 5 for reactors, 0 for read models: never). A *Rejected parks at once; the kernel
	// being unreachable or busy, NotSettled or the store failing never does.
	ParkAfter int
}

// ListenerOption changes a listener's options.
type ListenerOption func(*ListenerOptions)

// Order sets the listener's place in the order.
func Order(n int32) ListenerOption { return func(o *ListenerOptions) { o.Order = n } }

// StartAfter starts the listener after position on its first connect (0: from the beginning).
func StartAfter(position uint64) ListenerOption {
	return func(o *ListenerOptions) { o.Start = &position }
}

// StartAtNewest starts a new listener at the newest event (the default for reactors).
func StartAtNewest() ListenerOption { return func(o *ListenerOptions) { o.Start = nil } }

// Live makes the listener live: not registered, always from the newest event.
func Live() ListenerOption { return func(o *ListenerOptions) { o.Live = true } }

// Instance runs the listener as one instance of a group.
func Instance(name string) ListenerOption { return func(o *ListenerOptions) { o.Instance = name } }

// Single says whether a group's instances take turns.
func Single(single bool) ListenerOption { return func(o *ListenerOptions) { o.Single = &single } }

// ReconnectAfter sets the first pause before reconnecting.
func ReconnectAfter(d time.Duration) ListenerOption {
	return func(o *ListenerOptions) { o.Reconnect = d }
}

// WithStore keeps the listener's state in a store: sqlite:<path> or memory:.
func WithStore(url string) ListenerOption { return func(o *ListenerOptions) { o.Store = url } }

// StoreSchema sets the version of the stored data's format.
func StoreSchema(schema string) ListenerOption { return func(o *ListenerOptions) { o.Schema = schema } }

// ParkAfter sets how often a handler may fail on one event before the listener parks it.
func ParkAfter(n int) ListenerOption { return func(o *ListenerOptions) { o.ParkAfter = n } }

// When is where a middleware sits: before a request (it may refuse it) or after it.
type When int

const (
	Before When = iota
	After
)

// MiddlewareOptions say what a middleware checks and where it sits in the chain.
type MiddlewareOptions struct {
	// Request types it checks: `*` (all), a type of the listener's own domain (Type), another
	// domain's type (domain.Type) or a whole domain (domain.*). These also cover the file and
	// cache requests of a domain under the types PutData, GetData, DeleteData, ListData,
	// SetCache, GetCache and DeleteCache.
	Types []string
	// Before (default): checks a request before the kernel accepts it; an error refuses it.
	// After: gets the request's outcome too.
	When When
	// Position in the chain: lower runs first (default 0). Negative orders are reserved.
	Order int32
}

// Check is a request a middleware checks: a command or query, before the kernel accepts or
// routes it, or after; or a file or cache request, before it runs.
type Check struct {
	// command, query, data, cache or cancel.
	Kind string
	// The command or query ID; empty for a file or cache request.
	ID     string
	Tenant string
	Domain string
	// The type within the domain: the command or query type, or PutData, GetData, … for files and
	// cache.
	Type string
	// Commands only.
	AggregateType, AggregateID string
	// The aggregate the request is about, qualified (shop.order): a command's aggregate, or the
	// one a query declares. Empty for queries that declare none, and for file and cache requests.
	TargetType, TargetID string
	// File and cache requests only: the file's or value's key.
	Key string
	// The command's or query's data; nil for a file or cache request.
	Data json.RawMessage
	// Who sends it: check permissions here.
	Request RequestContext
}

// QueryOutcome is what came of a query: its response or why it was rejected.
type QueryOutcome struct {
	Answered bool
	Data     json.RawMessage
	Code     string
	Message  string
}

// MiddlewareContext is what a middleware handler gets first. After a request, Outcome (a
// command) or QueryOutcome (a query) says what came of it.
type MiddlewareContext struct {
	context.Context
	// The listener's store, if it has one.
	Store *Store
	// After a command.
	Outcome *Outcome
	// After a query.
	QueryOutcome *QueryOutcome
}

// QueryContext is what a query handler gets first.
type QueryContext struct {
	context.Context
	// A new UUID per query.
	QueryID string
	// The query type within the domain.
	Type   string
	Domain string
	Tenant string
	// Who asks: check permissions here.
	Request RequestContext
	// The listener's store, if it has one.
	Store *Store
}

// ListenerContext is what an event handler gets first: the event, the listener's store, and
// follow-up commands and queries on the listener's connection.
type ListenerContext struct {
	context.Context
	// The event, its data upcast (unless the handler is Raw) and its vault fields filled in as
	// far as this key may read them.
	Event StoredEvent
	// The event was committed before this connection began: a read model rebuilding, and
	// equally the backlog a reactor catches up on. A reactor must not skip side effects because
	// of it; submit follow-ups with fixed command IDs instead.
	Replaying bool
	// The listener's store, if it has one.
	Store *Store
	h     *ListenerHandle
}

// Follow is a request context for commands caused by this event: same sender and correlation,
// the event's command as causation, without ExpectedVersion and BypassAuthorization.
func (c *ListenerContext) Follow() RequestContext { return follow(c.Event) }

func follow(e StoredEvent) RequestContext {
	r := e.Request
	r.ExpectedVersion, r.BypassAuthorization = nil, false
	if r.CorrelationID == "" {
		r.CorrelationID = e.CommandID
	}
	r.CausationID = e.CommandID
	return r
}

// Submit submits a command this event causes, on the listener's connection, and returns its
// events. It needs a fixed ID (confirm-<order>), so a redelivered event runs it once. The
// domain defaults to the listener's, the context to Follow(), the tenant to the event's. A
// rejection returns a *Rejected (with the kernel's key or requirement): a reactor then parks
// the event at once.
func (c *ListenerContext) Submit(cmd Command) ([]StoredEvent, error) {
	if cmd.ID == "" {
		return nil, fmt.Errorf("%s: a follow-up command needs a fixed ID, so a redelivered event runs it once", cmd.Type)
	}
	if cmd.Domain == "" {
		cmd.Domain = c.h.listener.Domain.Name
	}
	if cmd.Tenant == "" {
		cmd.Tenant = c.Event.Tenant
	}
	if cmd.Context == nil {
		f := follow(c.Event)
		cmd.Context = &f
	}
	outcome, err := c.h.c.Submit(c, cmd)
	if err != nil {
		return nil, err
	}
	if outcome.Committed {
		return outcome.Events, nil
	}
	return nil, &Rejected{Code: outcome.Code, Message: outcome.Message, Key: outcome.Key, Requirement: outcome.Requirement}
}

// Query asks a query on the listener's connection: queryType is of the listener's domain
// (GetOrder) or qualified (crm.GetCustomer). The tenant defaults to the event's. It is the
// listener's own read: it keeps the client's context, not the event's sender.
func (c *ListenerContext) Query(queryType string, data any) (json.RawMessage, error) {
	domain, t := c.h.listener.Domain.Name, queryType
	if i := strings.IndexByte(queryType, '.'); i >= 0 {
		domain, t = queryType[:i], queryType[i+1:]
	}
	return c.h.c.Query(c, Query{Domain: domain, Type: t, Data: data, Tenant: c.Event.Tenant})
}

// ProcessAct sets as whom the process name acts (Client.ProcessAct), on the listener's
// connection: how an authorization kit hands a process its identity.
func (c *ListenerContext) ProcessAct(name string, request RequestContext) (bool, error) {
	return c.h.c.ProcessAct(c, name, request)
}

// HandlerOption is an option of one event handler.
type HandlerOption func(*eventHandler)

// Raw gets events as stored, without upcasting, whatever their schema (for archives and exports).
func Raw() HandlerOption { return func(h *eventHandler) { h.raw = true } }

type eventHandler struct {
	fn  func(*ListenerContext, any) error
	raw bool
}

type middleware struct {
	options MiddlewareOptions
	fn      func(*MiddlewareContext, Check) error
}

// Listener is the definition of a durable, named event listener: which events it handles
// (On), which queries it answers (Answer) and what it checks as a middleware (Middleware).
// Define it on a domain with Domain.ReadModel or Domain.Reactor and run it with Client.Listener
// or Client.App.
//
// Type names without a domain (ItemAdded, GetCart) are of the listener's own domain; types of
// other domains are qualified (identity.AccountRegistered) or name a whole domain (identity.*).
// Handlers get event payloads upcast to the versions of the listener's own domain and of the
// domains it Uses.
type Listener struct {
	// The domain the listener is defined on.
	Domain *Domain
	// The listener's name; the kernel tracks its position under it.
	Name        string
	options     ListenerOptions
	handlers    map[string]*eventHandler
	queries     map[string]func(*QueryContext, json.RawMessage) (any, error)
	middlewares map[When]*middleware
	domains     map[string]*Domain
	reactor     bool
}

func newListener(d *Domain, name string, options ListenerOptions, reactor bool) *Listener {
	return &Listener{
		Domain: d, Name: name, options: options, reactor: reactor,
		handlers: map[string]*eventHandler{}, queries: map[string]func(*QueryContext, json.RawMessage) (any, error){},
		middlewares: map[When]*middleware{}, domains: map[string]*Domain{d.Name: d},
	}
}

// Options are the options the listener was defined with.
func (l *Listener) Options() ListenerOptions { return l.options }

// Use upcasts the events of these other domains with their event schemas.
func (l *Listener) Use(domains ...*Domain) *Listener {
	for _, d := range domains {
		l.domains[d.Name] = d
	}
	return l
}

func (l *Listener) qualify(t string) string {
	if unqualified.MatchString(t) {
		return l.Domain.Name + "." + t
	}
	return t
}

// On handles events of eventType: a type of the own domain (ItemAdded), of another domain
// (identity.AccountRegistered) or a whole domain (identity.*): fn(ctx, data) gets the upcast
// payload as D, and the event in ctx.Event. A handler for the exact type wins over one for its
// domain; one handler per type. The listener acknowledges an event only after the handler
// returned; if it fails, the event is delivered again. Panics for a malformed type.
func On[D any](l *Listener, eventType string, fn func(ctx *ListenerContext, data D) error, options ...HandlerOption) {
	if !unqualified.MatchString(eventType) && !qualifiedOrDomain.MatchString(eventType) {
		panic(fmt.Sprintf("kernable: listen to a type (Type), a qualified type (domain.Type) or a domain (domain.*), not %q", eventType))
	}
	h := &eventHandler{fn: func(ctx *ListenerContext, data any) error {
		d, err := into[D](data)
		if err != nil {
			return err
		}
		return fn(ctx, d)
	}}
	for _, o := range options {
		o(h)
	}
	l.handlers[l.qualify(eventType)] = h
}

// Answer answers the query type queryType, of the own domain (GetCart) or another one
// (orders.GetOrder), from this listener's state. The kernel delivers queries in order with the
// events, so an answer reflects every event handled before it. fn(ctx, data) returns the answer,
// sent as JSON; a *Rejected refuses, any other error refuses with handler_error. Panics for a
// malformed type.
func Answer[D, R any](l *Listener, queryType string, fn func(ctx *QueryContext, data D) (R, error)) {
	if !unqualified.MatchString(queryType) && !qualifiedType.MatchString(queryType) {
		panic(fmt.Sprintf("kernable: answer a query type (Query) or a qualified one (domain.Query), not %q", queryType))
	}
	l.queries[l.qualify(queryType)] = func(ctx *QueryContext, payload json.RawMessage) (any, error) {
		var d D
		if err := decodeInto(payload, &d); err != nil {
			return nil, err
		}
		return fn(ctx, d)
	}
}

// Middleware makes this listener a middleware: the kernel asks fn about every command, query,
// file and cache request of options.Types, before accepting or routing it (or after), in chain
// order, and only once this listener has caught up with the events. Return a *Rejected to
// refuse; any other error refuses too. A listener has at most one middleware before and one
// after. Panics for a live listener, a malformed type, or a second middleware at the same When.
func (l *Listener) Middleware(options MiddlewareOptions, fn func(ctx *MiddlewareContext, check Check) error) *Listener {
	if l.options.Live {
		panic(fmt.Sprintf("kernable: %s is live: a live listener cannot be a middleware", l.Name))
	}
	var types []string
	for _, t := range options.Types {
		if t != "*" && !unqualified.MatchString(t) && !qualifiedOrDomain.MatchString(t) {
			panic(fmt.Sprintf("kernable: check `*`, a type (Type), a qualified type (domain.Type) or a domain (domain.*), not %q", t))
		}
		if t == "*" {
			types = append(types, t)
		} else {
			types = append(types, l.qualify(t))
		}
	}
	if _, ok := l.middlewares[options.When]; ok {
		panic(fmt.Sprintf("kernable: %s has a middleware %s already", l.Name, whenName(options.When)))
	}
	options.Types = types
	l.middlewares[options.When] = &middleware{options, fn}
	return l
}

func whenName(w When) string {
	if w == After {
		return "after"
	}
	return "before"
}

// details are what a listener does, one part per entry: where it starts, what it reads, answers
// and checks.
func (l *Listener) details(position uint64, name func(string) string) []string {
	list := func(types []string) string {
		slices.Sort(types)
		var out []string
		for _, t := range types {
			out = append(out, name(t))
		}
		return strings.Join(out, ", ")
	}
	var reads, answers []string
	for t := range l.handlers {
		reads = append(reads, t)
	}
	for t := range l.queries {
		answers = append(answers, t)
	}
	first := fmt.Sprintf("from %d", position)
	if len(reads) > 0 {
		first += " · reads " + list(reads)
	}
	out := []string{first}
	if len(answers) > 0 {
		out = append(out, "answers "+list(answers))
	}
	for _, w := range []When{Before, After} {
		if m, ok := l.middlewares[w]; ok {
			out = append(out, fmt.Sprintf("checks %s %s", list(slices.Clone(m.options.Types)), whenName(w)))
		}
	}
	return out
}

// ListenerHandle is a listener running over a connection: Client.Listener(definition). Events
// arrive in order, one at a time, after every listener with a lower order has processed them.
// Delivery is at least once: if a handler fails, the listener reconnects and the event is
// delivered again, until a reactor gives up on it and parks it. With a store, a batch's data and
// position are written together, so it takes effect once.
type ListenerHandle struct {
	c        *Client
	listener *Listener
	service  string
	// Whether the first connect gets its own line; an App prints it in its overview instead.
	announce bool
	readyCh  chan struct{}
	once     sync.Once
	store    *Store

	mu sync.Mutex
	// Position after which the kernel delivered on the first connect.
	startedAt uint64
	// Events skipped because this code does not know them.
	skipped int
	// Batches of all partitions take turns on the shared store: one at a time.
	turns sync.Mutex
	// How far each partition's stream has handled, for the order across moves.
	progress map[string]uint64
	moved    chan struct{}
}

func newListenerHandle(c *Client, l *Listener, service string) *ListenerHandle {
	return &ListenerHandle{c: c, listener: l, service: service, announce: true, readyCh: make(chan struct{}), progress: map[string]uint64{}, moved: make(chan struct{})}
}

// Ready is closed once the kernel accepted the listener (on every partition).
func (h *ListenerHandle) Ready() <-chan struct{} { return h.readyCh }

// Listener is the definition it runs.
func (h *ListenerHandle) Listener() *Listener { return h.listener }

// Skipped is the number of events skipped because this code does not know them: a schema newer
// than the listener's domains declare, written by a newer deployment.
func (h *ListenerHandle) Skipped() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.skipped
}

func (h *ListenerHandle) what() string { return "listener " + h.listener.Name }

// Run opens the store (if any; trying again while it cannot be opened), connects, and
// processes events, queries and checks until ctx ends, reconnecting after the last handled
// event when the stream drops or a handler fails. It returns nil once ctx ends, having closed the
// store.
func (h *ListenerHandle) Run(ctx context.Context) error {
	o := h.listener.options
	reconnect := o.Reconnect
	if reconnect <= 0 {
		reconnect = time.Second
	}
	parts := h.c.partitions(ctx, h.what(), reconnect)
	if parts == nil {
		return nil
	}
	if o.Store != "" {
		for attempt := 1; h.store == nil; attempt++ {
			schema := o.Schema
			if schema == "" {
				schema = "1"
			}
			s, err := openStore(ctx, o.Store, schema)
			if err == nil {
				h.store = s
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			h.c.log.storeFailed(ctx, h.what(), err, reconnect, attempt)
			if ctx.Err() != nil {
				return nil
			}
		}
		defer func() { h.store.close(); h.store = nil }()
	}
	// A stream per partition, each from its own position in the shared store.
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		ready.Add(1)
		s := &listenerStream{h: h, clients: p.clients, part: p.name}
		go func() {
			defer wg.Done()
			s.loop(ctx, reconnect, ready.Done)
		}()
	}
	go func() { ready.Wait(); h.once.Do(func() { close(h.readyCh) }) }()
	wg.Wait()
	return nil
}

// advanced notes that partition part's stream has handled everything up to position.
func (h *ListenerHandle) advanced(part string, position uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p, ok := h.progress[part]; ok && p >= position {
		return
	}
	h.progress[part] = position
	close(h.moved)
	h.moved = make(chan struct{})
}

// reached waits until partition part's stream has handled up to position, or 30 s.
func (h *ListenerHandle) reached(ctx context.Context, part string, position uint64, tenant string) error {
	deadline := time.After(30 * time.Second)
	for {
		h.mu.Lock()
		p, ok := h.progress[part]
		moved := h.moved
		h.mu.Unlock()
		if ok && p >= position {
			return nil
		}
		select {
		case <-moved:
		case <-ctx.Done():
			return errStopped
		case <-deadline:
			h.c.log.print(levelWarn, h.what(), fmt.Sprintf("tenant %s moved here from partition %s, whose stream has not passed position %d in 30 s; going on without its order", tenant, part, position))
			return nil
		}
	}
}

// listenerStream is a listener's stream to one kernel (or one partition).
type listenerStream struct {
	h        *ListenerHandle
	clients  *clients
	part     string
	position uint64
	// Newest event when the kernel accepted the listener; events up to it are replayed.
	replayUpTo uint64
	connects   int
	// How often the handler failed on an event, by position, until it gets past it.
	attempts map[uint64]int
	// Tenants it read the map again for.
	checked map[string]bool
}

func (s *listenerStream) loop(ctx context.Context, reconnect time.Duration, ready func()) {
	h := s.h
	var once sync.Once
	s.attempts, s.checked = map[uint64]int{}, map[string]bool{}
	start := h.listener.options.Start
	if h.store != nil {
		// A new (or cleared) store says nothing: the listener's own start applies (a reactor
		// starts at the newest event).
		if s.position = h.store.committedFor(s.part); s.position > 0 {
			p := s.position
			start = &p
		}
	}
	attempt := 0
	for ctx.Err() == nil {
		before := s.connects
		err := s.listen(ctx, start, func() { once.Do(ready) })
		if ctx.Err() != nil {
			return
		}
		if s.connects > before {
			attempt = 1
		} else {
			attempt++
		}
		// Resume after the last handled event, once there is one: before the first Ready, the
		// position says nothing (a new reactor would replay everything from 0).
		if s.connects > 0 {
			p := s.position
			start = &p
		}
		if err == nil {
			err = errors.New("stream ended")
		}
		h.c.log.reconnect(ctx, h.what(), err, reconnect, attempt)
	}
}

type listenerKey struct{}

// handlingListener is the listener whose handler runs in ctx: requests sent from it name it, so
// the kernel never makes them wait for it.
func handlingListener(ctx context.Context) string {
	name, _ := ctx.Value(listenerKey{}).(string)
	return name
}

func (s *listenerStream) listen(ctx context.Context, start *uint64, ready func()) error {
	h := s.h
	l := h.listener
	o := l.options
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, failed := s.clients.stream()
	stream, err := pb.NewListenerServiceClient(conn).Listen(s.clients.outgoing(ctx))
	if err != nil {
		failed(err)
		return err
	}
	instance := o.Instance
	if instance == "" {
		instance = os.Getenv("KERNABLE_INSTANCE")
	}
	single := instance != "" && o.Single != nil && *o.Single && !o.Live
	hello := &pb.Hello{
		Name: l.Name, KeyId: h.c.key.ID, Protocol: Protocol, Order: o.Order, Service: h.service, Live: o.Live,
		Instance: instance, Single: single, Deployment: h.c.deployment, Reactor: l.reactor, FromPosition: start,
	}
	for t := range l.handlers {
		hello.EventTypes = append(hello.EventTypes, t)
	}
	for t := range l.queries {
		hello.QueryTypes = append(hello.QueryTypes, t)
	}
	slices.Sort(hello.EventTypes)
	slices.Sort(hello.QueryTypes)
	for _, w := range []When{Before, After} {
		if m, ok := l.middlewares[w]; ok {
			hello.Middlewares = append(hello.Middlewares, &pb.Middleware{Order: m.options.Order, Types: m.options.Types, When: pb.When(w)})
		}
	}
	if err := stream.Send(&pb.ListenerMessage{Msg: &pb.ListenerMessage_Hello{Hello: hello}}); err != nil {
		failed(err)
		return err
	}
	// Requests sent from the handlers name this listener.
	hctx := context.WithValue(ctx, listenerKey{}, l.Name)
	for {
		m, err := stream.Recv()
		if err != nil {
			failed(err)
			if errors.Is(err, io.EOF) {
				return errors.New("stream ended")
			}
			return err
		}
		reply, err := s.watched(m, func() (*pb.ListenerMessage, error) { return s.onMessage(hctx, m, ready) })
		if err != nil {
			return err
		}
		if reply != nil {
			if err := stream.Send(reply); err != nil {
				failed(err)
				return err
			}
		}
	}
}

// watched runs f, logging an error if it is still running after too long: a query or check
// after 5 s, a batch after 30 s (everything behind it waits).
func (s *listenerStream) watched(m *pb.ListenerDelivery, f func() (*pb.ListenerMessage, error)) (*pb.ListenerMessage, error) {
	var what string
	var after time.Duration
	switch x := m.Msg.(type) {
	case *pb.ListenerDelivery_Batch:
		what, after = fmt.Sprintf("the batch up to %d", x.Batch.UpTo), 30*time.Second
	case *pb.ListenerDelivery_Query:
		what, after = fmt.Sprintf("the query %s.%s", x.Query.Query.GetDomain(), x.Query.Query.GetQueryType()), 5*time.Second
	case *pb.ListenerDelivery_Check:
		what, after = "a middleware check", 5*time.Second
	default:
		return f()
	}
	started := time.Now()
	t := time.AfterFunc(after, func() {
		s.h.c.log.print(levelError, s.h.what(), fmt.Sprintf("still handling %s after %d s: everything behind it waits; is a handler stuck?", what, int(time.Since(started).Seconds())))
	})
	defer t.Stop()
	return f()
}

func (s *listenerStream) onMessage(ctx context.Context, m *pb.ListenerDelivery, ready func()) (*pb.ListenerMessage, error) {
	h := s.h
	switch x := m.Msg.(type) {
	case *pb.ListenerDelivery_Challenge:
		return &pb.ListenerMessage{Msg: &pb.ListenerMessage_ChallengeResponse{ChallengeResponse: &pb.ChallengeResponse{
			Sig: h.c.key.sign(challengeMessage(x.Challenge.Nonce)).Sig,
		}}}, nil
	case *pb.ListenerDelivery_Ready:
		s.position = x.Ready.AfterPosition
		h.advanced(s.part, s.position)
		s.replayUpTo = x.Ready.HeadPosition
		s.logConnected()
		ready()
	case *pb.ListenerDelivery_Waiting:
		// Another instance runs the group, or another deployment serves: this one is ready to
		// take over.
		w := x.Waiting
		switch {
		case w.ReaderOf != "":
			h.c.log.print(levelInfo, h.what(), fmt.Sprintf("waiting: this kernel is a reader; reactors act on the writer (%s)", w.ReaderOf))
		case w.Serving != "":
			h.c.log.print(levelInfo, h.what(), fmt.Sprintf("waiting: deployment %s serves; this reactor acts once its own deployment does", w.Serving))
		default:
			h.c.log.print(levelInfo, h.what(), fmt.Sprintf("waiting: instance %s is active", w.Active))
		}
		ready()
	case *pb.ListenerDelivery_Batch:
		b := x.Batch
		// A kernel that goes back (a bug, or not the kernel) must not make a store apply events
		// twice: the session ends, and the next resumes after what was handled.
		if b.UpTo < s.position {
			return nil, fmt.Errorf("the kernel sent a batch up to %d, behind position %d", b.UpTo, s.position)
		}
		if err := s.inOrder(ctx, b.Events); err != nil {
			return nil, err
		}
		parked, skipped, err := s.batch(ctx, b)
		if err != nil {
			return nil, err
		}
		s.position = b.UpTo
		h.advanced(s.part, s.position)
		h.mu.Lock()
		h.skipped += len(skipped)
		h.mu.Unlock()
		return &pb.ListenerMessage{Msg: &pb.ListenerMessage_Ack{Ack: &pb.Ack{Position: b.UpTo, Parked: parked, Skipped: skipped}}}, nil
	case *pb.ListenerDelivery_Query:
		answer := s.answer(ctx, x.Query.Query)
		answer.CallId = x.Query.CallId
		return &pb.ListenerMessage{Msg: &pb.ListenerMessage_Answer{Answer: answer}}, nil
	case *pb.ListenerDelivery_Check:
		return &pb.ListenerMessage{Msg: &pb.ListenerMessage_Verdict{Verdict: &pb.Verdict{CallId: x.Check.CallId, Rejected: s.verdict(ctx, x.Check)}}}, nil
	}
	return nil, nil
}

// batch handles a batch with the store's transaction around it; the partitions' batches take
// turns on a shared store.
func (s *listenerStream) batch(ctx context.Context, b *pb.Batch) (parked []*pb.Parked, skipped []*pb.Skipped, err error) {
	h := s.h
	h.turns.Lock()
	defer h.turns.Unlock()
	st := h.store
	if st != nil {
		if err := st.begin(); err != nil {
			return nil, nil, err
		}
	}
	parked, skipped, err = s.handleBatch(ctx, b)
	if err == nil && st != nil {
		erased := false
		for _, e := range append(slices.Clone(b.Events), b.Retries...) {
			erased = erased || e.Erases != ""
		}
		// After an erasure, nothing the handlers removed may stay in the store's files.
		err = st.commit(b.UpTo, erased, s.part)
	}
	if err != nil && st != nil {
		st.rollback()
		s.position = st.committedFor(s.part)
	}
	return parked, skipped, err
}

// inOrder: before a batch of a partition's stream, the events of a tenant that moved here after
// its move wait until the stream of the partition it came from has passed the tenant's last
// event there, so a tenant's events keep their order across the move.
func (s *listenerStream) inOrder(ctx context.Context, events []*pb.Event) error {
	if s.part == "" || len(events) == 0 {
		return nil
	}
	p := s.h.c.parts
	m, err := p.current(ctx, false, 0)
	if err != nil || m == nil {
		return err
	}
	known := func(m *pb.PartitionMap, tenant string) bool {
		if partitionOf(m, tenant) == s.part {
			return true
		}
		for _, x := range m.Moved {
			if x.Tenant == tenant && (x.To == s.part || x.From == s.part) {
				return true
			}
		}
		return false
	}
	for _, e := range events {
		if !known(m, e.TenantId) && !s.checked[e.TenantId] {
			// The tenant came here after the map was read; another kernel may take a moment to hold the new one.
			s.checked[e.TenantId] = true
			for i := 0; i < 3 && !known(m, e.TenantId); i++ {
				if i > 0 {
					sleep(ctx, 500*time.Millisecond)
				}
				if fresh, err := p.current(ctx, true, 0); err == nil && fresh != nil {
					m = fresh
				}
			}
		}
		for _, mv := range m.Moved {
			if mv.Tenant != e.TenantId || mv.To != s.part || e.GlobalPosition <= mv.TargetAfter {
				continue
			}
			if !slices.ContainsFunc(m.Partitions, func(x *pb.Partition) bool { return x.Name == mv.From }) {
				continue
			}
			if err := s.h.reached(ctx, mv.From, mv.SourceHead, e.TenantId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *listenerStream) logConnected() {
	h := s.h
	if s.connects == 0 {
		h.mu.Lock()
		h.startedAt = s.position
		h.mu.Unlock()
		if h.announce {
			parts := append([]string{"connected to " + h.c.log.peer}, h.listener.details(s.position, func(t string) string { return t })...)
			h.c.log.print(levelInfo, h.what(), strings.Join(parts, " · "))
		}
	} else {
		h.c.log.print(levelInfo, h.what(), fmt.Sprintf("connected again at position %d", s.position))
	}
	s.connects++
}

// warnedUnknown are the unknown event types and schemas a listener warned about, as
// domain.Type@schema: once per process.
var warnedUnknown sync.Map

// handleBatch handles a batch: first the retries, then the events after the position. It returns
// the events it parked and those it skipped as unknown; it fails if a handler failed on one it
// does not park.
func (s *listenerStream) handleBatch(ctx context.Context, b *pb.Batch) ([]*pb.Parked, []*pb.Skipped, error) {
	h := s.h
	l := h.listener
	o := l.options
	park := o.ParkAfter
	if o.Live {
		park = 0
	}
	instance := o.Instance
	if instance == "" {
		instance = os.Getenv("KERNABLE_INSTANCE")
	}
	// Replicated instances (not single) cannot park.
	replicated := instance != "" && !(o.Single != nil && *o.Single)
	var parked []*pb.Parked
	var skipped []*pb.Skipped
	type item struct {
		e     *pb.Event
		retry bool
	}
	var items []item
	for _, e := range b.Retries {
		items = append(items, item{e, true})
	}
	for _, e := range b.Events {
		items = append(items, item{e, false})
	}
	for _, it := range items {
		e := it.e
		if !it.retry && e.GlobalPosition <= s.position {
			continue // handled already
		}
		typ := e.Domain + "." + e.EventType
		entry := l.handlers[typ]
		if entry == nil {
			entry = l.handlers[e.Domain+".*"]
		}
		var known uint32
		if entry != nil && !entry.raw {
			known = newerThanKnown(l.domains[e.Domain], e.EventType, e.Schema)
		}
		switch {
		case entry != nil && known != 0:
			// Newer than this code: a newer deployment wrote it. No handler call.
			if park > 0 && !replicated {
				msg := fmt.Sprintf("%s schema %d: this code reads it up to %d; a newer deployment wrote it", typ, e.Schema, known)
				h.c.log.print(levelWarn, h.what(), fmt.Sprintf("%s at %d parked: %s", typ, e.GlobalPosition, msg))
				parked = append(parked, &pb.Parked{Position: e.GlobalPosition, Code: "unknown_event", Message: msg, Attempts: 1})
			} else {
				skipped = append(skipped, &pb.Skipped{Position: e.GlobalPosition, EventType: typ, Schema: e.Schema})
				if _, warned := warnedUnknown.LoadOrStore(fmt.Sprintf("%s@%d", typ, e.Schema), true); !warned {
					h.c.log.print(levelWarn, h.what(), fmt.Sprintf("skipped %s schema %d: this code reads it up to %d; a newer deployment wrote it", typ, e.Schema, known))
				}
			}
		case entry != nil:
			stored, err := storedOf(e, b.Vault)
			if err != nil {
				return nil, nil, err
			}
			data, err := decodeAny(stored.Data)
			if err != nil {
				return nil, nil, err
			}
			if !entry.raw {
				if data, err = upcast(l.domains[e.Domain], e.Domain, e.EventType, e.Schema, data); err != nil {
					return nil, nil, err
				}
				if stored.Data, err = encode(data); err != nil {
					return nil, nil, err
				}
				if def, ok := l.domains[e.Domain]; ok {
					if es, ok := def.events[e.EventType]; ok {
						stored.Schema = es.Schema
					} else {
						stored.Schema = 1
					}
				}
			}
			lctx := &ListenerContext{Context: ctx, Event: stored, Replaying: e.GlobalPosition <= s.replayUpTo, Store: h.store, h: h}
			saved := h.store != nil && park > 0
			if saved {
				if err := h.store.savepoint(); err != nil {
					return nil, nil, err
				}
			}
			herr := callHandler(func() error { return entry.fn(lctx, data) })
			if herr == nil {
				if saved {
					if err := h.store.release(); err != nil {
						return nil, nil, err
					}
				}
				delete(s.attempts, e.GlobalPosition)
				h.c.log.print(levelDebug, h.what(), fmt.Sprintf("%s at %d", typ, e.GlobalPosition))
			} else {
				// An erasing event is never parked: its handler must get past it, or what it removes would stay behind.
				limit := park
				if e.Erases != "" {
					limit = 0
				}
				p, err := s.giveUp(typ, e.GlobalPosition, herr, limit)
				if err != nil {
					return nil, nil, err
				}
				parked = append(parked, p)
				if saved {
					if err := h.store.rollbackEvent(); err != nil {
						return nil, nil, err
					}
				}
			}
		}
		if !it.retry {
			s.position = e.GlobalPosition
		}
	}
	return parked, skipped, nil
}

// callHandler runs a handler, turning a panic into an error.
func callHandler(f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return f()
}

// busy are the codes of a kernel that is away or busy.
var busy = map[codes.Code]bool{
	codes.Canceled: true, codes.DeadlineExceeded: true, codes.ResourceExhausted: true,
	codes.Aborted: true, codes.Internal: true, codes.Unavailable: true,
}

// transient is whether a failed handler should simply try again: the kernel was unreachable or
// busy, a submit did not settle in time, or the store failed. Never parks.
func transient(err error) bool {
	var ns *NotSettled
	if errors.As(err, &ns) {
		return true
	}
	if s, ok := status.FromError(err); ok && s.Code() != codes.OK && busy[s.Code()] {
		return true
	}
	return strings.Contains(err.Error(), "SQLITE") || strings.Contains(err.Error(), "sqlite")
}

// giveUp decides after the handler failed on the event at position: park it (a rejection at
// once, another error after limit attempts), or return err, so the listener gets the event again.
func (s *listenerStream) giveUp(typ string, position uint64, err error, limit int) (*pb.Parked, error) {
	h := s.h
	if limit <= 0 || transient(err) {
		return nil, err
	}
	attempts := s.attempts[position] + 1
	s.attempts[position] = attempts
	var r *Rejected
	rejected := errors.As(err, &r)
	if !rejected && attempts < limit {
		return nil, err
	}
	delete(s.attempts, position)
	if rejected {
		h.c.log.print(levelWarn, h.what(), fmt.Sprintf("%s at %d rejected (%s), parked: %s", typ, position, r.Code, r.Message))
		return &pb.Parked{Position: position, Code: r.Code, Message: r.Message, Attempts: uint32(attempts)}, nil
	}
	h.c.log.print(levelError, h.what(), fmt.Sprintf("%s at %d failed %d times, parked: %v", typ, position, attempts, err))
	return &pb.Parked{Position: position, Code: "handler_error", Message: errorType(err), Attempts: uint32(attempts)}, nil
}

func (s *listenerStream) verdict(ctx context.Context, c *pb.MiddlewareCall) *pb.Rejected {
	when := Before
	if c.When == pb.When_WHEN_AFTER {
		when = After
	}
	m, ok := s.h.listener.middlewares[when]
	if !ok {
		return &pb.Rejected{Code: "not_a_middleware", Message: "no middleware " + whenName(when)}
	}
	check, err := toCheck(c)
	if err != nil {
		return &pb.Rejected{Code: "middleware_error", Message: err.Error()}
	}
	mctx := &MiddlewareContext{Context: ctx, Store: s.h.store}
	if when == After {
		mctx.Outcome, mctx.QueryOutcome = toOutcome(c)
	}
	if err := callHandler(func() error { return m.fn(mctx, check) }); err != nil {
		var r *Rejected
		if errors.As(err, &r) {
			return &pb.Rejected{Code: r.Code, Message: r.Message}
		}
		return &pb.Rejected{Code: "middleware_error", Message: err.Error()}
	}
	return nil
}

func (s *listenerStream) answer(ctx context.Context, q *pb.QueryRequest) *pb.QueryAnswer {
	typ := q.Domain + "." + q.QueryType
	a := s.tryAnswer(ctx, q)
	if r, ok := a.Outcome.(*pb.QueryAnswer_Rejected); ok {
		s.h.c.log.print(levelDebug, s.h.what(), fmt.Sprintf("rejected %s %s", typ, r.Rejected.Code))
	} else {
		s.h.c.log.print(levelDebug, s.h.what(), "answered "+typ)
	}
	return a
}

func (s *listenerStream) tryAnswer(ctx context.Context, q *pb.QueryRequest) *pb.QueryAnswer {
	rejected := func(code, message string) *pb.QueryAnswer {
		return &pb.QueryAnswer{Outcome: &pb.QueryAnswer_Rejected{Rejected: &pb.Rejected{Code: code, Message: message}}}
	}
	handler, ok := s.h.listener.queries[q.Domain+"."+q.QueryType]
	if !ok {
		return rejected("unknown_query", fmt.Sprintf("no handler for %s.%s", q.Domain, q.QueryType))
	}
	qctx := &QueryContext{Context: ctx, QueryID: q.QueryId, Type: q.QueryType, Domain: q.Domain, Tenant: q.TenantId, Request: contextOf(q.Context), Store: s.h.store}
	var result any
	err := callHandler(func() error {
		var err error
		result, err = handler(qctx, q.Payload)
		return err
	})
	if err != nil {
		var r *Rejected
		if errors.As(err, &r) {
			return rejected(r.Code, r.Message)
		}
		return rejected("handler_error", err.Error())
	}
	// No answer is null, not {} as for commands and events without data.
	b := []byte("null")
	if result != nil {
		if b, err = encode(result); err != nil {
			return rejected("handler_error", err.Error())
		}
	}
	return &pb.QueryAnswer{Outcome: &pb.QueryAnswer_Response{Response: b}}
}

// typeIn is the type within its domain of a qualified one: shop.PutData → PutData.
func typeIn(qualified, domain string) string { return strings.TrimPrefix(qualified, domain+".") }

func toCheck(c *pb.MiddlewareCall) (Check, error) {
	switch r := c.Request.(type) {
	case *pb.MiddlewareCall_Data:
		d := r.Data
		return Check{Kind: "data", Tenant: d.TenantId, Domain: d.Domain, Type: typeIn(c.Type, d.Domain), Key: d.Key, Request: contextOf(d.Context)}, nil
	case *pb.MiddlewareCall_Cache:
		d := r.Cache
		return Check{Kind: "cache", Tenant: d.TenantId, Domain: d.Domain, Type: typeIn(c.Type, d.Domain), Key: d.Key, Request: contextOf(d.Context)}, nil
	case *pb.MiddlewareCall_Command:
		x := r.Command
		data, _, err := unseal(x.Payload, c.Vault)
		if err != nil {
			return Check{}, err
		}
		raw, err := encode(data)
		return Check{Kind: "command", ID: x.CommandId, Tenant: x.TenantId, Domain: x.Domain, Type: x.CommandType,
			AggregateType: x.AggregateType, AggregateID: x.AggregateId, TargetType: x.Domain + "." + x.AggregateType, TargetID: x.AggregateId,
			Data: raw, Request: contextOf(x.Context)}, err
	case *pb.MiddlewareCall_Cancel:
		// A cancel of a scheduled command: its type, aggregate and data are the scheduled command's.
		x := c.Scheduled
		if x == nil {
			x = &pb.Command{}
		}
		data, _, err := unseal(x.Payload, nil)
		if err != nil {
			return Check{}, err
		}
		raw, err := encode(data)
		return Check{Kind: "cancel", ID: x.CommandId, Tenant: r.Cancel.TenantId, Domain: x.Domain, Type: x.CommandType,
			AggregateType: x.AggregateType, AggregateID: x.AggregateId, TargetType: x.Domain + "." + x.AggregateType, TargetID: x.AggregateId,
			Data: raw, Request: contextOf(r.Cancel.Context)}, err
	case *pb.MiddlewareCall_Query:
		q := r.Query
		check := Check{Kind: "query", ID: q.QueryId, Tenant: q.TenantId, Domain: q.Domain, Type: q.QueryType, Data: orEmpty(q.Payload), Request: contextOf(q.Context)}
		if q.Target != nil && q.Target.AggregateId != "" {
			check.TargetType, check.TargetID = q.Target.AggregateType, q.Target.AggregateId
		}
		return check, nil
	}
	return Check{}, errors.New("a check of nothing")
}

func orEmpty(payload []byte) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage("{}")
	}
	return payload
}

// toOutcome is what came of the request a middleware after it checks.
func toOutcome(c *pb.MiddlewareCall) (*Outcome, *QueryOutcome) {
	switch r := c.Outcome.(type) {
	case *pb.MiddlewareCall_CommandResult:
		res := r.CommandResult
		if committed, ok := res.Outcome.(*pb.CommandResult_Committed); ok {
			events, _ := storedAll(committed.Committed.Events)
			return &Outcome{Committed: true, Events: events}, nil
		}
		var rej *pb.Rejected
		if x, ok := res.Outcome.(*pb.CommandResult_Rejected); ok {
			rej = x.Rejected
		}
		o := rejectedOutcome(rej)
		return &o, nil
	case *pb.MiddlewareCall_QueryResult:
		res := r.QueryResult
		if x, ok := res.Outcome.(*pb.QueryResult_Response); ok {
			return nil, &QueryOutcome{Answered: true, Data: orEmpty(x.Response)}
		}
		q := &QueryOutcome{}
		if x, ok := res.Outcome.(*pb.QueryResult_Rejected); ok && x.Rejected != nil {
			q.Code, q.Message = x.Rejected.Code, x.Rejected.Message
		}
		return nil, q
	}
	return nil, nil
}
