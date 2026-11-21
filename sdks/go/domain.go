package kernable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

var (
	domainName  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*$`)
	unqualified = regexp.MustCompile(`^[^.*]+$`)
	deployments = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
)

// reindexPrefix is what the built-in reindex commands start with; reserved for them.
const reindexPrefix = "_reindex_"

// ReindexType is the command an aggregate type with unique keys or facts runs itself, one per
// aggregate type because command types are unique within a domain: _reindex_Customer. See
// Client.Reindex.
func ReindexType(aggregateType string) string { return reindexPrefix + aggregateType }

// Domain is a name that scopes its aggregates, commands and events, lowercase by convention
// (orders) like aggregates, while message types are capitalized (orders.PlaceOrder). Types are
// unique within a domain; elsewhere they are named domain.Type.
//
// Everything is defined on it: aggregates (NewAggregate), read models (ReadModel), reactors
// (Reactor), business processes (Process), event versions and vault fields (Event, Command).
// Client.App runs all of it; Client.Worker and Client.Listener run parts. Definitions panic on
// a programming error (a malformed name, a type defined twice), as regexp.MustCompile does.
type Domain struct {
	// The domain's name: shop.
	Name       string
	events     map[string]*EventSchema
	commands   map[string]*CommandSchema
	queries    map[string]*QuerySchema
	aggregates []aggregateDef
	listeners  []*Listener
	processes  []*ProcessBuilder
}

// NewDomain creates a domain named name: a letter, then letters, digits, `_` or `-`. Processes
// that share a domain (a worker and a read model in another process) each create it with the
// same name and the same Event and Command declarations.
func NewDomain(name string) *Domain {
	if !domainName.MatchString(name) {
		panic(fmt.Sprintf("kernable: invalid domain name %q", name))
	}
	return &Domain{Name: name, events: map[string]*EventSchema{}, commands: map[string]*CommandSchema{}, queries: map[string]*QuerySchema{}}
}

// Upcast turns a payload of one version into one of the next (as generic JSON: objects are
// map[string]any, numbers json.Number).
type Upcast func(data any) (any, error)

// EventSchema is the payload format of one event type: its current version, the steps that
// bring older payloads up to it, and which fields go to the vault. Create it with
// Domain.Event; its methods return it, so they chain. Events are stored as written and never
// changed; readers upcast them.
type EventSchema struct {
	// The event type within its domain, unqualified.
	Type string
	// The current payload version (from 1): what workers write and readers get.
	Schema uint32
	steps  map[uint32]Upcast
	vault  vaultFields
	eraser Subject
}

// Event declares the payload format of eventType events (unqualified): this domain writes them
// at version schema (from 1), with an Upcast per older version, and may keep fields in the
// vault (Personal) or erase a subject (Erases). Types not declared are at version 1.
func (d *Domain) Event(eventType string, schema int) *EventSchema {
	if !unqualified.MatchString(eventType) {
		panic(fmt.Sprintf("kernable: event types are unqualified within their domain, not %q", eventType))
	}
	if schema < 1 {
		panic(fmt.Sprintf("kernable: a schema is a version from 1, not %d", schema))
	}
	def := &EventSchema{Type: eventType, Schema: uint32(schema), steps: map[uint32]Upcast{}}
	d.events[eventType] = def
	return def
}

// Personal keeps these top-level fields in the vault, as personal data of the subject that
// subject returns from the payload and the aggregate's ID. Events then hold only references;
// the kernel hands the values to workers and to listeners allowed to read them, until an event
// erases the subject.
func (s *EventSchema) Personal(fields []string, subject Subject) *EventSchema {
	s.vault.personal = append(s.vault.personal, personalFields{fields, subject})
	return s
}

// Erases makes this an erasing event: it deletes every vault value of the subject that subject
// returns, in every stream, and the snapshots that may hold them.
func (s *EventSchema) Erases(subject Subject) *EventSchema {
	s.eraser = subject
	return s
}

// Upcast turns a payload of version from into one of version from+1. Steps chain: version 1
// reaches 3 through Upcast(1, …) and Upcast(2, …). Panics unless from is between 1 and
// Schema-1. A reader that lacks a step stops at such an event instead of misreading it.
func (s *EventSchema) Upcast(from int, fn Upcast) *EventSchema {
	if from < 1 || uint32(from) >= s.Schema {
		panic(fmt.Sprintf("kernable: upcast %s from a version between 1 and %d, not %d", s.Type, s.Schema-1, from))
	}
	s.steps[uint32(from)] = fn
	return s
}

// CommandSchema is how a command's payload is kept: which fields go to the vault instead of
// the signed log. Create it with Domain.Command. The submitting client seals the fields, so it
// needs the domain: Client.Use(domain).
type CommandSchema struct {
	// The command type within its domain, unqualified.
	Type  string
	vault vaultFields
}

// Command declares which fields of commandType commands (unqualified) the vault keeps. Only
// needed for commands with such fields.
func (d *Domain) Command(commandType string) *CommandSchema {
	if !unqualified.MatchString(commandType) {
		panic(fmt.Sprintf("kernable: command types are unqualified within their domain, not %q", commandType))
	}
	def := &CommandSchema{Type: commandType}
	d.commands[commandType] = def
	return def
}

// Personal keeps these top-level fields in the vault as personal data of subject; see
// EventSchema.Personal.
func (s *CommandSchema) Personal(fields []string, subject Subject) *CommandSchema {
	s.vault.personal = append(s.vault.personal, personalFields{fields, subject})
	return s
}

// Secret keeps these top-level fields (passwords, one-time tokens) in the vault only until the
// command completed. The handler gets them in clear; the stored command keeps a reference.
func (s *CommandSchema) Secret(fields ...string) *CommandSchema {
	s.vault.secret = append(s.vault.secret, fields...)
	return s
}

// QuerySchema is what a domain declares about a query type: the aggregate it is about.
type QuerySchema struct {
	// The query type within its domain, unqualified.
	Type   string
	target *queryTarget
}

type queryTarget struct{ aggregateType, field string }

// Query declares what queryType queries (unqualified) are about; chain Target on it. Clients
// that ask such queries need the domain: Client.Use(domain).
func (d *Domain) Query(queryType string) *QuerySchema {
	if !unqualified.MatchString(queryType) {
		panic(fmt.Sprintf("kernable: query types are unqualified within their domain, not %q", queryType))
	}
	def := &QuerySchema{Type: queryType}
	d.queries[queryType] = def
	return def
}

// Target says the query is about one aggregate of aggregateType (order, or qualified
// shop.order), whose ID is in the data's field. The SDK sends it with the query, signed, and a
// middleware decides on it as on a command's aggregate.
func (s *QuerySchema) Target(aggregateType, field string) *QuerySchema {
	s.target = &queryTarget{aggregateType, field}
	return s
}

// ReadModel defines a read model named name: a listener that builds a view from events (On)
// and answers queries from it (Answer). Without a store it is kept in memory and rebuilt from
// the first event on every start (order 0, start 0); with a Store it continues where it left
// off. Panics if the domain has a listener of that name already.
func (d *Domain) ReadModel(name string, options ...ListenerOption) *Listener {
	zero := uint64(0)
	l := newListener(d, name, ListenerOptions{Order: 0, Start: &zero}, false)
	for _, o := range options {
		o(&l.options)
	}
	return d.listen(l)
}

// Reactor defines a reactor named name: a listener that triggers side effects and follow-up
// commands. It starts at the newest event when it is new, then resumes after the last
// acknowledged event (order 10), so it never reacts to what happened before it existed.
// Submit follow-ups with ListenerContext.Submit and a fixed command ID; a rejected one parks the
// event (see ParkAfter). Panics if the domain has a listener of that name already.
func (d *Domain) Reactor(name string, options ...ListenerOption) *Listener {
	single := true
	l := newListener(d, name, ListenerOptions{Order: 10, ParkAfter: parkAfter, Single: &single}, true)
	for _, o := range options {
		o(&l.options)
	}
	return d.listen(l)
}

func (d *Domain) listen(l *Listener) *Listener {
	for _, x := range d.listeners {
		if x.Name == l.Name {
			panic(fmt.Sprintf("kernable: %s has a listener %s already", d.Name, l.Name))
		}
	}
	d.listeners = append(d.listeners, l)
	return l
}

// Process defines a business process name (fulfilment, qualified shop.fulfilment): returns the
// ProcessBuilder to chain its start and steps on. Client.App registers the processes of its
// domains; Client.RegisterProcess registers one alone. Panics if the domain has a process of
// that name already, or for a name other than letters, digits, `_` and `-`.
func (d *Domain) Process(name string) *ProcessBuilder {
	for _, p := range d.processes {
		if p.Name == name {
			panic(fmt.Sprintf("kernable: %s has a process %s already", d.Name, name))
		}
	}
	p := newProcessBuilder(d.Name, name)
	d.processes = append(d.processes, p)
	return p
}

// schemaError is an event a process cannot read: its schema is newer than known, or a step is
// missing.
type schemaError struct{ message string }

func (e *schemaError) Error() string { return e.message }

// newerThanKnown is the newest schema of an event type this code knows, if schema is newer: a
// newer deployment wrote it. 0 if the code knows it.
func newerThanKnown(d *Domain, eventType string, schema uint32) uint32 {
	known := uint32(1)
	if d != nil {
		if def, ok := d.events[eventType]; ok {
			known = def.Schema
		}
	}
	if schema > known {
		return known
	}
	return 0
}

// upcast brings data of an event stored at schema up to the current version.
func upcast(d *Domain, domain, eventType string, schema uint32, data any) (any, error) {
	var def *EventSchema
	if d != nil {
		def = d.events[eventType]
	}
	current := uint32(1)
	if def != nil {
		current = def.Schema
	}
	if schema == 0 {
		schema = 1
	}
	if schema > current {
		return nil, &schemaError{fmt.Sprintf("%s.%s has schema %d, but this process knows it only up to %d: update it", domain, eventType, schema, current)}
	}
	for v := schema; v < current; v++ {
		step, ok := def.steps[v]
		if !ok {
			return nil, &schemaError{fmt.Sprintf("no upcast for %s.%s from schema %d", domain, eventType, v)}
		}
		var err error
		if data, err = step(data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// into turns generic JSON into D.
func into[D any](data any) (D, error) {
	var d D
	if raw, ok := any(&d).(*json.RawMessage); ok {
		b, err := encode(data)
		*raw = b
		return d, err
	}
	if p, ok := any(&d).(*any); ok {
		*p = data
		return d, nil
	}
	b, err := encode(data)
	if err != nil {
		return d, err
	}
	err = json.Unmarshal(b, &d)
	return d, err
}

// clone is a deep copy of a state through JSON, as a snapshot would store it.
func clone[S any](s S) (S, error) {
	var out S
	b, err := json.Marshal(s)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	return out, err
}

// ApplyContext is what an Apply function gets first: the event's meta and the request context
// of the command that caused it. Nothing that differs between runs (no clock, no IDs), so a
// replay computes the same state.
type ApplyContext struct {
	EventMeta
	// The request context of the command that caused the event.
	Request RequestContext
}

// CommandContext is what a command handler gets first: the command, who sends it, and the
// clock and IDs that are the same every time it runs. It is a context.Context, done at the
// command's deadline. Create one with NewCommandContext to call a handler directly, e.g. in a
// unit test with Aggregate.Handle.
type CommandContext struct {
	context.Context
	// The command's ID: the idempotency key (Submit.ID) or a generated UUID.
	CommandID string
	// The command type within the domain.
	Type   string
	Domain string
	// The aggregate type, unqualified: cart.
	AggregateType string
	AggregateID   string
	Tenant        string
	// Who sends the command: check permissions here.
	Request RequestContext
	// When the kernel hands the command to another worker if this one has not answered.
	Deadline time.Time
	at       time.Time
	ids      uint64
}

// NewCommandContext is a context for command commandID, sent with request and received at
// receivedAt; its deadline is 30 s after receivedAt.
func NewCommandContext(ctx context.Context, commandID string, request RequestContext, receivedAt time.Time) *CommandContext {
	return &CommandContext{Context: ctx, CommandID: commandID, Request: request, at: receivedAt, Deadline: receivedAt.Add(30 * time.Second)}
}

// Now is when the kernel received the command: the same every time the command runs. Use it
// instead of time.Now().
func (c *CommandContext) Now() time.Time { return c.at }

// ID is a new ID that is the same every time the command runs (1st call, 2nd call, …). Use it
// instead of random IDs.
func (c *CommandContext) ID() string {
	id := derivedID(c.CommandID, c.ids)
	c.ids++
	return id
}

// FoldEvent is an event to fold (Aggregate.Fold): type, payload version and data, plus as much
// of EventMeta and its request context as known.
type FoldEvent struct {
	EventMeta
	Schema  uint32
	Data    any
	Request RequestContext
}

// HeldKey is a unique key an aggregate holds, as Aggregate.Index returns it.
type HeldKey struct {
	// The key's name, as declared with Aggregate.Unique.
	Name string
	// The value, as the key's function returned it. The kernel gets only its SHA-256 hash.
	Value  string
	Global bool
	// For a personal key: the person the value belongs to.
	Subject string
}

// Index is the unique keys and facts an aggregate holds in a state.
type Index struct {
	// In the order of declaration, the values of a key in the order its function returned them.
	Keys []HeldKey
	// The facts that are true, in the order of declaration.
	Facts []string
}

// AggregateOption sets a snapshot option of NewAggregate.
type AggregateOption func(*aggregateOptions)

type aggregateOptions struct {
	snapshotEvery int
	schema        string
}

// SnapshotEvery sends a snapshot of the state every n events (default 100; 0 turns them off).
func SnapshotEvery(n int) AggregateOption { return func(o *aggregateOptions) { o.snapshotEvery = n } }

// StateSchema is the version of the state format (default "1"). Change it when Apply produces
// differently shaped state; older snapshots are then ignored and the state is rebuilt.
func StateSchema(schema string) AggregateOption {
	return func(o *aggregateOptions) { o.schema = schema }
}

type keyDef[S any] struct {
	name     string
	values   func(S) []string
	global   bool
	personal func(S) string
}

type factDef[S any] struct {
	name string
	fn   func(S) bool
}

// Aggregate is how events change a state (Apply) and which events a command produces (Handle).
// Handlers must be deterministic and free of side effects; they may run again. The state must
// survive a JSON round trip, because snapshots store it as JSON. An aggregate belongs to a
// domain and produces events only of that domain.
//
// A worker folds the aggregate's events (from the latest snapshot, if any) into the state,
// calls the command's handler, and sends the events it returns. The kernel commits them only on
// the version the state was folded from; if another command came first, the command runs again
// on the new state.
type Aggregate[S any] struct {
	// The domain it belongs to.
	Domain *Domain
	// The aggregate type, unqualified: cart.
	Name string
	// The state every aggregate of this type starts from.
	Initial  S
	options  aggregateOptions
	appliers map[string]func(*ApplyContext, *S, any) error
	handlers map[string]func(*CommandContext, S, any) ([]NewEvent, error)
	requires map[string]func(any, S) ([]Requirement, error)
	keys     []keyDef[S]
	facts    []factDef[S]
}

// NewAggregate defines an aggregate type name (cart) of domain d, with the state initial every
// aggregate of it starts from. Add its event appliers and command handlers with Apply and
// Handle; run it with Client.Worker or Client.App. Panics if the domain has an aggregate of
// that name already.
func NewAggregate[S any](d *Domain, name string, initial S, options ...AggregateOption) *Aggregate[S] {
	for _, a := range d.aggregates {
		if a.typeName() == name {
			panic(fmt.Sprintf("kernable: %s has an aggregate %s already", d.Name, name))
		}
	}
	a := &Aggregate[S]{
		Domain: d, Name: name, Initial: initial, options: aggregateOptions{snapshotEvery: 100, schema: "1"},
		appliers: map[string]func(*ApplyContext, *S, any) error{},
		handlers: map[string]func(*CommandContext, S, any) ([]NewEvent, error){},
		requires: map[string]func(any, S) ([]Requirement, error){},
	}
	for _, o := range options {
		o(&a.options)
	}
	d.aggregates = append(d.aggregates, a)
	return a
}

// Apply says how an event of eventType (unqualified, of the aggregate's domain) changes the
// state: fn(ctx, state, data) changes *state. state is a copy, so changing it never touches the
// state it came from; data is upcast to the version declared with Domain.Event. Events without
// an Apply leave the state as it is. Must be deterministic: no clock, no random values, no I/O.
func Apply[S, D any](a *Aggregate[S], eventType string, fn func(ctx *ApplyContext, state *S, data D)) {
	a.appliers[eventType] = func(ctx *ApplyContext, s *S, data any) error {
		d, err := into[D](data)
		if err != nil {
			return fmt.Errorf("%s.%s: %w", a.Domain.Name, eventType, err)
		}
		fn(ctx, s, d)
		return nil
	}
}

// CommandOption is an option of one command handler (Handle).
type CommandOption[S, D any] func(*commandOptions[S, D])

type commandOptions[S, D any] struct {
	requires func(data D, state S) []Requirement
}

// Requires gives the facts other aggregates of the command's tenant must hold for the command
// to commit, from the command's data and the state before it. Checked by the kernel at commit,
// so no command in between can remove a fact unseen; a missing one rejects the command with
// requirement_missing.
func Requires[S, D any](fn func(data D, state S) []Requirement) CommandOption[S, D] {
	return func(o *commandOptions[S, D]) { o.requires = fn }
}

// Handle says which events a commandType command (unqualified, of the aggregate's domain)
// produces: fn(ctx, state, data) gets the current state and the command's data and returns the
// events (possibly none). Return a *Rejected to reject the command. It may run more than once
// for the same command, so it must decide the same way every time: use ctx.Now() and ctx.ID()
// instead of the clock and random IDs, and leave side effects to reactors. Panics for a type
// starting with _reindex_, reserved for the built-in reindex commands.
func Handle[S, D any](a *Aggregate[S], commandType string, fn func(ctx *CommandContext, state S, data D) ([]NewEvent, error), options ...CommandOption[S, D]) {
	if strings.HasPrefix(commandType, reindexPrefix) {
		panic(fmt.Sprintf("kernable: %s: command types starting with %s are reserved for the reindex commands", commandType, reindexPrefix))
	}
	a.handlers[commandType] = func(ctx *CommandContext, s S, data any) ([]NewEvent, error) {
		d, err := into[D](data)
		if err != nil {
			return nil, Reject("invalid_data", fmt.Sprintf("%s: %v", commandType, err))
		}
		return fn(ctx, s, d)
	}
	var o commandOptions[S, D]
	for _, opt := range options {
		opt(&o)
	}
	if o.requires == nil {
		delete(a.requires, commandType)
		return
	}
	a.requires[commandType] = func(data any, s S) ([]Requirement, error) {
		d, err := into[D](data)
		if err != nil {
			return nil, err
		}
		return o.requires(d, s), nil
	}
}

// UniqueOption is an option of a unique key (Aggregate.Unique).
type UniqueOption[S any] func(*keyDef[S])

// Global makes a unique key unique in every tenant, not only within the tenant.
func Global[S any]() UniqueOption[S] { return func(k *keyDef[S]) { k.global = true } }

// PersonalKey makes the key personal: the person (subject) its values belong to, from the
// state. The kernel keeps the key in the vault, and erasing the subject frees it. An empty
// subject: the aggregate holds no value of the key.
func PersonalKey[S any](subject func(S) string) UniqueOption[S] {
	return func(k *keyDef[S]) { k.personal = subject }
}

// Unique declares a unique key name: values(state) returns the values this aggregate holds
// (nil for none), and no other aggregate of this type holds the same value within the tenant
// (or in any tenant, Global). After every command the aggregate holds exactly the values its new
// state yields. A command that would leave the aggregate with a value another one holds is
// rejected with unique_taken. Normalise the value yourself; the kernel gets only its SHA-256.
func (a *Aggregate[S]) Unique(name string, values func(S) []string, options ...UniqueOption[S]) *Aggregate[S] {
	a.named("key", name)
	k := keyDef[S]{name: name, values: values}
	for _, o := range options {
		o(&k)
	}
	a.keys = append(a.keys, k)
	return a
}

// Fact declares a fact name this aggregate holds while fn(state) is true: what other
// aggregates' commands require (Requires). fn must be deterministic, as Apply must.
func (a *Aggregate[S]) Fact(name string, fn func(S) bool) *Aggregate[S] {
	a.named("fact", name)
	a.facts = append(a.facts, factDef[S]{name, fn})
	return a
}

// named checks a key's or fact's name, and adds the built-in reindex command.
func (a *Aggregate[S]) named(what, name string) {
	if name == "" || strings.Contains(name, ".") || len(name) > 128 {
		panic(fmt.Sprintf("kernable: a %s name is non-empty, without dots and at most 128 bytes, not %q", what, name))
	}
	for _, k := range a.keys {
		if k.name == name && what == "key" {
			panic(fmt.Sprintf("kernable: %s has a key %s already", a.Name, name))
		}
	}
	for _, f := range a.facts {
		if f.name == name && what == "fact" {
			panic(fmt.Sprintf("kernable: %s has a fact %s already", a.Name, name))
		}
	}
	a.handlers[ReindexType(a.Name)] = func(*CommandContext, S, any) ([]NewEvent, error) { return nil, nil }
}

// Index is the unique keys and facts this aggregate holds in state, as a worker computes them
// after a command; no kernel needed, so it suits tests.
func (a *Aggregate[S]) Index(state S) Index {
	var out Index
	for _, k := range a.keys {
		values := k.values(state)
		if values == nil {
			continue
		}
		subject := ""
		if k.personal != nil {
			if subject = k.personal(state); subject == "" {
				continue
			}
		}
		seen := map[string]bool{}
		for _, v := range values {
			if !seen[v] {
				seen[v] = true
				out.Keys = append(out.Keys, HeldKey{Name: k.name, Value: v, Global: k.global, Subject: subject})
			}
		}
	}
	for _, f := range a.facts {
		if f.fn(state) {
			out.Facts = append(out.Facts, f.name)
		}
	}
	return out
}

// SchemaOf is the payload version this aggregate writes eventType events at.
func (a *Aggregate[S]) SchemaOf(eventType string) uint32 {
	if def, ok := a.Domain.events[eventType]; ok {
		return def.Schema
	}
	return 1
}

// Fold is the state after history, starting from Initial, as a worker computes it; no kernel
// needed, so it suits tests. Events are upcast first; events without an Apply are skipped.
func (a *Aggregate[S]) Fold(history []FoldEvent) (S, error) { return a.foldFrom(a.Initial, history) }

func (a *Aggregate[S]) foldFrom(from S, history []FoldEvent) (S, error) {
	s, err := clone(from)
	if err != nil {
		return s, err
	}
	for _, e := range history {
		apply, ok := a.appliers[e.Type]
		if !ok {
			continue
		}
		data, err := upcast(a.Domain, a.Domain.Name, e.Type, e.Schema, e.Data)
		if err != nil {
			return s, err
		}
		meta := e.EventMeta
		if meta.Domain == "" {
			meta.Domain = a.Domain.Name
		}
		if meta.AggregateType == "" {
			meta.AggregateType = a.Name
		}
		if err := apply(&ApplyContext{EventMeta: meta, Request: e.Request}, &s, data); err != nil {
			return s, err
		}
	}
	return s, nil
}

// Handle runs the commandType handler on a copy of state, as a worker does, e.g. in a unit
// test, and returns the events it produces. data goes through JSON as it would on the wire.
func (a *Aggregate[S]) Handle(ctx *CommandContext, state S, commandType string, data any) ([]NewEvent, error) {
	handler, ok := a.handlers[commandType]
	if !ok {
		return nil, fmt.Errorf("no handler for %s on %s", commandType, a.Name)
	}
	ctx.Type, ctx.Domain, ctx.AggregateType = commandType, a.Domain.Name, a.Name
	if ctx.Context == nil {
		ctx.Context = context.Background()
	}
	payload, err := encode(data)
	if err != nil {
		return nil, err
	}
	generic, err := decodeAny(payload)
	if err != nil {
		return nil, err
	}
	s, err := clone(state)
	if err != nil {
		return nil, err
	}
	return handler(ctx, s, generic)
}

// aggregateDef is an aggregate whatever its state type: what workers and apps need.
type aggregateDef interface {
	domain() *Domain
	typeName() string
	commandTypes() []string
	eventTypes() []string
	schemaOf(string) uint32
	execute(*execution) (*executed, error)
}

func (a *Aggregate[S]) domain() *Domain          { return a.Domain }
func (a *Aggregate[S]) typeName() string         { return a.Name }
func (a *Aggregate[S]) schemaOf(t string) uint32 { return a.SchemaOf(t) }

func (a *Aggregate[S]) commandTypes() []string {
	var out []string
	for t := range a.handlers {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// eventTypes is the event types it knows: those it applies and those its domain declares.
func (a *Aggregate[S]) eventTypes() []string {
	set := map[string]bool{}
	for t := range a.appliers {
		set[t] = true
	}
	for t := range a.Domain.events {
		set[t] = true
	}
	var out []string
	for t := range set {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// execution is one command for an aggregate, as the kernel leases it.
type execution struct {
	ctx             *CommandContext
	snapshotSchema  string
	snapshotState   []byte
	history         []FoldEvent
	data            any
	expectedVersion uint64
}

// executed is what a command produced: events, and what to index and snapshot.
type executed struct {
	events   []NewEvent
	schemas  []uint32
	keys     []HeldKey
	facts    []string
	requires []Requirement
	indexed  bool
	// The state after the events, encoded, if a snapshot is due.
	snapshot []byte
	schema   string
}

func (a *Aggregate[S]) execute(x *execution) (*executed, error) {
	handler, ok := a.handlers[x.ctx.Type]
	if !ok {
		return nil, Reject("unknown_command", fmt.Sprintf("no handler for %s on %s.%s", x.ctx.Type, a.Domain.Name, a.Name))
	}
	from := a.Initial
	if x.snapshotState != nil {
		if x.snapshotSchema != a.options.schema {
			return nil, Reject("snapshot_stale", fmt.Sprintf("snapshot schema %s is not %s", x.snapshotSchema, a.options.schema))
		}
		// A fresh value: decoding into a copy of Initial would write into maps it shares.
		var snap S
		from = snap
		if err := json.Unmarshal(x.snapshotState, &from); err != nil {
			return nil, Reject("snapshot_stale", fmt.Sprintf("snapshot state: %v", err))
		}
	}
	state, err := a.foldFrom(from, x.history)
	if err != nil {
		return nil, err
	}
	// A copy: a handler that changes it must not change the state the index is computed from.
	given, err := clone(state)
	if err != nil {
		return nil, err
	}
	produced, err := handler(x.ctx, given, x.data)
	if err != nil {
		return nil, err
	}
	out := &executed{events: produced}
	if err := a.inClear(x.ctx.Type, produced); err != nil {
		return nil, err
	}
	for _, e := range produced {
		out.schemas = append(out.schemas, a.SchemaOf(e.Type))
	}
	var after *S
	stateAfter := func() (S, error) {
		if after != nil {
			return *after, nil
		}
		// The events as they will be read back (a JSON round trip), so it equals a replay.
		var read []FoldEvent
		for i, e := range produced {
			payload, err := encode(e.Data)
			if err != nil {
				return state, err
			}
			data, err := decodeAny(payload)
			if err != nil {
				return state, err
			}
			read = append(read, FoldEvent{
				EventMeta: EventMeta{Type: e.Type, Version: x.expectedVersion + uint64(i) + 1, Tenant: x.ctx.Tenant, Domain: x.ctx.Domain,
					AggregateType: x.ctx.AggregateType, AggregateID: x.ctx.AggregateID, CommandID: x.ctx.CommandID},
				Schema: a.SchemaOf(e.Type), Data: data, Request: x.ctx.Request,
			})
		}
		s, err := a.foldFrom(state, read)
		if err != nil {
			return s, err
		}
		after = &s
		return s, nil
	}
	version := x.expectedVersion + uint64(len(produced))
	every := uint64(a.options.snapshotEvery)
	if every > 0 && len(produced) > 0 && version/every != x.expectedVersion/every {
		s, err := stateAfter()
		if err != nil {
			return nil, err
		}
		if out.snapshot, err = encode(s); err != nil {
			return nil, err
		}
		out.schema = a.options.schema
	}
	// The requirements from the data and the state before; keys and facts from the state after.
	if req, ok := a.requires[x.ctx.Type]; ok {
		before, err := clone(state)
		if err != nil {
			return nil, err
		}
		if out.requires, err = req(x.data, before); err != nil {
			return nil, err
		}
	}
	if len(a.keys) > 0 || len(a.facts) > 0 {
		s, err := stateAfter()
		if err != nil {
			return nil, err
		}
		held := a.Index(s)
		out.keys, out.facts = held.Keys, held.Facts
	}
	out.indexed = len(out.keys) > 0 || len(out.facts) > 0 || len(out.requires) > 0
	return out, nil
}

// inClear rejects the command (personal_in_clear) if an event carries a field that the command
// declares personal but the event does not: forgotten on the event, the value would go into the
// signed log in clear, for good.
func (a *Aggregate[S]) inClear(commandType string, events []NewEvent) error {
	schema, ok := a.Domain.commands[commandType]
	if !ok {
		return nil
	}
	var personal []string
	for _, p := range schema.vault.personal {
		personal = append(personal, p.fields...)
	}
	if len(personal) == 0 {
		return nil
	}
	for _, e := range events {
		payload, err := encode(e.Data)
		if err != nil {
			return err
		}
		generic, _ := decodeAny(payload)
		obj, ok := generic.(map[string]any)
		if !ok {
			continue
		}
		sealed := map[string]bool{}
		if def, ok := a.Domain.events[e.Type]; ok {
			for _, p := range def.vault.personal {
				for _, f := range p.fields {
					sealed[f] = true
				}
			}
		}
		for _, f := range personal {
			if v, ok := obj[f]; ok && v != nil && !sealed[f] {
				return Reject("personal_in_clear", fmt.Sprintf(
					"%s declares %s personal, but %s does not: it would be stored in clear (declare it with Domain.Event(%q, …).Personal)",
					commandType, f, e.Type, e.Type))
			}
		}
	}
	return nil
}

var errStopped = errors.New("stopped")
