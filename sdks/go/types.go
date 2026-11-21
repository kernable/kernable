// Package kernable writes event-sourced business logic in Go; the kernel does the rest.
//
// A client (New) submits commands and asks queries; domains (NewDomain) hold aggregates
// (NewAggregate), read models and reactors (Domain.ReadModel, Domain.Reactor) and business
// processes (Domain.Process); Client.App runs whole domains as one service. Every call that
// can block takes a context.Context first.
package kernable

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// Protocol is the protocol version this SDK speaks; see "Protocol versions" in kernable.proto.
const Protocol = 1

// Rejected is a refusal with a business reason: return it from a command handler, a query
// handler or a middleware. Code is a short machine-readable reason ("rejected" unless given),
// Message the text for people.
//
//   - From a command handler: the command is rejected and stored as rejected, with no events;
//     Client.Submit returns an Outcome with Committed false, not an error.
//   - From a query handler or a middleware: the request is refused; Client.Query returns a
//     *Rejected with the same code and message.
//
// Any other error a handler returns rejects too, with the code handler_error (a middleware's:
// middleware_error).
type Rejected struct {
	Code    string
	Message string
	// For a rejected follow-up command (ListenerContext.Submit): the key the kernel named
	// (unique_taken).
	Key string
	// For a rejected follow-up command: the requirement the kernel named (requirement_missing).
	Requirement *Requirement
}

func (r *Rejected) Error() string { return r.Message }

// Reject returns a *Rejected with code and message: `return nil, kernable.Reject("exists", "Order already exists")`.
func Reject(code, message string) *Rejected {
	if code == "" {
		code = "rejected"
	}
	return &Rejected{Code: code, Message: message}
}

// NotSettled is returned by a settling Submit (the default) when the command was committed but
// not every listener that counts processed its events within the kernel's --settle-timeout-ms
// (default 5 s). The command is not lost: do not submit it again under a new ID. Submitting
// again with the same ID returns the same outcome; a query with MinPosition waits for the events.
type NotSettled struct {
	// The committed outcome, with the stored events.
	Outcome Outcome
}

func (*NotSettled) Error() string {
	return "command committed, but not all listeners have processed its events yet"
}

// NotDue is returned by Submit for the ID of a scheduled command (Enqueue with At) that is not
// due yet: it has no outcome to wait for. Submit its ID again after its time.
type NotDue struct{ CommandID string }

func (e *NotDue) Error() string {
	return fmt.Sprintf("command %s is scheduled and not due yet: ask again after its time", e.CommandID)
}

// NewEvent is an event a command handler returns. The worker stores it at the payload version
// declared with Domain.Event (1 if not declared) and moves fields declared personal to the vault.
type NewEvent struct {
	// The event type within the aggregate's domain, unqualified: ItemAdded.
	Type string
	// The payload; it must encode as JSON. Nil is stored as {}.
	Data any
}

// Event is a NewEvent: kernable.Event("ItemAdded", ItemAdded{SKU: "apple", Qty: 2}).
func Event(eventType string, data any) NewEvent { return NewEvent{Type: eventType, Data: data} }

// Events is a list of events, for a handler's return: `return kernable.Events(kernable.Event(…)), nil`.
func Events(events ...NewEvent) []NewEvent { return events }

// RequestContext says who sends a request and in which context: signed with the command or
// query and stored with every event a command causes. Middleware decides on it;
// ListenerContext.Follow passes it on to follow-up commands. Every field is optional;
// Attributes are business data only, never security decisions.
type RequestContext struct {
	// The sender's tenant, as your application defines it.
	SenderTenant string `json:"senderTenant,omitempty"`
	// The person or system the request is for: who middleware checks.
	SenderIdentity string `json:"senderIdentity,omitempty"`
	// The sender's account, as your application defines it.
	SenderAccount string `json:"senderAccount,omitempty"`
	// The sender's session, as your application defines it.
	SenderSession string `json:"senderSession,omitempty"`
	// Authorize as this identity of the target tenant; the sender stays the real principal.
	OnBehalfOf string `json:"onBehalfOf,omitempty"`
	// Reject the command (version_mismatch) unless the aggregate is at this version.
	ExpectedVersion *uint64 `json:"expectedVersion,omitempty"`
	// Shared by everything one original request causes; defaults to the command ID.
	CorrelationID string `json:"correlationId,omitempty"`
	// The command that caused this one (Follow sets it).
	CausationID string `json:"causationId,omitempty"`
	// Your tracing system's ID.
	TraceID string `json:"traceId,omitempty"`
	// Privileged: only keys granted `privileged` for bypass_authorization may set it.
	BypassAuthorization bool `json:"bypassAuthorization,omitempty"`
	// Free key-value pairs for business data, never for security decisions.
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (c *RequestContext) wire() *pb.RequestContext {
	if c == nil {
		c = &RequestContext{}
	}
	w := &pb.RequestContext{
		SenderTenant: c.SenderTenant, SenderIdentity: c.SenderIdentity, SenderAccount: c.SenderAccount,
		SenderSession: c.SenderSession, OnBehalfOf: c.OnBehalfOf, ExpectedVersion: c.ExpectedVersion,
		CorrelationId: c.CorrelationID, CausationId: c.CausationID, TraceId: c.TraceID,
		BypassAuthorization: c.BypassAuthorization,
	}
	// In the order of their keys: the same attributes sign the same way every time.
	for _, k := range slices.Sorted(maps.Keys(c.Attributes)) {
		w.Attributes = append(w.Attributes, &pb.Attribute{Key: k, Value: c.Attributes[k]})
	}
	return w
}

func contextOf(c *pb.RequestContext) RequestContext {
	if c == nil {
		return RequestContext{}
	}
	r := RequestContext{
		SenderTenant: c.SenderTenant, SenderIdentity: c.SenderIdentity, SenderAccount: c.SenderAccount,
		SenderSession: c.SenderSession, OnBehalfOf: c.OnBehalfOf, ExpectedVersion: c.ExpectedVersion,
		CorrelationID: c.CorrelationId, CausationID: c.CausationId, TraceID: c.TraceId,
		BypassAuthorization: c.BypassAuthorization,
	}
	if len(c.Attributes) > 0 {
		r.Attributes = map[string]string{}
		for _, a := range c.Attributes {
			r.Attributes[a.Key] = a.Value
		}
	}
	return r
}

// EventMeta is what Apply gets about an event besides its data: only fields known before the
// kernel stores the event, so a snapshot and a replay compute the same state.
type EventMeta struct {
	// The event type within its domain: ItemAdded.
	Type string
	// The aggregate's version after this event: 1 for its first event.
	Version uint64
	Tenant  string
	// Always the domain of the command that caused the event.
	Domain string
	// The aggregate type, unqualified: cart.
	AggregateType string
	AggregateID   string
	// The command that produced the event.
	CommandID string
}

// StoredEvent is an event as the kernel stored it: in a submit's Outcome, in a subscription
// (Client.Subscribe) and, as ListenerContext.Event, in a listener's handler.
type StoredEvent struct {
	EventMeta
	// The event's global position: its place in the one order of all domains and tenants.
	Position uint64
	// When the kernel committed the event.
	CreatedAt time.Time
	// The payload format's version. A listener's handler gets the data upcast to the version it
	// knows (see Domain.Event); here it is as stored.
	Schema uint32
	// The payload as stored (in a listener: upcast, with the vault fields it may read).
	Data json.RawMessage
	// Fields kept in the vault whose value is not here (null in Data): erased, or not readable
	// with this key.
	Redacted []string
	// The request context of the command that caused the event.
	Request RequestContext
	// The code version of the worker that wrote it; empty if none.
	Deployment string
}

// Decode unmarshals the event's data into v.
func (e *StoredEvent) Decode(v any) error { return decodeInto(e.Data, v) }

// Outcome is what came of a command (Client.Submit): committed with the events it stored, or
// rejected with a code and message (by its handler, a middleware or the kernel, e.g.
// version_mismatch, unique_taken, requirement_missing). A rejection is a result, not an error:
// check Committed.
type Outcome struct {
	Committed bool
	// Committed: the stored events.
	Events []StoredEvent
	// Rejected: why.
	Code    string
	Message string
	// unique_taken: the key's name (never its value).
	Key string
	// requirement_missing: the first requirement that does not hold.
	Requirement *Requirement
}

func rejectedOutcome(r *pb.Rejected) Outcome {
	if r == nil {
		r = &pb.Rejected{}
	}
	o := Outcome{Code: r.Code, Message: r.Message, Key: r.Key}
	if q := r.Requirement; q != nil {
		// Named by a key: its name only, never the value.
		req := &Requirement{Domain: q.Domain, AggregateType: q.AggregateType, Fact: q.Fact, Absent: q.Absent}
		if q.Key != "" {
			req.Key = q.Key
		} else {
			req.AggregateID = q.AggregateId
		}
		o.Requirement = req
	}
	return o
}

// Requirement is a fact another aggregate of the command's tenant must hold (or, Absent, must
// not hold) when the command commits: what a command's Requires option returns. The kernel
// checks it atomically with the commit; if it does not hold, the command is rejected with
// requirement_missing. The aggregate is named by its ID, or by a unique key of its type (Key and
// Value), which the kernel resolves at commit.
type Requirement struct {
	// The domain of the aggregate; default: the command's own.
	Domain string
	// The aggregate type within the domain, unqualified: Vehicle.
	AggregateType string
	// The aggregate's ID; empty when it is named by Key.
	AggregateID string
	// A unique key of the aggregate type that names the aggregate instead of its ID.
	Key string
	// The key's value, normalised as the key's function does: the kernel gets only its SHA-256.
	Value string
	// The fact it must hold, as declared with Aggregate.Fact: open.
	Fact string
	// The aggregate must not hold the fact.
	Absent bool
}

var issued struct {
	sync.Mutex
	last int64
}

// issuedNow is a request's issued_at: now, but later than every one this process issued
// before. Two calls in the same millisecond would otherwise carry the same signature, and the
// kernel refuses the second as a replay.
func issuedNow() int64 {
	issued.Lock()
	defer issued.Unlock()
	issued.last = max(time.Now().UnixMilli(), issued.last+1)
	return issued.last
}

// encode is a payload: data as JSON, nil as {}. Data that is JSON already (json.RawMessage,
// []byte of JSON) is sent as it is.
func encode(data any) ([]byte, error) {
	switch d := data.(type) {
	case nil:
		return []byte("{}"), nil
	case json.RawMessage:
		if len(d) == 0 {
			return []byte("{}"), nil
		}
		return d, nil
	}
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(data); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

// decodeAny reads a payload as generic JSON, numbers kept exact; empty reads as {}.
func decodeAny(payload []byte) (any, error) {
	if len(payload) == 0 {
		return map[string]any{}, nil
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// decodeInto unmarshals a payload into v; empty reads as {}.
func decodeInto(payload []byte, v any) error {
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if raw, ok := v.(*json.RawMessage); ok {
		*raw = append((*raw)[:0], payload...)
		return nil
	}
	return unmarshal(payload, v)
}

// unmarshal is json.Unmarshal with numbers in generic values (any, map[string]any) kept exact as
// json.Number instead of float64, which would round integers beyond 2^53.
func unmarshal(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("kernable: unexpected data after the JSON value")
	}
	return nil
}

// vaultFields are payload fields kept in the vault instead of the signed log.
type vaultFields struct {
	personal []personalFields
	secret   []string
}

type personalFields struct {
	fields  []string
	subject Subject
}

// Subject says whose personal data a payload holds: from the payload (as generic JSON) and the
// aggregate's ID. `func(_ map[string]any, id string) string { return id }` for the aggregate itself.
type Subject func(data map[string]any, aggregateID string) string

func (v *vaultFields) empty() bool { return v == nil || (len(v.personal) == 0 && len(v.secret) == 0) }

// sealed is a payload with its vault fields moved out.
type sealed struct {
	payload []byte
	refs    []string
	values  []*pb.VaultValue
}

// seal moves the vault fields of data into vault values: the payload keeps {"$vault": ref}.
// Absent and null fields stay as they are.
func seal(data any, vault *vaultFields, aggregateID string) (sealed, error) {
	payload, err := encode(data)
	if err != nil {
		return sealed{}, err
	}
	if vault.empty() {
		return sealed{payload: payload}, nil
	}
	generic, err := decodeAny(payload)
	if err != nil {
		return sealed{}, err
	}
	obj, ok := generic.(map[string]any)
	if !ok {
		return sealed{payload: payload}, nil
	}
	out := sealed{}
	keep := func(field, subject string, secret bool) error {
		v, ok := obj[field]
		if !ok || v == nil {
			return nil
		}
		value, err := encode(v)
		if err != nil {
			return err
		}
		salt := make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			return err
		}
		ref := vaultRef(salt, subject, secret, value)
		out.values = append(out.values, &pb.VaultValue{Ref: ref, Subject: subject, Secret: secret, Salt: salt, Value: value})
		out.refs = append(out.refs, ref)
		obj[field] = map[string]any{"$vault": ref}
		return nil
	}
	original, _ := decodeAny(payload)
	for _, p := range vault.personal {
		who := p.subject(original.(map[string]any), aggregateID)
		if who == "" {
			return sealed{}, fmt.Errorf("no subject for the personal fields %v", p.fields)
		}
		for _, f := range p.fields {
			if err := keep(f, who, false); err != nil {
				return sealed{}, err
			}
		}
	}
	for _, f := range vault.secret {
		if err := keep(f, "", true); err != nil {
			return sealed{}, err
		}
	}
	if out.payload, err = encode(obj); err != nil {
		return sealed{}, err
	}
	return out, nil
}

// unseal puts vault values back into a payload; references without a value become null. It
// returns the payload as generic JSON and the paths of the fields it could not fill.
func unseal(payload []byte, vault []*pb.VaultValue) (any, []string, error) {
	data, err := decodeAny(payload)
	if err != nil {
		return nil, nil, err
	}
	values := map[string][]byte{}
	for _, v := range vault {
		values[v.Ref] = v.Value
	}
	var redacted []string
	var walk func(v any, path string) (any, error)
	walk = func(v any, path string) (any, error) {
		switch x := v.(type) {
		case []any:
			for i := range x {
				var err error
				if x[i], err = walk(x[i], fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return nil, err
				}
			}
			return x, nil
		case map[string]any:
			if ref, ok := x["$vault"].(string); ok && len(x) == 1 {
				value, found := values[ref]
				if !found {
					redacted = append(redacted, path)
					return nil, nil
				}
				return decodeAny(value)
			}
			for k, item := range x {
				p := k
				if path != "" {
					p = path + "." + k
				}
				var err error
				if x[k], err = walk(item, p); err != nil {
					return nil, err
				}
			}
			return x, nil
		}
		return v, nil
	}
	data, err = walk(data, "")
	slices.Sort(redacted)
	return data, redacted, err
}

func storedOf(e *pb.Event, vault []*pb.VaultValue) (StoredEvent, error) {
	data, redacted, err := unseal(e.Payload, vault)
	if err != nil {
		return StoredEvent{}, err
	}
	raw, err := encode(data)
	if err != nil {
		return StoredEvent{}, err
	}
	return StoredEvent{
		EventMeta: EventMeta{
			Type: e.EventType, Version: e.Version, Tenant: e.TenantId, Domain: e.Domain,
			AggregateType: e.AggregateType, AggregateID: e.AggregateId, CommandID: e.CommandId,
		},
		Position: e.GlobalPosition, CreatedAt: time.UnixMilli(e.CreatedAt), Schema: e.Schema, Data: raw,
		Redacted: redacted, Request: contextOf(e.Context), Deployment: e.Deployment,
	}, nil
}

func storedAll(events []*pb.Event) ([]StoredEvent, error) {
	out := make([]StoredEvent, 0, len(events))
	for _, e := range events {
		s, err := storedOf(e, nil)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// errorType is the type of an error for a rejection the kernel keeps: never its text, which can
// hold personal data.
func errorType(err error) string {
	if err == nil {
		return "nil"
	}
	var r *Rejected
	if errors.As(err, &r) {
		return "Rejected"
	}
	t := fmt.Sprintf("%T", err)
	if i := bytes.LastIndexByte([]byte(t), '.'); i >= 0 {
		t = t[i+1:]
	}
	return t
}
