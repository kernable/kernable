package kernable

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	processName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	duration    = regexp.MustCompile(`^([0-9]+)(s|m|h|d)$`)
)

// Literal is a literal in a mapping, where a plain string would be a reference; build it with
// Value.
type Literal struct {
	Value any `json:"value"`
}

// Value is a literal for a mapping: Mapping{"reason": kernable.Value("unpaid")} sends the
// string unpaid, while "case.reason" sends the case's field.
func Value(x any) Literal { return Literal{Value: x} }

// Mapping maps fields to references (strings: aggregateId, data.<path>, case.<path>, answer,
// answer.<path>) or literals (Value(…)). Paths are wire names, as the messages' fields.
type Mapping map[string]any

// StartOptions are the options of ProcessBuilder.Start.
type StartOptions struct {
	// Where the case's key is in the event: aggregateId or data.<field>. One case per key.
	Key string
	// The case's data, from the event.
	Data Mapping
}

// CancelOnOptions are the options of ProcessBuilder.CancelOn.
type CancelOnOptions struct {
	// Where the case's key is in the event.
	Key string
	// The end step the case ends at.
	End string
}

// CommandStep are the options of ProcessBuilder.Command.
type CommandStep struct {
	// The aggregate the command goes to, a reference: case.orderId.
	Aggregate string
	// The command's data, from the case.
	Data Mapping
	// The step to go to per rejection code; any other rejection is an incident.
	Rejected map[string]string
	// The step after the command committed. Default: the next step written.
	Next string
}

// WaitStep are the options of ProcessBuilder.Wait.
type WaitStep struct {
	// Where the case's key is in the awaited event.
	Key string
	// Merged into the case's data, from the event.
	Data Mapping
	// How long to wait (3d); with OnTimeout, the step to go to then.
	Timeout string
	// The step after Timeout; required with it.
	OnTimeout string
	// The step after the event came. Default: the next step written.
	Next string
}

// TimerStep says when a timer step fires: After a duration (1d), or At a reference to a time in
// ms since epoch (case.dueAt); and where it goes then.
type TimerStep struct {
	After string
	At    string
	// The step after the timer fired. Default: the next step written.
	Next string
}

// DecideCase is one case of a decision: if the condition When holds, go to Then.
type DecideCase struct {
	When string
	Then string
}

// DecideStep are the options of ProcessBuilder.Decide: one condition (When, Then) or several in
// order (Cases, the first that holds wins); Otherwise if none holds, by default the step written
// next.
type DecideStep struct {
	When      string
	Then      string
	Cases     []DecideCase
	Otherwise string
}

// PathPair is an answer and the step it leads to, for answers Paths cannot say (a string that
// looks like a boolean or a number).
type PathPair struct {
	Value any
	Next  string
}

// QueryStep are the options of ProcessBuilder.Query.
type QueryStep struct {
	// The query's data, from the case.
	Data Mapping
	// For an answer that is an object: the path of the field to match against. Default: the answer.
	Answer string
	// The step per answer: {"true": "approved", "false": "paid"}. Keys true and false stand for the
	// booleans, numeric keys for numbers, others for strings.
	Paths map[string]string
	// Pairs instead of Paths, in order.
	Pairs []PathPair
	// A case field to keep the answer in.
	Into string
	// The step for an answer no path matches. Without it, such an answer is an incident.
	Otherwise string
}

// ProcessModel is a process model, as the kernel registers it.
type ProcessModel struct {
	Name     string          `json:"name"`
	Start    ModelStart      `json:"start"`
	Steps    []ModelStep     `json:"steps"`
	CancelOn []ModelCancelOn `json:"cancelOn,omitempty"`
}

// ModelStart is a model's start.
type ModelStart struct {
	On   string  `json:"on"`
	Key  string  `json:"key"`
	Data Mapping `json:"data,omitempty"`
}

// ModelCancelOn is an event that ends a running case at once.
type ModelCancelOn struct {
	Event string `json:"event"`
	Key   string `json:"key"`
	End   string `json:"end"`
}

// ModelWhen is one condition of a decide step.
type ModelWhen struct {
	If   string `json:"if"`
	Next string `json:"next"`
}

// ModelPath is one path of a query step.
type ModelPath struct {
	Value any    `json:"value"`
	Next  string `json:"next"`
}

// ModelStep is one step of a model; every step but end names what comes next.
type ModelStep struct {
	Name      string            `json:"name"`
	Kind      string            `json:"kind"`
	Command   string            `json:"command,omitempty"`
	Event     string            `json:"event,omitempty"`
	Query     string            `json:"query,omitempty"`
	Aggregate string            `json:"aggregate,omitempty"`
	Key       string            `json:"key,omitempty"`
	Data      Mapping           `json:"data,omitempty"`
	Rejected  map[string]string `json:"rejected,omitempty"`
	TimeoutMs uint64            `json:"timeoutMs,omitempty"`
	OnTimeout string            `json:"onTimeout,omitempty"`
	AfterMs   uint64            `json:"afterMs,omitempty"`
	At        string            `json:"at,omitempty"`
	When      []ModelWhen       `json:"when,omitempty"`
	Answer    string            `json:"answer,omitempty"`
	Paths     []ModelPath       `json:"paths,omitempty"`
	Into      string            `json:"into,omitempty"`
	Otherwise string            `json:"otherwise,omitempty"`
	Next      string            `json:"next,omitempty"`
}

type written struct {
	name, kind, message string
	command             CommandStep
	wait                WaitStep
	timer               TimerStep
	decide              DecideStep
	query               QueryStep
}

// ProcessBuilder is a business process of a domain, built in code and run by the kernel: a case
// starts when an event commits, and moves through steps (commands, waits for events, timers,
// decisions by the case's data or by a query) to an end. Create it with Domain.Process.
//
// Steps run in the order written, unless a step names Next; branching steps (Decide, Query) and
// error paths (Rejected, OnTimeout) name their targets. Messages are names: an unqualified event
// or query is of the process's domain (OrderApproved), others qualified (payments.PaymentReceived);
// a command is aggregate.Type of the process's domain (order.Ship) or domain.aggregate.Type.
type ProcessBuilder struct {
	// The domain it belongs to: shop.
	Domain string
	// The process's name within its domain: fulfilment.
	Name  string
	start *struct {
		event   string
		options StartOptions
	}
	written []written
	cancels []struct {
		event   string
		options CancelOnOptions
	}
	err error
}

func newProcessBuilder(domain, name string) *ProcessBuilder {
	if !processName.MatchString(name) {
		panic(fmt.Sprintf("kernable: invalid process name %q: letters, digits, _ and - only", name))
	}
	return &ProcessBuilder{Domain: domain, Name: name}
}

// Qualified is the name the kernel knows: shop.fulfilment.
func (p *ProcessBuilder) Qualified() string { return p.Domain + "." + p.Name }

// Start starts a case when event commits.
func (p *ProcessBuilder) Start(event string, options StartOptions) *ProcessBuilder {
	if p.start != nil && p.err == nil {
		p.err = fmt.Errorf("process %s: start is given already", p.Qualified())
	}
	p.start = &struct {
		event   string
		options StartOptions
	}{event, options}
	return p
}

// Command sends a command as the process, to the aggregate options.Aggregate names.
func (p *ProcessBuilder) Command(name, command string, options CommandStep) *ProcessBuilder {
	p.written = append(p.written, written{name: name, kind: "command", message: command, command: options})
	return p
}

// Wait waits for an event of this case and merges options.Data from it into the case's data.
func (p *ProcessBuilder) Wait(name, event string, options WaitStep) *ProcessBuilder {
	p.written = append(p.written, written{name: name, kind: "wait", message: event, wait: options})
	return p
}

// Timer waits for a time: After a duration from when the case reached the step, or until At.
func (p *ProcessBuilder) Timer(name string, options TimerStep) *ProcessBuilder {
	p.written = append(p.written, written{name: name, kind: "timer", timer: options})
	return p
}

// Decide picks a path by the case's data. Conditions are references to the case, literals,
// == != < <= > >=, and, or, not and parentheses; anything more is a query.
func (p *ProcessBuilder) Decide(name string, options DecideStep) *ProcessBuilder {
	p.written = append(p.written, written{name: name, kind: "decide", decide: options})
	return p
}

// Query asks a read model a query with options.Data built from the case; the answer (or its
// field options.Answer) picks the step.
func (p *ProcessBuilder) Query(name, query string, options QueryStep) *ProcessBuilder {
	p.written = append(p.written, written{name: name, kind: "query", message: query, query: options})
	return p
}

// CancelOn ends a running case at once when an event of its key commits, whatever step it is at.
func (p *ProcessBuilder) CancelOn(event string, options CancelOnOptions) *ProcessBuilder {
	p.cancels = append(p.cancels, struct {
		event   string
		options CancelOnOptions
	}{event, options})
	return p
}

// End ends the case. A process may have several ends.
func (p *ProcessBuilder) End(name string) *ProcessBuilder {
	p.written = append(p.written, written{name: name, kind: "end"})
	return p
}

// processError names the process and the step.
type processError struct{ message string }

func (e *processError) Error() string { return e.message }

// Model is the model the kernel registers: types qualified, durations in ms, the order written
// resolved into explicit nexts. It checks what it can before the kernel does.
func (p *ProcessBuilder) Model() (model ProcessModel, err error) {
	if p.err != nil {
		return model, p.err
	}
	process := p.Qualified()
	defer func() {
		if r := recover(); r != nil {
			pe, ok := r.(*processError)
			if !ok {
				panic(r)
			}
			err = pe
		}
	}()
	fail := func(step *string, reason string) {
		if step == nil {
			panic(&processError{fmt.Sprintf("process %s: %s", process, reason)})
		}
		panic(&processError{fmt.Sprintf("process %s, step %q: %s", process, *step, reason)})
	}
	if p.start == nil {
		fail(nil, "no start: begin with .Start(event, StartOptions{Key: …})")
	}
	if len(p.written) == 0 {
		fail(nil, "no steps")
	}
	names := map[string]bool{}
	for _, s := range p.written {
		if !processName.MatchString(s.name) {
			fail(&s.name, "a step name has letters, digits, _ and - only")
		}
		if names[s.name] {
			fail(&s.name, "a second step of this name")
		}
		names[s.name] = true
	}
	target := func(step, what, to string) string {
		if to == "" {
			fail(&step, what+" names no step")
		}
		if !names[to] {
			fail(&step, fmt.Sprintf("%s names an unknown step %q", what, to))
		}
		return to
	}
	ms := func(step, d string) uint64 {
		n, err := durationMs(d)
		if err != nil {
			fail(&step, err.Error())
		}
		return n
	}
	for i, s := range p.written {
		following := func(next string) string {
			if next != "" {
				return target(s.name, "next", next)
			}
			if i+1 >= len(p.written) {
				fail(&s.name, "the last step is not an end: add .End(…) or name its next")
			}
			return p.written[i+1].name
		}
		step := ModelStep{Name: s.name, Kind: s.kind}
		switch s.kind {
		case "command":
			o := s.command
			step.Command = p.commandType(s.name, s.message)
			step.Aggregate, step.Data = o.Aggregate, o.Data
			if o.Rejected != nil {
				step.Rejected = map[string]string{}
				for code, to := range o.Rejected {
					step.Rejected[code] = target(s.name, "rejected."+code, to)
				}
			}
			step.Next = following(o.Next)
		case "wait":
			o := s.wait
			if (o.Timeout == "") != (o.OnTimeout == "") {
				fail(&s.name, "timeout and onTimeout go together")
			}
			step.Event = p.messageName(&s.name, s.message)
			step.Key, step.Data = o.Key, o.Data
			if o.Timeout != "" {
				step.TimeoutMs = ms(s.name, o.Timeout)
				step.OnTimeout = target(s.name, "onTimeout", o.OnTimeout)
			}
			step.Next = following(o.Next)
		case "timer":
			o := s.timer
			if (o.After == "") == (o.At == "") {
				fail(&s.name, "a timer waits either after a duration or until a time (at)")
			}
			if o.After != "" {
				step.AfterMs = ms(s.name, o.After)
			} else {
				step.At = o.At
			}
			step.Next = following(o.Next)
		case "decide":
			o := s.decide
			cases := o.Cases
			if cases == nil && o.When != "" {
				cases = []DecideCase{{When: o.When, Then: o.Then}}
			}
			if len(cases) == 0 {
				fail(&s.name, "a decision needs a condition: When and Then, or Cases")
			}
			for n, c := range cases {
				what := fmt.Sprintf("cases[%d].then", n)
				if o.Cases == nil {
					what = "then"
				}
				step.When = append(step.When, ModelWhen{If: c.When, Next: target(s.name, what, c.Then)})
			}
			if o.Otherwise != "" {
				step.Otherwise = target(s.name, "otherwise", o.Otherwise)
			} else {
				step.Otherwise = following("")
			}
		case "query":
			o := s.query
			pairs := o.Pairs
			if pairs == nil {
				for _, k := range sortedKeys(o.Paths) {
					pairs = append(pairs, PathPair{answerValue(k), o.Paths[k]})
				}
			}
			if len(pairs) == 0 {
				fail(&s.name, "a query step needs paths")
			}
			step.Query = p.messageName(&s.name, s.message)
			step.Data, step.Answer, step.Into = o.Data, o.Answer, o.Into
			for _, pp := range pairs {
				v, _ := json.Marshal(pp.Value)
				step.Paths = append(step.Paths, ModelPath{Value: pp.Value, Next: target(s.name, fmt.Sprintf("paths[%s]", v), pp.Next)})
			}
			if o.Otherwise != "" {
				step.Otherwise = target(s.name, "otherwise", o.Otherwise)
			}
		}
		model.Steps = append(model.Steps, step)
	}
	model.Name = process
	model.Start = ModelStart{On: p.messageName(nil, p.start.event), Key: p.start.options.Key, Data: p.start.options.Data}
	for _, c := range p.cancels {
		var end *written
		for i := range p.written {
			if p.written[i].name == c.options.End {
				end = &p.written[i]
			}
		}
		if end == nil {
			fail(nil, fmt.Sprintf("cancelOn names an unknown step %q", c.options.End))
		}
		if end.kind != "end" {
			fail(nil, fmt.Sprintf("cancelOn ends at %q, which is no end step", c.options.End))
		}
		model.CancelOn = append(model.CancelOn, ModelCancelOn{Event: p.messageName(nil, c.event), Key: c.options.Key, End: c.options.End})
	}
	return model, nil
}

// messageName is an event or query type: <domain>.<Type>; an unqualified name is of the
// process's domain.
func (p *ProcessBuilder) messageName(step *string, message string) string {
	parts := strings.Split(message, ".")
	if len(parts) == 1 && processName.MatchString(message) {
		return p.Domain + "." + message
	}
	if len(parts) == 2 && domainName.MatchString(parts[0]) && processName.MatchString(parts[1]) {
		return message
	}
	where := "process " + p.Qualified()
	if step != nil {
		where += fmt.Sprintf(", step %q", *step)
	}
	panic(&processError{fmt.Sprintf("%s: %q is no type: Type or domain.Type", where, message)})
}

// commandType is a command type: <domain>.<aggregateType>.<Type>; aggregate.Type is of the
// process's domain.
func (p *ProcessBuilder) commandType(step, command string) string {
	parts := strings.Split(command, ".")
	if len(parts) == 2 && processName.MatchString(parts[0]) && processName.MatchString(parts[1]) {
		return p.Domain + "." + command
	}
	if len(parts) == 3 && domainName.MatchString(parts[0]) && processName.MatchString(parts[1]) && processName.MatchString(parts[2]) {
		return command
	}
	panic(&processError{fmt.Sprintf("process %s, step %q: %q is no command type: aggregate.Type or domain.aggregate.Type", p.Qualified(), step, command)})
}

// durationMs is 30s, 15m, 12h or 3d in ms.
func durationMs(d string) (uint64, error) {
	m := duration.FindStringSubmatch(d)
	if m == nil {
		return 0, fmt.Errorf("%q is no duration: a positive number with s, m, h or d (30s, 15m, 12h, 3d)", d)
	}
	n, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("%q is no duration: a positive number with s, m, h or d (30s, 15m, 12h, 3d)", d)
	}
	unit := map[string]uint64{"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[m[2]]
	return n * unit, nil
}

// answerValue is a key of Paths as the value it matches: true, false, a number, else the string.
func answerValue(key string) any {
	switch key {
	case "true":
		return true
	case "false":
		return false
	}
	if f, err := strconv.ParseFloat(key, 64); err == nil && strings.TrimSpace(key) != "" && strconv.FormatFloat(f, 'f', -1, 64) == key {
		return json.Number(key)
	}
	return key
}
