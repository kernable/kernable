package kernable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// WorkerOptions say how a worker runs: the argument of Worker.Run and App.Run.
type WorkerOptions struct {
	// Commands executed in parallel, of different aggregates (default 16).
	Concurrency int
	// How long to wait before reconnecting after the session ends (default 1 s); twice as long
	// after each further failed attempt, at most 30 s.
	Reconnect time.Duration
}

// Worker runs the commands of its aggregates over a connection. Create it with Client.Worker
// (or let Client.App do it) and start it with Run. The kernel leases it one command at a time
// per aggregate, with the aggregate's events (and latest snapshot); the worker folds them, calls
// the command's handler, and sends back the signed events, or the rejection. A command whose
// handler returns a *Rejected is rejected with its code; any other error rejects with
// handler_error. An event whose schema the worker does not know ends the session instead (it
// logs and reconnects until it is updated).
type Worker struct {
	c           *Client
	aggregates  []aggregateDef
	byAggregate map[string]aggregateDef
	service     string
	// Whether the first connect gets its own line; an App prints it in its overview instead.
	announce bool
	readyCh  chan struct{}
	once     sync.Once
}

func newWorker(c *Client, aggregates []aggregateDef, service string) *Worker {
	w := &Worker{c: c, aggregates: aggregates, byAggregate: map[string]aggregateDef{}, service: service, announce: true, readyCh: make(chan struct{})}
	for _, a := range aggregates {
		w.byAggregate[a.domain().Name+"."+a.typeName()] = a
	}
	return w
}

// Ready is closed once the kernel accepted the worker (on every partition).
func (w *Worker) Ready() <-chan struct{} { return w.readyCh }

func (w *Worker) markReady() { w.once.Do(func() { close(w.readyCh) }) }

// Run registers the aggregates and processes commands until ctx ends, reconnecting when the
// session drops. It returns nil once ctx ends.
func (w *Worker) Run(ctx context.Context, options ...WorkerOptions) error {
	o := WorkerOptions{Concurrency: 16, Reconnect: time.Second}
	if len(options) > 0 {
		if options[0].Concurrency > 0 {
			o.Concurrency = options[0].Concurrency
		}
		if options[0].Reconnect > 0 {
			o.Reconnect = options[0].Reconnect
		}
	}
	parts := w.c.partitions(ctx, "worker", o.Reconnect)
	if parts == nil {
		return nil
	}
	// A session per partition: commands of every partition's tenants run here.
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		ready.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx, p.clients, o, ready.Done)
		}()
	}
	go func() { ready.Wait(); w.markReady() }()
	wg.Wait()
	return nil
}

func (w *Worker) loop(ctx context.Context, c *clients, o WorkerOptions, ready func()) {
	var once sync.Once
	connects := 0
	attempt := 0
	for ctx.Err() == nil {
		before := connects
		err := w.session(ctx, c, o.Concurrency, &connects, func() { once.Do(ready) })
		if ctx.Err() != nil {
			return
		}
		if connects > before {
			attempt = 1
		} else {
			attempt++
		}
		if err == nil {
			err = errors.New("stream ended")
		}
		w.c.log.reconnect(ctx, "worker", err, o.Reconnect, attempt)
	}
}

// eventSchemas are the payload versions the worker reads and writes, for every event type its
// aggregates know: without a deployment, the kernel refuses the worker if the store already
// holds a newer one; with one, it rejects commands on aggregates holding an event newer than
// declared here (unknown_history).
func (w *Worker) eventSchemas() []*pb.EventSchema {
	schemas := map[string]uint32{}
	for _, a := range w.aggregates {
		for _, t := range a.eventTypes() {
			schemas[a.domain().Name+"."+t] = a.schemaOf(t)
		}
	}
	var out []*pb.EventSchema
	for _, t := range slices.Sorted(mapsKeys(schemas)) {
		out = append(out, &pb.EventSchema{EventType: t, Schema: schemas[t]})
	}
	return out
}

func (w *Worker) runs() string {
	var all []string
	for _, a := range w.aggregates {
		for _, t := range a.commandTypes() {
			all = append(all, a.domain().Name+"."+t)
		}
	}
	slices.Sort(all)
	return strings.Join(all, ", ")
}

func (w *Worker) session(ctx context.Context, c *clients, concurrency int, connects *int, ready func()) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, failed := c.stream()
	stream, err := pb.NewWorkerGatewayClient(conn).Session(c.outgoing(ctx))
	if err != nil {
		failed(err)
		return err
	}
	var sendMu sync.Mutex
	send := func(m *pb.WorkerMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(m)
	}
	var aggs []*pb.WorkerAggregate
	for _, a := range w.aggregates {
		var types []string
		for _, t := range a.commandTypes() {
			types = append(types, a.domain().Name+"."+t)
		}
		aggs = append(aggs, &pb.WorkerAggregate{AggregateType: a.domain().Name + "." + a.typeName(), CommandTypes: types})
	}
	if err := send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Register{Register: &pb.Register{
		WorkerId: fmt.Sprintf("go-%d", os.Getpid()), KeyId: w.c.key.ID, Protocol: Protocol, Aggregates: aggs,
		MaxConcurrency: uint32(concurrency), EventSchemas: w.eventSchemas(), Service: w.service, Deployment: w.c.deployment,
	}}}); err != nil {
		failed(err)
		return err
	}
	slots := make(chan struct{}, concurrency)
	var running sync.WaitGroup
	defer running.Wait()
	// Executes whose history is still coming in parts, by lease.
	partial := map[string]*pb.Execute{}
	// A failure outside a handler (a malformed message, an event this code cannot read) ends the
	// session, not the process.
	var endErr error
	var endOnce sync.Once
	end := func(err error) { endOnce.Do(func() { endErr = err; cancel() }) }
	for {
		m, err := stream.Recv()
		if err != nil {
			if endErr != nil {
				return endErr
			}
			failed(err)
			if errors.Is(err, io.EOF) {
				return errors.New("stream ended")
			}
			return err
		}
		switch x := m.Msg.(type) {
		case *pb.KernelMessage_Challenge:
			if err := send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_ChallengeResponse{ChallengeResponse: &pb.ChallengeResponse{
				Sig: w.c.key.sign(challengeMessage(x.Challenge.Nonce)).Sig,
			}}}); err != nil {
				return err
			}
		case *pb.KernelMessage_Registered:
			if r := x.Registered.ReaderOf; r != "" {
				w.c.log.print(levelInfo, "worker", fmt.Sprintf("connected to a reader: commands run on the writer (%s); this worker gets none here", r))
			}
			if *connects > 0 {
				w.c.log.print(levelInfo, "worker", "connected again")
			} else if w.announce {
				w.c.log.print(levelInfo, "worker", fmt.Sprintf("connected to %s · runs %s", w.c.log.peer, w.runs()))
			}
			*connects++
			ready()
		case *pb.KernelMessage_Execute, *pb.KernelMessage_ExecuteMore:
			// A long history comes in parts; run the command once the last one is here.
			var e *pb.Execute
			if ex, ok := x.(*pb.KernelMessage_Execute); ok {
				e = ex.Execute
			} else {
				more := x.(*pb.KernelMessage_ExecuteMore).ExecuteMore
				e = partial[more.LeaseId]
				delete(partial, more.LeaseId)
				if e == nil {
					continue
				}
				e.History = append(e.History, more.History...)
				e.Vault = append(e.Vault, more.Vault...)
				e.More = more.More
			}
			if e == nil {
				continue
			}
			if e.More {
				partial[e.LeaseId] = e
				continue
			}
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				continue
			}
			running.Add(1)
			go func() {
				defer running.Done()
				defer func() { <-slots }()
				if err := w.execute(ctx, e, send); err != nil {
					w.c.log.print(levelError, "worker", err.Error())
					end(err)
				}
			}()
		}
	}
}

// execute runs one command and answers it; an error ends the session.
func (w *Worker) execute(ctx context.Context, x *pb.Execute, send func(*pb.WorkerMessage) error) (err error) {
	cmd := x.Command
	if cmd == nil {
		return errors.New("cannot execute: an execute without a command")
	}
	stream := fmt.Sprintf("%s.%s %s/%s", cmd.Domain, cmd.CommandType, cmd.AggregateType, cmd.AggregateId)
	// A handler cannot be stopped from outside; one that runs past its lease at least shows.
	deadline := time.UnixMilli(x.Deadline)
	hung := time.AfterFunc(time.Until(deadline), func() {
		w.c.log.print(levelError, "worker", stream+" still running after its lease ended: the kernel gives it to another worker; is the handler stuck?")
	})
	defer hung.Stop()
	reject := func(reason *pb.Rejected) error {
		sig := w.c.key.sign(rejectionMessage(cmd.CommandId, reason.Code, reason.Message))
		w.c.log.print(levelDebug, "worker", fmt.Sprintf("%s rejected %s", stream, reason.Code))
		return send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Rejection{Rejection: &pb.Rejection{LeaseId: x.LeaseId, Reason: reason, WorkerSig: sig}}})
	}
	out, err := w.run(ctx, x, deadline)
	if err != nil {
		var se *schemaError
		if errors.As(err, &se) {
			// Not the command's fault: this worker cannot read the aggregate. End the session; Run
			// reconnects and meets the same event again until the worker is updated.
			return err
		}
		var r *Rejected
		if errors.As(err, &r) {
			return reject(&pb.Rejected{Code: r.Code, Message: r.Message})
		}
		// An unexpected error's text may hold personal data or internals, and a rejection is
		// kept for good: it names only the error's type; the log has the rest.
		w.c.log.print(levelError, "worker", fmt.Sprintf("%s failed: %v", stream, err))
		return reject(&pb.Rejected{Code: "handler_error", Message: errorType(err)})
	}
	return send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Result{Result: out}})
}

// run folds, handles and seals one command: the signed result, or why it is rejected.
func (w *Worker) run(ctx context.Context, x *pb.Execute, deadline time.Time) (result *pb.Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	cmd := x.Command
	a, ok := w.byAggregate[cmd.Domain+"."+cmd.AggregateType]
	if !ok {
		return nil, Reject("unknown_command", fmt.Sprintf("no handler for %s on %s.%s", cmd.CommandType, cmd.Domain, cmd.AggregateType))
	}
	request := contextOf(cmd.Context)
	var history []FoldEvent
	for _, e := range x.History {
		data, _, err := unseal(e.Payload, x.Vault)
		if err != nil {
			return nil, err
		}
		history = append(history, FoldEvent{
			EventMeta: EventMeta{Type: e.EventType, Version: e.Version, Tenant: cmd.TenantId, Domain: cmd.Domain,
				AggregateType: cmd.AggregateType, AggregateID: cmd.AggregateId, CommandID: e.CommandId},
			Schema: e.Schema, Data: data, Request: contextOf(e.Context),
		})
	}
	data, _, err := unseal(cmd.Payload, x.Vault)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	ex := &execution{
		ctx: &CommandContext{Context: cctx, CommandID: cmd.CommandId, Type: cmd.CommandType, Domain: cmd.Domain,
			AggregateType: cmd.AggregateType, AggregateID: cmd.AggregateId, Tenant: cmd.TenantId, Request: request,
			Deadline: deadline, at: time.UnixMilli(x.ReceivedAt)},
		history: history, data: data, expectedVersion: x.ExpectedVersion,
	}
	if x.Snapshot != nil {
		ex.snapshotSchema, ex.snapshotState = x.Snapshot.Schema, x.Snapshot.State
		if ex.snapshotState == nil {
			ex.snapshotState = []byte{}
		}
	}
	done, err := a.execute(ex)
	if err != nil {
		return nil, err
	}
	s := streamOf(cmd)
	result = &pb.Result{LeaseId: x.LeaseId}
	for i, e := range done.events {
		var def *EventSchema
		if d, ok := a.domain().events[e.Type]; ok {
			def = d
		}
		var vault *vaultFields
		if def != nil {
			vault = &def.vault
		}
		sealedEvent, err := seal(e.Data, vault, cmd.AggregateId)
		if err != nil {
			return nil, err
		}
		erases := ""
		if def != nil && def.eraser != nil {
			payload, _ := encode(e.Data)
			generic, _ := decodeAny(payload)
			obj, _ := generic.(map[string]any)
			erases = def.eraser(obj, cmd.AggregateId)
		}
		result.Events = append(result.Events, &pb.NewEvent{EventType: e.Type, Schema: done.schemas[i], Payload: sealedEvent.payload, VaultRefs: sealedEvent.refs, Erases: erases})
		result.Vault = append(result.Vault, sealedEvent.values...)
	}
	result.WorkerSig = w.c.key.sign(resultMessage(s, x.ExpectedVersion, result.Events))
	if done.snapshot != nil {
		version := x.ExpectedVersion + uint64(len(done.events))
		snap := &pb.Snapshot{Version: version, Schema: done.schema, State: done.snapshot}
		snap.WorkerSig = w.c.key.sign(snapshotMessage(cmd.TenantId, cmd.Domain, cmd.AggregateType, cmd.AggregateId, version, snap.Schema, snap.State))
		result.Snapshot = snap
	}
	// An aggregate that holds nothing and requires nothing sends no index, and no signature over it.
	if done.indexed {
		for _, k := range done.keys {
			result.Keys = append(result.Keys, &pb.UniqueKey{Name: k.Name, Global: k.Global, ValueHash: valueHash(k.Value), Subject: k.Subject})
		}
		result.Facts = done.facts
		for _, r := range done.requires {
			req := &pb.Requirement{Domain: r.Domain, AggregateType: r.AggregateType, AggregateId: r.AggregateID, Fact: r.Fact, Key: r.Key, Absent: r.Absent}
			if req.Domain == "" {
				req.Domain = cmd.Domain
			}
			if r.Key != "" {
				req.ValueHash = valueHash(r.Value)
			}
			result.Requires = append(result.Requires, req)
		}
		result.IndexSig = w.c.key.sign(indexMessage(s, x.ExpectedVersion, result.Keys, result.Facts, result.Requires))
	}
	w.c.log.print(levelDebug, "worker", fmt.Sprintf("%s.%s %s/%s committed %d events", cmd.Domain, cmd.CommandType, cmd.AggregateType, cmd.AggregateId, len(result.Events)))
	return result, nil
}

func mapsKeys[K comparable, V any](m map[K]V) func(func(K) bool) {
	return func(yield func(K) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}
