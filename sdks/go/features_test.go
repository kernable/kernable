package kernable

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type counter struct {
	N int `json:"n"`
}

func counterDomain() (*Domain, *Aggregate[counter]) {
	d := NewDomain("ops")
	a := NewAggregate(d, "counter", counter{}, SnapshotEvery(3))
	Apply(a, "Added", func(_ *ApplyContext, s *counter, d struct{ By int }) { s.N += d.By })
	Handle(a, "Add", func(ctx *CommandContext, s counter, d struct{ By int }) ([]NewEvent, error) {
		return Events(Event("Added", map[string]int{"by": d.By})), nil
	})
	Handle(a, "Get", func(ctx *CommandContext, s counter, _ struct{}) ([]NewEvent, error) {
		return Events(Event("Seen", map[string]int{"n": s.N})), nil
	})
	return d, a
}

func setup(t *testing.T, o kernelOptions, types ...string) (*testKernel, *Client) {
	k := newTestKernel(t, types, o)
	k.start()
	return k, k.client()
}

func TestMiddleware(t *testing.T) {
	_, client := setup(t, kernelOptions{}, "ops.*")
	d, a := counterDomain()
	guard := d.ReadModel("guard")
	var after atomic.Int32
	guard.Middleware(MiddlewareOptions{Types: []string{"ops.*"}}, func(ctx *MiddlewareContext, c Check) error {
		if c.Request.SenderIdentity != "ann" {
			return Reject("forbidden", "sign in first")
		}
		return nil
	})
	guard.Middleware(MiddlewareOptions{Types: []string{"ops.*"}, When: After}, func(ctx *MiddlewareContext, c Check) error {
		if c.Kind == "command" && ctx.Outcome != nil && ctx.Outcome.Committed {
			after.Add(1)
		}
		return nil
	})
	runWorker(t, client, a)
	h := client.Listener(guard)
	running(t, h.Run)
	waitFor(t, h.Ready(), "guard")
	ctx := context.Background()
	o := must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "x", Type: "Add", Data: map[string]int{"by": 1}}))
	if o.Committed || o.Code != "forbidden" {
		t.Fatalf("refused: %+v", o)
	}
	ann := RequestContext{SenderIdentity: "ann"}
	o = must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "x", Type: "Add", Data: map[string]int{"by": 1}, Context: &ann}))
	if !o.Committed {
		t.Fatalf("let through: %+v", o)
	}
	for i := 0; i < 100 && after.Load() == 0; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if after.Load() != 1 {
		t.Fatalf("the middleware after saw %d commits", after.Load())
	}
	// Files and cache pass the chain too.
	if _, err := client.Data.Put(ctx, "ops", "f", []byte("x")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a refused file: %v", err)
	}
}

func TestSnapshotsAndData(t *testing.T) {
	_, client := setup(t, kernelOptions{}, "ops.*")
	_, a := counterDomain()
	w := client.Worker(a)
	running(t, func(ctx context.Context) error { return w.Run(ctx) })
	waitFor(t, w.Ready(), "worker")
	ctx := context.Background()
	for i := 1; i <= 7; i++ {
		must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "s", Type: "Add", Data: map[string]int{"by": i}}))
	}
	o := must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "s", Type: "Get"}))
	if string(o.Events[0].Data) != `{"n":28}` {
		t.Fatalf("state from a snapshot and the events after it: %s", o.Events[0].Data)
	}

	sum := must[string](t)(client.Data.Put(ctx, "ops", "docs/a.txt", []byte("hello"), DataOptions{ContentType: "text/plain"}))
	f := must[*File](t)(client.Data.Get(ctx, "ops", "docs/a.txt"))
	if f == nil || string(f.Data) != "hello" || f.ContentType != "text/plain" || f.SHA256 != sum {
		t.Fatalf("file %+v", f)
	}
	files, next, err := client.Data.List(ctx, "ops", "docs/")
	if err != nil || len(files) != 1 || files[0].Size != 5 || next != "" {
		t.Fatalf("list %+v %q %v", files, next, err)
	}
	if !must[bool](t)(client.Data.Delete(ctx, "ops", "docs/a.txt")) {
		t.Fatal("delete")
	}
	if f := must[*File](t)(client.Data.Get(ctx, "ops", "docs/a.txt")); f != nil {
		t.Fatal("deleted")
	}
	if err := client.Cache.Set(ctx, "ops", "rate", []byte("1.08"), DataOptions{TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if v := must[[]byte](t)(client.Cache.Get(ctx, "ops", "rate")); string(v) != "1.08" {
		t.Fatalf("cache %q", v)
	}
}

func TestStoreAndSubscription(t *testing.T) {
	k, client := setup(t, kernelOptions{}, "ops.*")
	d, a := counterDomain()
	totals := d.ReadModel("totals", WithStore("sqlite:"+filepath.Join(k.dir, "totals.db")))
	On(totals, "Added", func(ctx *ListenerContext, d struct{ By int }) error {
		var n int
		if _, err := ctx.Store.Get("t", ctx.Event.AggregateID, &n); err != nil {
			return err
		}
		return ctx.Store.Set("t", ctx.Event.AggregateID, n+d.By)
	})
	Answer(totals, "Total", func(ctx *QueryContext, d struct{ ID string }) (int, error) {
		var n int
		_, err := ctx.Store.Get("t", d.ID, &n)
		return n, err
	})
	runWorker(t, client, a)
	ctx := context.Background()
	runModel := func() context.CancelFunc {
		mctx, cancel := context.WithCancel(ctx)
		h := client.Listener(totals)
		done := make(chan struct{})
		go func() { h.Run(mctx); close(done) }()
		waitFor(t, h.Ready(), "totals")
		return func() { cancel(); <-done }
	}
	stop := runModel()
	for i := 0; i < 3; i++ {
		must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "t1", Type: "Add", Data: map[string]int{"by": 2}}))
	}
	stop()
	// Restarted, it continues after the stored position: no event counts twice.
	stop = runModel()
	defer stop()
	must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "t1", Type: "Add", Data: map[string]int{"by": 1}}))
	if n := must[int](t)(QueryAs[int](ctx, client, Query{Domain: "ops", Type: "Total", Data: map[string]string{"id": "t1"}})); n != 7 {
		t.Fatalf("total %d", n)
	}

	sctx, cancel := context.WithCancel(ctx)
	var seen []uint64
	err := client.Subscribe(sctx, Subscription{Types: []string{"ops.Added"}}, func(_ context.Context, e StoredEvent) error {
		seen = append(seen, e.Position)
		if len(seen) == 4 {
			cancel()
		}
		return nil
	})
	if err != nil || len(seen) != 4 {
		t.Fatalf("subscription %v %v", seen, err)
	}
}

func TestScheduledAndProcess(t *testing.T) {
	_, client := setup(t, kernelOptions{processes: true}, "ops.*")
	d, a := counterDomain()
	Handle(a, "Close", func(ctx *CommandContext, s counter, _ struct{}) ([]NewEvent, error) {
		return Events(Event("Closed", map[string]int{"n": s.N})), nil
	})
	d.Process("closing").
		Start("Added", StartOptions{Key: "aggregateId", Data: Mapping{"id": "aggregateId"}}).
		Timer("later", TimerStep{After: "1s"}).
		Command("close", "counter.Close", CommandStep{Aggregate: "case.id"}).
		End("done")
	app, err := client.App(AppOptions{Service: "ops", Version: "1.0.0", Domains: []*Domain{d}})
	if err != nil {
		t.Fatal(err)
	}
	var runErr error
	running(t, func(ctx context.Context) error { runErr = app.Run(ctx); return runErr })
	waitFor(t, app.Ready(), "app")
	if r := app.Registered(); len(r) != 1 || r[0].Name != "ops.closing" || r[0].Version != 1 {
		t.Fatalf("registered %+v (%v)", r, runErr)
	}
	ctx := context.Background()
	id := must[string](t)(client.Enqueue(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "p", Type: "Add", Data: map[string]int{"by": 5}, At: time.Now().Add(300 * time.Millisecond)}))
	// Submitted again by its ID, it waits for the scheduled command's outcome.
	if o := must[Outcome](t)(client.Submit(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "p", Type: "Add", ID: id})); !o.Committed || o.Events[0].Type != "Added" {
		t.Fatalf("the scheduled command's outcome: %+v", o)
	}
	later := must[string](t)(client.Enqueue(ctx, Command{Domain: "ops", AggregateType: "counter", AggregateID: "q", Type: "Add", Data: map[string]int{"by": 1}, At: time.Now().Add(time.Hour)}))
	if c := must[CancelOutcome](t)(client.Cancel(ctx, later)); !c.Cancelled {
		t.Fatalf("cancel a scheduled command: %+v", c)
	}
	// The process closes the counter about a second after the scheduled Add ran.
	var closed bool
	for i := 0; i < 150 && !closed; i++ {
		time.Sleep(50 * time.Millisecond)
		sctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		client.Subscribe(sctx, Subscription{Types: []string{"ops.Closed"}}, func(_ context.Context, e StoredEvent) error {
			closed = e.AggregateID == "p"
			cancel()
			return nil
		})
		cancel()
	}
	if !closed {
		t.Fatal("the process never closed the counter")
	}
	cancelled := must[CancelOutcome](t)(client.Cancel(ctx, "no-such-command"))
	if cancelled.Cancelled || cancelled.Code != "not_found" {
		t.Fatalf("cancel %+v", cancelled)
	}
}

// runWorker runs a worker for aggregates until the test ends, once it is connected.
func runWorker(t *testing.T, client *Client, aggregates ...AnyAggregate) {
	t.Helper()
	w := client.Worker(aggregates...)
	running(t, func(ctx context.Context) error { return w.Run(ctx) })
	waitFor(t, w.Ready(), "worker")
}
