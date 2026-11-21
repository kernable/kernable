package kernable

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type cart struct {
	Items      int  `json:"items"`
	CheckedOut bool `json:"checkedOut"`
}

type addItem struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type register struct {
	Email    string `json:"email"`
	Password string `json:"password,omitempty"`
}

func shopDomain() (*Domain, *Aggregate[cart], *Listener, *Listener, *sync.Map) {
	shop := NewDomain("shop")
	shop.Command("Register").Personal([]string{"email"}, func(_ map[string]any, id string) string { return id }).Secret("password")
	shop.Event("CustomerRegistered", 1).Personal([]string{"email"}, func(_ map[string]any, id string) string { return id })
	c := NewAggregate(shop, "cart", cart{})
	Apply(c, "ItemAdded", func(_ *ApplyContext, s *cart, d addItem) { s.Items += d.Qty })
	Apply(c, "CheckedOut", func(_ *ApplyContext, s *cart, _ struct{}) { s.CheckedOut = true })
	Handle(c, "AddItem", func(ctx *CommandContext, s cart, d addItem) ([]NewEvent, error) {
		if s.CheckedOut {
			return nil, Reject("checked_out", "Cart is checked out")
		}
		return Events(Event("ItemAdded", d)), nil
	})
	Handle(c, "CheckOut", func(ctx *CommandContext, s cart, _ struct{}) ([]NewEvent, error) {
		if s.Items == 0 {
			return nil, Reject("empty", "Cart is empty")
		}
		return Events(Event("CheckedOut", map[string]any{"at": ctx.Now().UnixMilli(), "id": ctx.ID()})), nil
	})
	customer := NewAggregate(shop, "customer", struct{}{})
	Handle(customer, "Register", func(ctx *CommandContext, _ struct{}, d register) ([]NewEvent, error) {
		if d.Password != "secret" {
			return nil, Reject("password", "the handler gets the secret in clear")
		}
		return Events(Event("CustomerRegistered", map[string]any{"email": d.Email})), nil
	})
	rows := &sync.Map{}
	carts := shop.ReadModel("carts")
	On(carts, "ItemAdded", func(ctx *ListenerContext, d addItem) error {
		v, _ := rows.LoadOrStore(ctx.Event.AggregateID, 0)
		rows.Store(ctx.Event.AggregateID, v.(int)+d.Qty)
		return nil
	})
	On(carts, "CustomerRegistered", func(ctx *ListenerContext, d map[string]any) error {
		rows.Store("email:"+ctx.Event.AggregateID, d["email"])
		return nil
	})
	Answer(carts, "GetCart", func(ctx *QueryContext, d struct{ ID string }) (map[string]int, error) {
		v, ok := rows.Load(d.ID)
		if !ok {
			return nil, Reject("not_found", "no cart "+d.ID)
		}
		return map[string]int{"items": v.(int)}, nil
	})
	Answer(carts, "GetEmail", func(ctx *QueryContext, d struct{ ID string }) (any, error) {
		v, _ := rows.Load("email:" + d.ID)
		return v, nil
	})
	checkout := shop.Reactor("checkout")
	On(checkout, "ItemAdded", func(ctx *ListenerContext, d addItem) error {
		if d.SKU != "last" {
			return nil
		}
		_, err := ctx.Submit(Command{AggregateType: "cart", AggregateID: ctx.Event.AggregateID, Type: "CheckOut", ID: "checkout-" + ctx.Event.AggregateID})
		return err
	})
	return shop, c, carts, checkout, rows
}

func TestShop(t *testing.T) {
	k := newTestKernel(t, []string{"shop.*"}, kernelOptions{})
	k.start()
	shop, c, carts, checkout, _ := shopDomain()
	client := k.client().Use(shop)
	worker := client.Worker(c, mustAggregate(shop, "customer"))
	running(t, func(ctx context.Context) error { return worker.Run(ctx) })
	model := client.Listener(carts)
	running(t, model.Run)
	reactor := client.Listener(checkout)
	running(t, reactor.Run)
	waitFor(t, worker.Ready(), "worker")
	waitFor(t, model.Ready(), "read model")
	waitFor(t, reactor.Ready(), "reactor")
	ctx := context.Background()

	add := func(id string, item addItem) Outcome {
		return must[Outcome](t)(client.Submit(ctx, Command{Domain: "shop", AggregateType: "cart", AggregateID: id, Type: "AddItem", Data: item}))
	}
	if o := add("c1", addItem{"apple", 2}); !o.Committed || len(o.Events) != 1 || o.Events[0].Type != "ItemAdded" {
		t.Fatalf("outcome %+v", o)
	}
	got := must[map[string]int](t)(QueryAs[map[string]int](ctx, client, Query{Domain: "shop", Type: "GetCart", Data: map[string]string{"id": "c1"}}))
	if got["items"] != 2 {
		t.Fatalf("read your writes: %v", got)
	}
	_, err := client.Query(ctx, Query{Domain: "shop", Type: "GetCart", Data: map[string]string{"id": "nope"}})
	var r *Rejected
	if !errors.As(err, &r) || r.Code != "not_found" {
		t.Fatalf("a rejected query: %v", err)
	}

	// The reactor checks the cart out when the last item comes; the next AddItem is rejected.
	add("c1", addItem{"last", 1})
	for i := 0; i < 200; i++ {
		o := add("c1", addItem{"pear", 1})
		if !o.Committed {
			if o.Code != "checked_out" {
				t.Fatalf("rejection %+v", o)
			}
			break
		}
		if i == 199 {
			t.Fatal("the reactor never checked the cart out")
		}
	}
	// Idempotent on the ID: the same command again returns the same outcome.
	first := must[Outcome](t)(client.Submit(ctx, Command{Domain: "shop", AggregateType: "cart", AggregateID: "c2", Type: "AddItem", ID: "same", Data: addItem{"x", 1}}))
	again := must[Outcome](t)(client.Submit(ctx, Command{Domain: "shop", AggregateType: "cart", AggregateID: "c2", Type: "AddItem", ID: "same", Data: addItem{"x", 1}}))
	if first.Events[0].Position != again.Events[0].Position {
		t.Fatalf("submitted twice: %d, %d", first.Events[0].Position, again.Events[0].Position)
	}

	// Vault: the email is personal, the password a secret; the read model gets the email back.
	o := must[Outcome](t)(client.Submit(ctx, Command{Domain: "shop", AggregateType: "customer", AggregateID: "k1", Type: "Register", Data: register{"ann@example.com", "secret"}}))
	if !o.Committed {
		t.Fatalf("register: %+v", o)
	}
	if string(o.Events[0].Data) == `{"email":"ann@example.com"}` || len(o.Events[0].Redacted) != 1 {
		t.Fatalf("the email is in the log in clear: %s %v", o.Events[0].Data, o.Events[0].Redacted)
	}
	email := must[string](t)(QueryAs[string](ctx, client, Query{Domain: "shop", Type: "GetEmail", Data: map[string]string{"id": "k1"}}))
	if email != "ann@example.com" {
		t.Fatalf("the read model got %q", email)
	}
	if v := k.verify(); !containsLine(v, "OK") {
		t.Fatalf("verify: %s", v)
	}
}

func mustAggregate(d *Domain, name string) AnyAggregate {
	for _, a := range d.aggregates {
		if a.typeName() == name {
			return a
		}
	}
	panic(name)
}

func containsLine(s, line string) bool {
	for _, l := range splitLines(s) {
		if l == line {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := range s {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}
