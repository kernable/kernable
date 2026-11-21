package kit

// Lists that show only what one may see: the middleware decides on single requests; a read model
// answering a list filters its rows with the same decision. Visibility(listener) teaches an app's
// read model what the middleware knows; ask it in the query handler:
//
//	orders := shop.ReadModel("shop.orders")
//	kernable.On(orders, "OrderPlaced", …)       // your handlers first
//	visible := kit.Visibility(orders)
//	kernable.Answer(orders, "ListOrders", func(ctx *kernable.QueryContext, _ ListOrders) ([]Order, error) {
//		… visible.Allows(ctx.Store, ctx.Tenant, ctx.Request, "shop.GetOrder", kit.Target{Type: "shop.order", ID: o.ID}) …
//	})

import (
	"encoding/json"
	"strings"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
)

// Visible is what the middleware knows, in an app's read model: ask Allows.
type Visible struct {
	// Without a store: memory.
	memory *MemoryKeyValues
}

func (v Visible) knowledge(store *kernable.Store) Knowledge {
	if store != nil {
		return Knowledge{KV: storeKV{s: store, prefix: "kit:"}}
	}
	return Knowledge{KV: v.memory}
}

// Allows is whether the asker (request) may typ (a qualified type, e.g. shop.GetOrder) on target,
// as the middleware would decide. store: the query handler's ctx.Store.
func (v Visible) Allows(store *kernable.Store, tenant string, request kernable.RequestContext, typ string, target Target) bool {
	domain, name, ok := strings.Cut(typ, ".")
	if !ok {
		domain, name = "", typ
	}
	_, aggregate, _ := strings.Cut(target.Type, ".")
	check := kernable.Check{Kind: "command", Tenant: tenant, Domain: domain, Type: name, AggregateType: aggregate,
		AggregateID: target.ID, Data: json.RawMessage("{}"), Request: request}
	return Decide(v.knowledge(store), check, time.Now().UnixMilli(), nil, false) == nil
}

// Visibility adds the middleware's event handlers to listener (after its own, for the same
// events: call it after registering yours) and returns what answers Visible.Allows. Declare the
// relations first. Kept in the listener's store, if it has one, else in memory.
func Visibility(listener *kernable.Listener) Visible {
	v := Visible{memory: &MemoryKeyValues{}}
	for _, typ := range LearnedEvents() {
		kernable.AlsoOn(listener, typ, func(c *lc, payload json.RawMessage) error {
			return Learn(v.knowledge(c.Store), Seen{Type: typ, Tenant: c.Event.Tenant, AggregateID: c.Event.AggregateID,
				Domain: c.Event.Domain, AggregateType: c.Event.AggregateType}, payload)
		})
	}
	return v
}
