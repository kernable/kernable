package main

// The parity shop: the same in every language.

import (
	"slices"
	"sync"

	kernable "github.com/kernable/kernable/sdks/go"
	"github.com/kernable/kernable/sdks/go/kit"
)

type OrderPlaced struct {
	Customer string `json:"customer"`
}

type OrderDetails struct {
	Customer string `json:"customer"`
}

type GetOrder struct {
	ID string `json:"id"`
}

type Order struct {
	Status string `json:"status"`
}

type Parcel struct {
	Requested bool `json:"requested"`
	Shipped   bool `json:"shipped"`
}

type none = struct{}

func shop() *kernable.Domain {
	shop := kernable.NewDomain("shop")
	order := kernable.NewAggregate(shop, "order", Order{Status: "none"})
	kernable.Apply(order, "OrderPlaced", func(_ *kernable.ApplyContext, s *Order, _ none) { s.Status = "placed" })
	kernable.Apply(order, "OrderCancelled", func(_ *kernable.ApplyContext, s *Order, _ none) { s.Status = "cancelled" })
	kernable.Handle(order, "PlaceOrder", func(c *kernable.CommandContext, s Order, _ none) ([]kernable.NewEvent, error) {
		if s.Status != "none" {
			return nil, kernable.Reject("exists", "The order exists")
		}
		return kernable.Events(kernable.Event("OrderPlaced", OrderPlaced{Customer: c.Request.SenderIdentity})), nil
	})
	kernable.Handle(order, "CancelOrder", func(_ *kernable.CommandContext, s Order, _ none) ([]kernable.NewEvent, error) {
		if s.Status != "placed" {
			return nil, kernable.Reject("invalid_state", "Not placed")
		}
		return kernable.Events(kernable.Event("OrderCancelled", none{})), nil
	})
	kit.Relation("shop.order", "owner").Set("shop.OrderPlaced", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["customer"]) })

	type row struct{ tenant, id string }
	var mu sync.Mutex
	var rows []row
	orders := shop.ReadModel("shop.orders")
	kernable.On(orders, "OrderPlaced", func(c *kernable.ListenerContext, _ OrderPlaced) error {
		mu.Lock()
		defer mu.Unlock()
		r := row{c.Event.Tenant, c.Event.AggregateID}
		if !slices.Contains(rows, r) {
			rows = append(rows, r)
		}
		return nil
	})
	visible := kit.Visibility(orders)
	kernable.Answer(orders, "ListOrders", func(c *kernable.QueryContext, _ none) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		mine := []string{}
		for _, r := range rows {
			if r.tenant == c.Tenant && visible.Allows(c.Store, c.Tenant, c.Request, "shop.GetOrder", kit.Target{Type: "shop.order", ID: r.id}) {
				mine = append(mine, r.id)
			}
		}
		slices.Sort(mine)
		return mine, nil
	})
	customers := map[string]string{}
	details := shop.ReadModel("shop.order-details")
	kernable.On(details, "OrderPlaced", func(c *kernable.ListenerContext, d OrderPlaced) error {
		mu.Lock()
		defer mu.Unlock()
		customers[c.Event.Tenant+"/"+c.Event.AggregateID] = d.Customer
		return nil
	})
	shop.Query("GetOrder").Target("order", "id")
	kernable.Answer(details, "GetOrder", func(c *kernable.QueryContext, d GetOrder) (OrderDetails, error) {
		mu.Lock()
		defer mu.Unlock()
		customer, ok := customers[c.Tenant+"/"+d.ID]
		if !ok {
			return OrderDetails{}, kernable.Reject("not_found", "No such order")
		}
		return OrderDetails{Customer: customer}, nil
	})

	parcel := kernable.NewAggregate(shop, "parcel", Parcel{})
	kernable.Apply(parcel, "ParcelRequested", func(_ *kernable.ApplyContext, s *Parcel, _ none) { s.Requested = true })
	kernable.Apply(parcel, "ParcelShipped", func(_ *kernable.ApplyContext, s *Parcel, _ none) { s.Shipped = true })
	kernable.Handle(parcel, "RequestParcel", func(_ *kernable.CommandContext, _ Parcel, _ none) ([]kernable.NewEvent, error) {
		return kernable.Events(kernable.Event("ParcelRequested", none{})), nil
	})
	kernable.Handle(parcel, "ShipParcel", func(_ *kernable.CommandContext, s Parcel, _ none) ([]kernable.NewEvent, error) {
		if s.Shipped {
			return nil, kernable.Reject("invalid_state", "Shipped already")
		}
		return kernable.Events(kernable.Event("ParcelShipped", none{})), nil
	})
	shop.Process("dispatch").
		Start("ParcelRequested", kernable.StartOptions{Key: "aggregateId", Data: kernable.Mapping{"parcelId": "aggregateId"}}).
		Command("ship", "parcel.ShipParcel", kernable.CommandStep{Aggregate: "case.parcelId"}).
		End("done")
	return shop
}
