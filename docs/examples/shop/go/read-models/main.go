// The read models: customers and orders, in memory, answering queries.
package main

import (
	"context"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

type byID struct {
	ID string `json:"id"`
}

// #region customers
type CustomerRow struct {
	ID    string  `json:"id"`
	Email *string `json:"email"`
	Name  *string `json:"name"`
}

var customers = map[string]CustomerRow{}

var customersReadModel = shop.Domain.ReadModel("customers")

func init() {
	kernable.On(customersReadModel, "CustomerRegistered", func(ctx *kernable.ListenerContext, d struct{ Email, Name *string }) error {
		customers[ctx.Event.AggregateID] = CustomerRow{ID: ctx.Event.AggregateID, Email: d.Email, Name: d.Name}
		return nil
	})
	kernable.Answer(customersReadModel, "GetCustomer", func(_ *kernable.QueryContext, d byID) (CustomerRow, error) {
		row, ok := customers[d.ID]
		if !ok {
			return row, kernable.Reject("not_found", "No customer "+d.ID)
		}
		return row, nil
	})
}

// #endregion customers

// #region orders
type OrderRow struct {
	ID          string         `json:"id"`
	CustomerID  string         `json:"customerId"`
	Status      string         `json:"status"`
	Items       map[string]int `json:"items"`
	ConfirmedTo *string        `json:"confirmedTo,omitempty"`
	Cancelled   string         `json:"cancelled,omitempty"`
}

var orders = map[string]*OrderRow{}

var ordersReadModel = shop.Domain.ReadModel("orders")

func init() {
	kernable.On(ordersReadModel, "OrderPlaced", func(ctx *kernable.ListenerContext, d shop.Place) error {
		orders[ctx.Event.AggregateID] = &OrderRow{ID: ctx.Event.AggregateID, CustomerID: d.CustomerID, Status: "draft", Items: map[string]int{}}
		return nil
	})
	kernable.On(ordersReadModel, "ItemAdded", func(ctx *kernable.ListenerContext, d shop.Item) error {
		orders[ctx.Event.AggregateID].Items[d.SKU] += d.Qty
		return nil
	})
	kernable.On(ordersReadModel, "OrderSubmitted", func(ctx *kernable.ListenerContext, _ struct{}) error {
		orders[ctx.Event.AggregateID].Status = "submitted"
		return nil
	})
	kernable.Answer(ordersReadModel, "GetOrder", func(_ *kernable.QueryContext, d byID) (*OrderRow, error) {
		row, ok := orders[d.ID]
		if !ok {
			return nil, kernable.Reject("not_found", "No order "+d.ID)
		}
		return row, nil
	})
}

// #endregion orders

// #region confirmed
func init() {
	kernable.On(ordersReadModel, "OrderConfirmed", func(ctx *kernable.ListenerContext, d struct{ Email *string }) error {
		row := orders[ctx.Event.AggregateID]
		row.Status = "confirmed"
		row.ConfirmedTo = d.Email
		return nil
	})
}

// #endregion confirmed

// #region erased
// An erased customer's personal data must not outlive the erasure in a read model either.
func init() {
	kernable.On(customersReadModel, "CustomerErased", func(ctx *kernable.ListenerContext, _ struct{}) error {
		delete(customers, ctx.Event.AggregateID)
		return nil
	})
	kernable.On(ordersReadModel, "CustomerErased", func(ctx *kernable.ListenerContext, _ struct{}) error {
		for _, row := range orders {
			if row.CustomerID == ctx.Event.AggregateID && row.ConfirmedTo != nil {
				row.ConfirmedTo = nil
			}
		}
		return nil
	})
}

// #endregion erased

// #region fulfilled
func init() {
	status := func(s string) func(*kernable.ListenerContext, struct{}) error {
		return func(ctx *kernable.ListenerContext, _ struct{}) error {
			orders[ctx.Event.AggregateID].Status = s
			return nil
		}
	}
	kernable.On(ordersReadModel, "OrderPaid", status("paid"))
	kernable.On(ordersReadModel, "OrderShipped", status("shipped"))
	kernable.On(ordersReadModel, "OrderCancelled", func(ctx *kernable.ListenerContext, d shop.Cancel) error {
		row := orders[ctx.Event.AggregateID]
		row.Status = "cancelled"
		row.Cancelled = d.Reason
		return nil
	})
}

// #endregion fulfilled

// #region run
func main() {
	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	go func() { log.Fatal(client.Listener(customersReadModel).Run(ctx)) }()
	log.Fatal(client.Listener(ordersReadModel).Run(ctx))
}

// #endregion run
