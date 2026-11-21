// The customers read model with a store: it keeps its rows in a file and continues after a
// restart where it left off, instead of reading every event again.
package main

import (
	"context"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

// #region persistent
type CustomerRow struct {
	ID    string  `json:"id"`
	Email *string `json:"email"`
	Name  *string `json:"name"`
}

func main() {
	customers := shop.Domain.ReadModel("customers", kernable.WithStore("sqlite:customers.db"), kernable.StoreSchema("1"))
	kernable.On(customers, "CustomerRegistered", func(ctx *kernable.ListenerContext, d struct{ Email, Name *string }) error {
		return ctx.Store.Set("customers", ctx.Event.AggregateID, CustomerRow{ID: ctx.Event.AggregateID, Email: d.Email, Name: d.Name})
	})
	kernable.On(customers, "CustomerErased", func(ctx *kernable.ListenerContext, _ struct{}) error {
		return ctx.Store.Remove("customers", ctx.Event.AggregateID)
	})
	kernable.Answer(customers, "GetCustomer", func(ctx *kernable.QueryContext, d struct{ ID string }) (CustomerRow, error) {
		var row CustomerRow
		found, err := ctx.Store.Get("customers", d.ID, &row)
		if err == nil && !found {
			err = kernable.Reject("not_found", "No customer "+d.ID)
		}
		return row, err
	})

	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(client.Listener(customers).Run(context.Background()))
}

// #endregion persistent
