// #region file
package main

import (
	"context"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
)

// #region register
type Row struct {
	Items int    `json:"items"`
	Order string `json:"order,omitempty"`
}

var shopDomain = kernable.NewDomain("shop")
var cartsReadModel = shopDomain.ReadModel("carts")
var rows = map[string]*Row{}

// #endregion register

func main() {
	// #region events
	kernable.On(cartsReadModel, "ItemAdded", func(ctx *kernable.ListenerContext, d struct{ Qty int }) error {
		row, ok := rows[ctx.Event.AggregateID]
		if !ok {
			row = &Row{}
			rows[ctx.Event.AggregateID] = row
		}
		row.Items += d.Qty
		return nil
	})

	kernable.On(cartsReadModel, "CheckedOut", func(ctx *kernable.ListenerContext, d struct{ OrderID string }) error {
		rows[ctx.Event.AggregateID].Order = d.OrderID
		return nil
	})
	// #endregion events

	// #region queries
	kernable.Answer(cartsReadModel, "GetCart", func(_ *kernable.QueryContext, d struct{ ID string }) (*Row, error) {
		row, ok := rows[d.ID]
		if !ok {
			return nil, kernable.Reject("not_found", "no such cart")
		}
		return row, nil
	})
	// #endregion queries

	// #region run
	client, err := kernable.New(kernable.Options{Key: "app.key"})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(client.Listener(cartsReadModel).Run(context.Background()))
	// #endregion run
}

// #endregion file
