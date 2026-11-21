// #region file
package main

import (
	"context"
	"log"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
)

// #region aggregate
type Cart struct {
	Items      int  `json:"items"`
	CheckedOut bool `json:"checkedOut"`
}

var shopDomain = kernable.NewDomain("shop")
var cartAggregate = kernable.NewAggregate(shopDomain, "cart", Cart{})

// #endregion aggregate

type AddItem struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

func main() {
	// #region events
	kernable.Apply(cartAggregate, "ItemAdded", func(_ *kernable.ApplyContext, s *Cart, d AddItem) { s.Items += d.Qty })
	kernable.Apply(cartAggregate, "CheckedOut", func(_ *kernable.ApplyContext, s *Cart, _ struct{}) { s.CheckedOut = true })
	// #endregion events

	// #region commands
	kernable.Handle(cartAggregate, "AddItem", func(_ *kernable.CommandContext, s Cart, d AddItem) ([]kernable.NewEvent, error) {
		if s.CheckedOut {
			return nil, kernable.Reject("", "Cart is checked out")
		}
		return kernable.Events(kernable.Event("ItemAdded", d)), nil
	})

	kernable.Handle(cartAggregate, "CheckOut", func(ctx *kernable.CommandContext, s Cart, _ struct{}) ([]kernable.NewEvent, error) {
		if s.Items == 0 {
			return nil, kernable.Reject("", "Cart is empty")
		}
		if s.CheckedOut {
			return nil, kernable.Reject("", "Cart is checked out")
		}
		// ctx.Now() and ctx.ID() stay the same if the command runs again.
		return kernable.Events(kernable.Event("CheckedOut", map[string]string{"at": ctx.Now().UTC().Format(time.RFC3339Nano), "orderId": ctx.ID()})), nil
	})
	// #endregion commands

	// #region worker
	client, err := kernable.New(kernable.Options{Key: "app.key"})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(client.Worker(cartAggregate).Run(context.Background()))
	// #endregion worker
}

// #endregion file
