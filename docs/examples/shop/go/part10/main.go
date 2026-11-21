// Part 10: the fulfilment process ships a paid order and cancels an unpaid one.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

type orderRow struct {
	Status    string `json:"status"`
	Cancelled string `json:"cancelled"`
}

func main() {
	client, err := kernable.New(kernable.Options{Context: kernable.RequestContext{SenderIdentity: "c1"}})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	client.Use(shop.Domain)
	ctx := context.Background()
	type m = map[string]any
	send := func(aggregateType, aggregateID, commandType string, data any) {
		if _, err := client.Submit(ctx, kernable.Command{Domain: "shop", AggregateType: aggregateType, AggregateID: aggregateID, Type: commandType, Data: data}); err != nil {
			log.Fatal(err)
		}
	}
	// The order once it is status: the process moves it on its own, a little later.
	until := func(id, status string) orderRow {
		for {
			order, err := kernable.QueryAs[orderRow](ctx, client, kernable.Query{Domain: "shop", Type: "GetOrder", Data: m{"id": id}})
			if err != nil {
				log.Fatal(err)
			}
			if order.Status == status {
				return order
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// #region try
	send("customer", "c1", "Register", m{"email": "ann@example.com", "name": "Ann"})
	for _, id := range []string{"o1", "o2"} {
		send("order", id, "Place", m{"customerId": "c1"})
		send("order", id, "AddItem", m{"sku": "apple", "qty": 1})
		send("order", id, "Submit", nil) // confirmed by the reactor: a case starts for the order
	}
	send("order", "o1", "Pay", nil)
	fmt.Println("o1:", until("o1", "shipped").Status)
	// o2 is never paid: when its time is up, the process cancels it.
	o2 := until("o2", "cancelled")
	fmt.Println("o2:", o2.Status, o2.Cancelled)
	// #endregion try
}
