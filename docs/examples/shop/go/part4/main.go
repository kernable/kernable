// Part 4: the reactor confirms a submitted order.
package main

import (
	"context"
	"fmt"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
)

func main() {
	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	type m = map[string]any
	send := func(aggregateType, aggregateID, commandType string, data any) {
		if _, err := client.Submit(ctx, kernable.Command{Domain: "shop", AggregateType: aggregateType, AggregateID: aggregateID, Type: commandType, Data: data}); err != nil {
			log.Fatal(err)
		}
	}

	// #region try
	send("customer", "c1", "Register", m{"email": "ann@example.com", "name": "Ann"})
	send("order", "o1", "Place", m{"customerId": "c1"})
	send("order", "o1", "AddItem", m{"sku": "apple", "qty": 2})
	send("order", "o1", "Submit", nil)
	// The submit settled after the reactor, which confirmed the order before it returned.
	order, err := client.Query(ctx, kernable.Query{Domain: "shop", Type: "GetOrder", Data: m{"id": "o1"}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(order))
	// #endregion try
}
