// Part 2: commands, rejections and idempotency.
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
	show := func(what string) func(kernable.Outcome, error) {
		return func(o kernable.Outcome, err error) {
			if err != nil {
				log.Fatal(err)
			}
			if o.Committed {
				fmt.Printf("%s: committed, version %d\n", what, o.Events[len(o.Events)-1].Version)
			} else {
				fmt.Printf("%s: %s (%s)\n", what, o.Code, o.Message)
			}
		}
	}

	// #region try
	send := func(aggregateType, aggregateID, commandType string, data any, id string) (kernable.Outcome, error) {
		return client.Submit(ctx, kernable.Command{Domain: "shop", AggregateType: aggregateType, AggregateID: aggregateID, Type: commandType, Data: data, ID: id})
	}
	type m = map[string]any

	show("register")(send("customer", "c1", "Register", m{"email": "ann@example.com", "name": "Ann"}, ""))
	show("register again")(send("customer", "c1", "Register", m{"email": "ann@example.com", "name": "Ann"}, ""))
	show("place")(send("order", "o1", "Place", m{"customerId": "c1"}, ""))
	show("add apples")(send("order", "o1", "AddItem", m{"sku": "apple", "qty": 2}, "add-1"))
	show("same command again")(send("order", "o1", "AddItem", m{"sku": "apple", "qty": 2}, "add-1"))
	show("submit")(send("order", "o1", "Submit", nil, ""))
	show("add after submit")(send("order", "o1", "AddItem", m{"sku": "pear", "qty": 1}, ""))
	// #endregion try
}
