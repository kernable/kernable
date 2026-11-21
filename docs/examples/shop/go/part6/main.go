// Part 6: the guard decides who may do what.
package main

import (
	"context"
	"errors"
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
	show := func(what string) func(kernable.Outcome, error) {
		return func(o kernable.Outcome, err error) {
			if err != nil {
				log.Fatal(err)
			}
			if o.Committed {
				fmt.Printf("%s: committed\n", what)
			} else {
				fmt.Printf("%s: %s (%s)\n", what, o.Code, o.Message)
			}
		}
	}

	// #region try
	// Who sends a request is part of its signed request context.
	as := func(who string) *kernable.RequestContext { return &kernable.RequestContext{SenderIdentity: who} }
	send := func(who, aggregateType, aggregateID, commandType string, data any) (kernable.Outcome, error) {
		return client.Submit(ctx, kernable.Command{Domain: "shop", AggregateType: aggregateType, AggregateID: aggregateID, Type: commandType, Data: data, Context: as(who)})
	}

	show("c1 registers")(send("c1", "customer", "c1", "Register", m{"email": "ann@example.com", "name": "Ann"}))
	show("c2 registers")(send("c2", "customer", "c2", "Register", m{"email": "bob@example.com", "name": "Bob"}))
	show("c1 places o1")(send("c1", "order", "o1", "Place", m{"customerId": "c1"}))
	show("c2 adds to o1")(send("c2", "order", "o1", "AddItem", m{"sku": "apple", "qty": 1}))
	show("c2 places for c1")(send("c2", "order", "o2", "Place", m{"customerId": "c1"}))
	for _, who := range []string{"c1", "c2"} {
		order, err := client.Query(ctx, kernable.Query{Domain: "shop", Type: "GetOrder", Data: m{"id": "o1"}, Context: as(who)})
		var rejected *kernable.Rejected
		switch {
		case errors.As(err, &rejected):
			fmt.Printf("%s reads o1: %s (%s)\n", who, rejected.Code, rejected.Message)
		case err != nil:
			log.Fatal(err)
		default:
			fmt.Printf("%s reads o1: %s\n", who, order)
		}
	}
	// #endregion try
}
