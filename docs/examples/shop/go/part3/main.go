// Part 3: read your own writes through queries.
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
	send := func(aggregateType, aggregateID, commandType string, data any) {
		if _, err := client.Submit(ctx, kernable.Command{Domain: "shop", AggregateType: aggregateType, AggregateID: aggregateID, Type: commandType, Data: data}); err != nil {
			log.Fatal(err)
		}
	}

	// #region try
	query := func(queryType, id string) {
		answer, err := client.Query(ctx, kernable.Query{Domain: "shop", Type: queryType, Data: m{"id": id}})
		var rejected *kernable.Rejected
		switch {
		case errors.As(err, &rejected):
			fmt.Printf("%s: %s\n", rejected.Code, rejected.Message)
		case err != nil:
			log.Fatal(err)
		default:
			fmt.Println(string(answer))
		}
	}

	send("customer", "c1", "Register", m{"email": "ann@example.com", "name": "Ann"})
	send("order", "o1", "Place", m{"customerId": "c1"})
	send("order", "o1", "AddItem", m{"sku": "apple", "qty": 2})
	// The submit returned after the read model processed the event: the query sees it.
	query("GetOrder", "o1")
	query("GetCustomer", "c1")
	query("GetOrder", "o9")
	// #endregion try
}
