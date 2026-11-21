package main

import (
	"context"
	"fmt"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
)

func main() {
	client, err := kernable.New(kernable.Options{Key: "app.key"})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	cartC1 := kernable.Command{Domain: "shop", AggregateType: "cart", AggregateID: "c1"}
	command := func(commandType string, data any) kernable.Outcome {
		c := cartC1
		c.Type, c.Data = commandType, data
		outcome, err := client.Submit(ctx, c)
		if err != nil {
			log.Fatal(err)
		}
		return outcome
	}
	getCart := func() string {
		cart, err := client.Query(ctx, kernable.Query{Domain: "shop", Type: "GetCart", Data: map[string]string{"id": "c1"}})
		if err != nil {
			log.Fatal(err)
		}
		return string(cart)
	}

	command("AddItem", map[string]any{"sku": "apple", "qty": 2})
	// Submit returned only after every listener processed the event: no polling needed.
	fmt.Println(getCart())
	// {"items":2}

	command("CheckOut", nil)
	fmt.Println(getCart())
	// {"items":2,"order":"0b9c5e1a-6f3d-8a42-9c1e-5d7f2b8e4a10"}

	outcome := command("AddItem", map[string]any{"sku": "pear", "qty": 1})
	fmt.Printf("committed: %v, %s: %s\n", outcome.Committed, outcome.Code, outcome.Message)
	// committed: false, rejected: Cart is checked out
}
