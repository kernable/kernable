// The guard: a middleware that lets customers act only on themselves and their own orders.
package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

// #region guard
var services = []string{"svc-confirmations"}

// What a business process may send (part 10): it acts as process:<name>.
var processes = map[string][]string{"process:shop.fulfilment": {"Ship", "Cancel"}}
var registered = map[string]bool{}
var ownerOf = map[string]string{} // order -> customer

func main() {
	guard := shop.Domain.ReadModel("guard")
	kernable.On(guard, "CustomerRegistered", func(ctx *kernable.ListenerContext, _ struct{}) error {
		registered[ctx.Event.AggregateID] = true
		return nil
	})
	kernable.On(guard, "CustomerErased", func(ctx *kernable.ListenerContext, _ struct{}) error {
		delete(registered, ctx.Event.AggregateID)
		return nil
	})
	kernable.On(guard, "OrderPlaced", func(ctx *kernable.ListenerContext, d shop.Place) error {
		ownerOf[ctx.Event.AggregateID] = d.CustomerID
		return nil
	})
	guard.Middleware(kernable.MiddlewareOptions{Types: []string{"shop.*"}}, func(_ *kernable.MiddlewareContext, check kernable.Check) error {
		who := check.Request.SenderIdentity
		if check.Type == "Register" {
			return nil // anyone may sign up
		}
		if check.Kind == "query" && slices.Contains(services, who) {
			return nil // services may read
		}
		if check.Kind == "command" && slices.Contains(processes[who], check.Type) {
			return nil
		}
		// Files and cache requests carry no data: they have no owner, so only the rules above pass them.
		var data struct {
			ID         string `json:"id"`
			CustomerID string `json:"customerId"`
		}
		_ = json.Unmarshal(check.Data, &data)
		target := data.ID
		if check.Kind == "command" {
			target = check.AggregateID
		}
		var owner string
		switch {
		case check.Type == "Place":
			owner = data.CustomerID
		case check.Type == "GetCustomer" || check.AggregateType == "customer":
			owner = target
		default:
			owner = ownerOf[target]
		}
		if !registered[who] || owner != who {
			if who == "" {
				who = "anonymous"
			}
			return kernable.Reject("forbidden", check.Type+" is not allowed for "+who)
		}
		return nil
	})

	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(client.Listener(guard).Run(context.Background()))
}

// #endregion guard
