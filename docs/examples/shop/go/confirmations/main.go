// The reactor: confirms every submitted order to the customer's email.
package main

import (
	"context"
	"encoding/json"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

// #region reactor
func main() {
	// Who this service is (part 6), and the domain's declarations, e.g. which fields are personal (part 7).
	client, err := kernable.New(kernable.Options{Context: kernable.RequestContext{SenderIdentity: "svc-confirmations"}})
	if err != nil {
		log.Fatal(err)
	}
	client.Use(shop.Domain)

	confirmations := shop.Domain.Reactor("confirmations")
	kernable.On(confirmations, "OrderSubmitted", func(ctx *kernable.ListenerContext, _ struct{}) error {
		// The read models (order 0) have processed this event before the reactor (order 10) gets it.
		var order struct {
			CustomerID string `json:"customerId"`
		}
		var customer struct {
			Email string `json:"email"`
		}
		if err := query(ctx, "GetOrder", ctx.Event.AggregateID, &order); err != nil {
			return err
		}
		if err := query(ctx, "GetCustomer", order.CustomerID, &customer); err != nil {
			return err
		}
		// Same sender and correlation as the submit that caused the event; a rejection parks the event.
		_, err := ctx.Submit(kernable.Command{
			AggregateType: "order",
			AggregateID:   ctx.Event.AggregateID,
			Type:          "Confirm",
			Data:          shop.Confirm{CustomerID: order.CustomerID, Email: customer.Email},
			ID:            "confirm-" + ctx.Event.AggregateID, // a redelivered event confirms once
		})
		return err
	})
	log.Fatal(client.Listener(confirmations).Run(context.Background()))
}

func query(ctx *kernable.ListenerContext, queryType, id string, into any) error {
	answer, err := ctx.Query(queryType, map[string]string{"id": id})
	if err != nil {
		return err
	}
	return json.Unmarshal(answer, into)
}

// #endregion reactor
