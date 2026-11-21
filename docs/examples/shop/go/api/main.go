// The shop's API (part 9): the kit's domains with the authorization middleware, and the kit's
// example HTTP server. People sign in here; the kernel and the kit decide what they may do.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	kernable "github.com/kernable/kernable/sdks/go"
	"github.com/kernable/kernable/sdks/go/kit"
	"github.com/kernable/kernable/sdks/go/kit/example"
	"shop"
)

func main() {
	ctx := context.Background()
	// #region relation
	// Who owns an order: the customer it was placed for. Declared before the kit's domains start.
	kit.Relation("shop.order", "owner").Set("shop.OrderPlaced", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["customerId"]) })
	// #endregion relation

	// #region mail
	// In development the kit's mails go to files in mail/, named by address, instead of a mail server.
	_ = os.MkdirAll("mail", 0o755)
	kit.UseMailer(kit.MailerFunc(func(m kit.Mail) { _ = os.WriteFile("mail/"+m.To, []byte(m.Token), 0o644) }))
	// #endregion mail

	// #region rule
	// A rule of the shop that no group expresses: a new order's customer is whoever places it. A
	// middleware of the shop's own, after the kit's (order 0), so the sender is checked already.
	ownOrders := shop.Domain.ReadModel("own-orders")
	ownOrders.Middleware(kernable.MiddlewareOptions{Types: []string{"shop.Place"}}, func(_ *kernable.MiddlewareContext, check kernable.Check) error {
		var d shop.Place
		_ = json.Unmarshal(check.Data, &d)
		if d.CustomerID != check.Request.SenderIdentity {
			return kernable.Reject("forbidden", "An order is placed by its own customer")
		}
		return nil
	})
	go func() { log.Fatal(connect().Listener(ownOrders).Run(ctx)) }()
	// #endregion rule

	// #region run
	// The kit's domains: its aggregates, read models, reactors and the authorization middleware.
	app, err := connect().App(kernable.AppOptions{Service: "kit", Domains: kit.KitDomains()})
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(app.Run(ctx)) }()
	<-app.Ready()

	// The API's client knows the shop's schemas too: GetOrder declares the order it is about.
	api := connect().Use(append(kit.KitDomains(), shop.Domain)...)
	// Once: the platform tenant, the sign-up policy, and the API's own identity and key.
	booted, err := kit.Bootstrap(ctx, api, kit.SignUpPolicy{Accounts: kit.AccountSignUpOpen, Tenants: kit.TenantSignUpActive}, "Platform", nil)
	if err != nil {
		log.Fatal(err)
	}
	kitAPI := kit.NewKitAPI(api, kit.NewSigningKeys(), booted.APIIdentity, booted.APIKeyID)
	log.Fatal(http.ListenAndServe(":"+cmp(os.Getenv("PORT"), "3000"), example.Server(kitAPI, api)))
	// #endregion run
}

func connect() *kernable.Client {
	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func cmp(v, otherwise string) string {
	if v != "" {
		return v
	}
	return otherwise
}
