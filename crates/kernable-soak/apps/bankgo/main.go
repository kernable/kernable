// The soak's Go app: domain bank_go, the same as bank.ts, bank.py and the Rust app. Accounts take
// deposits and withdrawals (never below zero); a read model keeps balances in a store; a reactor
// flags every withdrawal of 50 or more, once. An account holds its number as a unique key and,
// while open, the fact open, which a withdrawal sent to an account requires of that account. The
// soak checks all three against the log.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	kernable "github.com/kernable/kernable/sdks/go"
)

type account struct {
	Balance int64   `json:"balance"`
	Open    bool    `json:"open"`
	Number  *string `json:"number"`
}

type amount struct {
	Amount int64 `json:"amount"`
}

type withdrawal struct {
	Amount int64   `json:"amount"`
	To     *string `json:"to"`
}

type number struct {
	Number *string `json:"number"`
}

type flag struct {
	Position any `json:"position"`
}

type row struct {
	Balance int64 `json:"balance"`
	Flags   int64 `json:"flags"`
}

type id struct {
	ID string `json:"id"`
}

type (
	ac     = kernable.ApplyContext
	cc     = kernable.CommandContext
	lc     = kernable.ListenerContext
	events = []kernable.NewEvent
	none   = struct{}
)

func main() {
	bank := kernable.NewDomain("bank_go")
	a := kernable.NewAggregate(bank, "account", account{}, kernable.SnapshotEvery(5))
	kernable.Apply(a, "Deposited", func(_ *ac, s *account, d amount) { s.Balance += d.Amount })
	kernable.Apply(a, "Withdrawn", func(_ *ac, s *account, d amount) { s.Balance -= d.Amount })
	kernable.Apply(a, "Opened", func(_ *ac, s *account, _ none) { s.Open = true })
	kernable.Apply(a, "Closed", func(_ *ac, s *account, _ none) { s.Open = false })
	kernable.Apply(a, "Numbered", func(_ *ac, s *account, d number) { s.Number = d.Number })
	a.Unique("number", func(s account) []string {
		if s.Number == nil {
			return nil
		}
		return []string{*s.Number}
	})
	a.Fact("open", func(s account) bool { return s.Open })
	kernable.Handle(a, "Deposit", func(_ *cc, _ account, d amount) (events, error) {
		if d.Amount <= 0 {
			return nil, kernable.Reject("invalid", "amount must be positive")
		}
		return kernable.Events(kernable.Event("Deposited", amount{d.Amount})), nil
	})
	kernable.Handle(a, "Withdraw", func(_ *cc, s account, d withdrawal) (events, error) {
		if s.Balance < d.Amount {
			return nil, kernable.Reject("insufficient", "insufficient funds")
		}
		return kernable.Events(kernable.Event("Withdrawn", withdrawal{d.Amount, d.To})), nil
	}, kernable.Requires(func(d withdrawal, _ account) []kernable.Requirement {
		if d.To == nil || *d.To == "" {
			return nil
		}
		return []kernable.Requirement{{Domain: "bank_go", AggregateType: "account", AggregateID: *d.To, Fact: "open"}}
	}))
	kernable.Handle(a, "Open", func(_ *cc, _ account, _ none) (events, error) {
		return kernable.Events(kernable.Event("Opened", none{})), nil
	})
	kernable.Handle(a, "Close", func(_ *cc, _ account, _ none) (events, error) {
		return kernable.Events(kernable.Event("Closed", none{})), nil
	})
	kernable.Handle(a, "Number", func(_ *cc, _ account, d number) (events, error) {
		return kernable.Events(kernable.Event("Numbered", number{d.Number})), nil
	})
	kernable.Handle(a, "Flag", func(_ *cc, _ account, d flag) (events, error) {
		return kernable.Events(kernable.Event("Flagged", flag{d.Position})), nil
	})

	balances := bank.ReadModel("balances-go", kernable.WithStore("sqlite:"+os.Getenv("SOAK_STORE")), kernable.StoreSchema("1"))
	change := func(c *lc, f func(*row)) error {
		var r row
		if _, err := c.Store.Get("accounts", c.Event.AggregateID, &r); err != nil {
			return err
		}
		f(&r)
		return c.Store.Set("accounts", c.Event.AggregateID, r)
	}
	kernable.On(balances, "Deposited", func(c *lc, d amount) error { return change(c, func(r *row) { r.Balance += d.Amount }) })
	kernable.On(balances, "Withdrawn", func(c *lc, d amount) error { return change(c, func(r *row) { r.Balance -= d.Amount }) })
	kernable.On(balances, "Flagged", func(c *lc, _ none) error { return change(c, func(r *row) { r.Flags++ }) })
	kernable.Answer(balances, "GetAccount", func(c *kernable.QueryContext, d id) (row, error) {
		var r row
		_, err := c.Store.Get("accounts", d.ID, &r)
		return r, err
	})

	flagger := bank.Reactor("flagger-go")
	kernable.On(flagger, "Withdrawn", func(c *lc, d amount) error {
		if d.Amount < 50 {
			return nil
		}
		// A rejection is stored for good under this ID: the reactor parks the event, and B7
		// checks that only such events are parked.
		_, err := c.Submit(kernable.Command{AggregateType: "account", AggregateID: c.Event.AggregateID, Type: "Flag",
			Data: flag{c.Event.Position}, ID: fmt.Sprintf("flag-%d", c.Event.Position), Committed: true})
		return err
	})

	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	app, err := client.App(kernable.AppOptions{Service: "bank-go", Domains: []*kernable.Domain{bank}})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(app.Run(context.Background()))
}
