// Part 7: personal data in the vault, passwords as secrets, erasure.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

func main() {
	ctx := context.Background()
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
	// Use(shop.Domain): the client moves the fields the domain declares personal or secret to the vault.
	client, err := kernable.New(kernable.Options{Context: kernable.RequestContext{SenderIdentity: "c1"}})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	client.Use(shop.Domain)
	type m = map[string]any
	customer := func(commandType string, data any) (kernable.Outcome, error) {
		return client.Submit(ctx, kernable.Command{Domain: "shop", AggregateType: "customer", AggregateID: "c1", Type: commandType, Data: data})
	}
	read := func() (string, error) {
		answer, err := client.Query(ctx, kernable.Query{Domain: "shop", Type: "GetCustomer", Data: m{"id": "c1"}})
		return string(answer), err
	}

	show("register")(customer("Register", m{"email": "ann@example.com", "name": "Ann"}))
	show("set password")(customer("SetPassword", m{"password": "correct horse"}))
	show("sign in, wrong password")(customer("SignIn", m{"password": "wrong horse"}))
	show("sign in")(customer("SignIn", m{"password": "correct horse"}))
	row, err := read()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(row)
	show("erase")(customer("Erase", nil))
	// Erased: the customer's data is gone, and with it the right to act.
	var rejected *kernable.Rejected
	if _, err := read(); errors.As(err, &rejected) {
		fmt.Printf("read: %s\n", rejected.Code)
	}
	// #endregion try
}
