// The worker: runs the shop's commands.
package main

import (
	"context"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

// #region worker
func main() {
	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(client.Worker(shop.CustomerAggregate, shop.OrderAggregate).Run(context.Background()))
}

// #endregion worker
