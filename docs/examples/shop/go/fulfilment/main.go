// Registers the fulfilment process with the kernel: once per deploy, after the worker is up.
package main

import (
	"context"
	"fmt"
	"log"

	kernable "github.com/kernable/kernable/sdks/go"
	"shop"
)

// #region register
func main() {
	client, err := kernable.New(kernable.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	v, err := client.RegisterProcess(context.Background(), shop.Fulfilment)
	if err != nil {
		log.Fatal(err)
	}
	unchanged := ""
	if !v.New {
		unchanged = " (unchanged)"
	}
	fmt.Printf("%s version %d%s\n", v.Name, v.Version, unchanged)
}

// #endregion register
