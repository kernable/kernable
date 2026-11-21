# kernable SDK for Go

Write event-sourced business logic in Go; the kernel does the rest. The same protocol, the
same signatures and the same behaviour as the TypeScript, Python and Rust SDKs: a Go worker,
read model or reactor works next to processes in any of them.

```sh
go get github.com/kernable/kernable/sdks/go
```

Go 1.26 or newer. No protoc needed: the generated protocol code is part of the module.

```go
import kernable "github.com/kernable/kernable/sdks/go"

shop := kernable.NewDomain("shop")
cart := kernable.NewAggregate(shop, "cart", Cart{})
kernable.Apply(cart, "ItemAdded", func(ctx *kernable.ApplyContext, s *Cart, d ItemAdded) { s.Items += d.Qty })
kernable.Handle(cart, "AddItem", func(ctx *kernable.CommandContext, s Cart, d AddItem) ([]kernable.NewEvent, error) {
	if s.CheckedOut {
		return nil, kernable.Reject("checked_out", "Cart is checked out")
	}
	return kernable.Events(kernable.Event("ItemAdded", ItemAdded{SKU: d.SKU, Qty: d.Qty})), nil
})

client, err := kernable.New(kernable.Options{}) // KERNABLE_URL, KERNABLE_KEY, … or app.key
app, err := client.App(kernable.AppOptions{Service: "shop", Domains: []*kernable.Domain{shop}})
err = app.Run(ctx) // until ctx ends
```

Every call that can block takes a `context.Context` first; handlers get a kernable context first
(`*CommandContext`, `*ListenerContext`, `*QueryContext`, `*MiddlewareContext`), each a
`context.Context`. Business rejections are values (`*kernable.Rejected`); `Submit` returns them
in its `Outcome`, `Query` as an error.

Documentation: https://kernable.com/docs (the tutorial and the reference show Go next to the
other languages).

## Develop

```sh
./gen.sh            # after a change of proto/kernable/v1/kernable.proto (needs protoc)
go test ./...       # signing vectors; with target/debug/kernable built (cargo build), against a kernel
```

MIT licensed, see [LICENSE](../../LICENSE).
