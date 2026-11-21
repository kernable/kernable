// Package shop is the shop's domain: customers and their orders. Imported by every process of the shop.
package shop

import (
	"encoding/hex"
	"fmt"
	"os"
	"regexp"

	kernable "github.com/kernable/kernable/sdks/go"
	"golang.org/x/crypto/scrypt"
)

// #region domain
var Domain = kernable.NewDomain("shop")

// #endregion domain

// #region customer
type Customer struct {
	Registered   bool   `json:"registered"`
	PasswordHash string `json:"passwordHash,omitempty"`
	Salt         string `json:"salt,omitempty"`
}

type Register struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

var email = regexp.MustCompile(`^[^@\s]+@[^@\s]+$`)

var CustomerAggregate = kernable.NewAggregate(Domain, "customer", Customer{})

func init() {
	kernable.Apply(CustomerAggregate, "CustomerRegistered", func(_ *kernable.ApplyContext, s *Customer, _ struct{}) { s.Registered = true })
	kernable.Handle(CustomerAggregate, "Register", func(_ *kernable.CommandContext, s Customer, d Register) ([]kernable.NewEvent, error) {
		if s.Registered {
			return nil, kernable.Reject("exists", "Customer already registered")
		}
		if !email.MatchString(d.Email) {
			return nil, kernable.Reject("invalid", "Invalid email")
		}
		return kernable.Events(kernable.Event("CustomerRegistered", Register{Email: d.Email, Name: d.Name})), nil
	})
}

// #endregion customer

// #region order
type Order struct {
	Status string `json:"status"` // none, draft, submitted, confirmed, paid, shipped, cancelled
	Items  int    `json:"items"`
}

type Place struct {
	CustomerID string `json:"customerId"`
}

type Item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

var OrderAggregate = kernable.NewAggregate(Domain, "order", Order{Status: "none"})

func init() {
	kernable.Apply(OrderAggregate, "OrderPlaced", func(_ *kernable.ApplyContext, s *Order, _ struct{}) { *s = Order{Status: "draft"} })
	kernable.Apply(OrderAggregate, "ItemAdded", func(_ *kernable.ApplyContext, s *Order, d Item) { s.Items += d.Qty })
	kernable.Apply(OrderAggregate, "OrderSubmitted", func(_ *kernable.ApplyContext, s *Order, _ struct{}) { s.Status = "submitted" })
	kernable.Apply(OrderAggregate, "OrderConfirmed", func(_ *kernable.ApplyContext, s *Order, _ struct{}) { s.Status = "confirmed" })
	kernable.Handle(OrderAggregate, "Place", func(_ *kernable.CommandContext, s Order, d Place) ([]kernable.NewEvent, error) {
		if s.Status != "none" {
			return nil, kernable.Reject("exists", "Order already exists")
		}
		return kernable.Events(kernable.Event("OrderPlaced", Place{CustomerID: d.CustomerID})), nil
	})
	kernable.Handle(OrderAggregate, "AddItem", func(_ *kernable.CommandContext, s Order, d Item) ([]kernable.NewEvent, error) {
		if s.Status != "draft" {
			return nil, kernable.Reject("invalid_state", fmt.Sprintf("Cannot add items to a %s order", s.Status))
		}
		if d.Qty <= 0 {
			return nil, kernable.Reject("invalid", "Quantity must be positive")
		}
		return kernable.Events(kernable.Event("ItemAdded", Item{SKU: d.SKU, Qty: d.Qty})), nil
	})
	kernable.Handle(OrderAggregate, "Submit", func(_ *kernable.CommandContext, s Order, _ struct{}) ([]kernable.NewEvent, error) {
		if s.Status != "draft" || s.Items == 0 {
			return nil, kernable.Reject("invalid_state", fmt.Sprintf("Cannot submit a %s order with %d items", s.Status, s.Items))
		}
		return kernable.Events(kernable.Event("OrderSubmitted", nil)), nil
	})
}

// #endregion order

// #region confirm
type Confirm struct {
	CustomerID string `json:"customerId"`
	Email      string `json:"email"`
}

func init() {
	kernable.Handle(OrderAggregate, "Confirm", func(_ *kernable.CommandContext, s Order, d Confirm) ([]kernable.NewEvent, error) {
		if s.Status != "submitted" {
			return nil, kernable.Reject("invalid_state", fmt.Sprintf("Cannot confirm a %s order", s.Status))
		}
		return kernable.Events(kernable.Event("OrderConfirmed", Confirm{CustomerID: d.CustomerID, Email: d.Email})), nil
	})
}

// #endregion confirm

// #region vault
// Personal data goes to the kernel's vault; the signed log keeps references only.
func init() {
	self := func(_ map[string]any, id string) string { return id }
	customerOf := func(d map[string]any, _ string) string { s, _ := d["customerId"].(string); return s }
	Domain.Command("Register").Personal([]string{"email", "name"}, self)
	Domain.Event("CustomerRegistered", 1).Personal([]string{"email", "name"}, self)
	Domain.Command("Confirm").Personal([]string{"email"}, customerOf)
	Domain.Event("OrderConfirmed", 1).Personal([]string{"email"}, customerOf)
	// Passwords are secrets: the handler sees them once; the vault deletes them when it is done.
	Domain.Command("SetPassword").Secret("password")
	Domain.Command("SignIn").Secret("password")
	Domain.Event("PasswordSet", 1).Personal([]string{"hash", "salt"}, self)
	// Erasing a customer deletes their personal data everywhere, in every stream.
	Domain.Event("CustomerErased", 1).Erases(self)

	kernable.Apply(CustomerAggregate, "PasswordSet", func(_ *kernable.ApplyContext, s *Customer, d PasswordSet) { s.PasswordHash, s.Salt = d.Hash, d.Salt })
	kernable.Apply(CustomerAggregate, "CustomerErased", func(_ *kernable.ApplyContext, s *Customer, _ struct{}) { *s = Customer{} })
	kernable.Handle(CustomerAggregate, "SetPassword", func(ctx *kernable.CommandContext, s Customer, d Password) ([]kernable.NewEvent, error) {
		if !s.Registered {
			return nil, kernable.Reject("not_found", "No such customer")
		}
		if len([]rune(d.Password)) < 8 {
			return nil, kernable.Reject("invalid", "At least 8 characters")
		}
		salt := ctx.ID() // deterministic: the command may run again
		return kernable.Events(kernable.Event("PasswordSet", PasswordSet{Hash: hash(d.Password, salt), Salt: salt})), nil
	})
	kernable.Handle(CustomerAggregate, "SignIn", func(_ *kernable.CommandContext, s Customer, d Password) ([]kernable.NewEvent, error) {
		if s.PasswordHash == "" || hash(d.Password, s.Salt) != s.PasswordHash {
			return nil, kernable.Reject("denied", "Wrong customer or password")
		}
		return kernable.Events(kernable.Event("SignedIn", nil)), nil
	})
	kernable.Handle(CustomerAggregate, "Erase", func(_ *kernable.CommandContext, s Customer, _ struct{}) ([]kernable.NewEvent, error) {
		if !s.Registered {
			return nil, kernable.Reject("not_found", "No such customer")
		}
		return kernable.Events(kernable.Event("CustomerErased", nil)), nil
	})
}

type Password struct {
	Password string `json:"password"`
}

type PasswordSet struct {
	Hash string `json:"hash"`
	Salt string `json:"salt"`
}

func hash(password, salt string) string {
	h, _ := scrypt.Key([]byte(password), []byte(salt), 16384, 8, 1, 32)
	return hex.EncodeToString(h)
}

// #endregion vault

// #region target
// GetOrder { id } is about the order id: the SDK sends that order with the query, signed, and a
// middleware decides on it as on a command's aggregate (part 9).
func init() { Domain.Query("GetOrder").Target("order", "id") }

// #endregion target

// #region fulfil
// Part 10: a confirmed order is paid, then shipped; or cancelled when it is not paid in time.
type Cancel struct {
	Reason string `json:"reason"`
}

func init() {
	kernable.Apply(OrderAggregate, "OrderPaid", func(_ *kernable.ApplyContext, s *Order, _ struct{}) { s.Status = "paid" })
	kernable.Apply(OrderAggregate, "OrderShipped", func(_ *kernable.ApplyContext, s *Order, _ struct{}) { s.Status = "shipped" })
	kernable.Apply(OrderAggregate, "OrderCancelled", func(_ *kernable.ApplyContext, s *Order, _ struct{}) { s.Status = "cancelled" })
	kernable.Handle(OrderAggregate, "Pay", func(_ *kernable.CommandContext, s Order, _ struct{}) ([]kernable.NewEvent, error) {
		if s.Status != "confirmed" {
			return nil, kernable.Reject("invalid_state", fmt.Sprintf("Cannot pay a %s order", s.Status))
		}
		return kernable.Events(kernable.Event("OrderPaid", nil)), nil
	})
	kernable.Handle(OrderAggregate, "Ship", func(_ *kernable.CommandContext, s Order, _ struct{}) ([]kernable.NewEvent, error) {
		if s.Status != "paid" {
			return nil, kernable.Reject("invalid_state", fmt.Sprintf("Cannot ship a %s order", s.Status))
		}
		return kernable.Events(kernable.Event("OrderShipped", nil)), nil
	})
	kernable.Handle(OrderAggregate, "Cancel", func(_ *kernable.CommandContext, s Order, d Cancel) ([]kernable.NewEvent, error) {
		if s.Status != "confirmed" {
			return nil, kernable.Reject("invalid_state", fmt.Sprintf("Cannot cancel a %s order", s.Status))
		}
		return kernable.Events(kernable.Event("OrderCancelled", Cancel{Reason: d.Reason})), nil
	})
}

// #endregion fulfil

// #region process
// How long a customer has to pay; PAY_WITHIN=10s to try it.
var payWithin = cmp(os.Getenv("PAY_WITHIN"), "3d")

var Fulfilment = Domain.Process("fulfilment").
	Start("OrderConfirmed", kernable.StartOptions{Key: "aggregateId", Data: kernable.Mapping{"orderId": "aggregateId"}}).
	Wait("paid", "OrderPaid", kernable.WaitStep{Key: "aggregateId", Timeout: payWithin, OnTimeout: "cancel"}).
	Command("ship", "order.Ship", kernable.CommandStep{Aggregate: "case.orderId", Next: "shipped"}).
	Command("cancel", "order.Cancel", kernable.CommandStep{Aggregate: "case.orderId", Data: kernable.Mapping{"reason": kernable.Value("unpaid")}}).
	End("unpaid").
	End("shipped")

// #endregion process

func cmp(v, otherwise string) string {
	if v != "" {
		return v
	}
	return otherwise
}
