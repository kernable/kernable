// The shop's domain: customers and their orders. Imported by every process of the shop.
import { scryptSync } from "node:crypto";

import { Rejected, domain } from "@kernable/sdk";

// #region domain
export const shop = domain("shop");
// #endregion domain

// #region customer
type Customer = { registered: boolean; passwordHash?: string; salt?: string };

export const customer = shop.aggregate<Customer>("Customer", { registered: false })
  .apply("CustomerRegistered", (s) => ({ ...s, registered: true }))
  .command("Register", (s, cmd: { email: string; name: string }) => {
    if (s.registered) throw new Rejected("Customer already registered", "exists");
    if (!/^[^@\s]+@[^@\s]+$/.test(cmd.email ?? "")) throw new Rejected("Invalid email", "invalid");
    return [{ type: "CustomerRegistered", data: { email: cmd.email, name: cmd.name } }];
  });
// #endregion customer

// #region order
type Order = { status: "none" | "draft" | "submitted" | "confirmed"; items: number };

export const order = shop.aggregate<Order>("Order", { status: "none", items: 0 })
  .apply("OrderPlaced", () => ({ status: "draft", items: 0 }))
  .apply("ItemAdded", (s, e: { qty: number }) => ({ ...s, items: s.items + e.qty }))
  .apply("OrderSubmitted", (s) => ({ ...s, status: "submitted" }))
  .apply("OrderConfirmed", (s) => ({ ...s, status: "confirmed" }))
  .command("Place", (s, cmd: { customerId: string }) => {
    if (s.status !== "none") throw new Rejected("Order already exists", "exists");
    return [{ type: "OrderPlaced", data: { customerId: cmd.customerId } }];
  })
  .command("AddItem", (s, cmd: { sku: string; qty: number }) => {
    if (s.status !== "draft") throw new Rejected(`Cannot add items to a ${s.status} order`, "invalid_state");
    if (!(cmd.qty > 0)) throw new Rejected("Quantity must be positive", "invalid");
    return [{ type: "ItemAdded", data: { sku: cmd.sku, qty: cmd.qty } }];
  })
  .command("Submit", (s) => {
    if (s.status !== "draft" || s.items === 0) throw new Rejected(`Cannot submit a ${s.status} order with ${s.items} items`, "invalid_state");
    return [{ type: "OrderSubmitted" }];
  });
// #endregion order

// #region confirm
order.command("Confirm", (s, cmd: { customerId: string; email: string }) => {
  if (s.status !== "submitted") throw new Rejected(`Cannot confirm a ${s.status} order`, "invalid_state");
  return [{ type: "OrderConfirmed", data: { customerId: cmd.customerId, email: cmd.email } }];
});
// #endregion confirm

// #region vault
// Personal data goes to the kernel's vault; the signed log keeps references only.
shop.command("Register").personal(["email", "name"], (_d, id) => id);
shop.event("CustomerRegistered", 1).personal(["email", "name"], (_d, id) => id);
shop.command("Confirm").personal(["email"], (d: { customerId: string }) => d.customerId);
shop.event("OrderConfirmed", 1).personal(["email"], (d: { customerId: string }) => d.customerId);
// Passwords are secrets: the handler sees them once; the vault deletes them when it is done.
shop.command("SetPassword").secret(["password"]);
shop.command("SignIn").secret(["password"]);
shop.event("PasswordSet", 1).personal(["hash", "salt"], (_d, id) => id);
// Erasing a customer deletes their personal data everywhere, in every stream.
shop.event("CustomerErased", 1).erases((_d, id) => id);

const hash = (password: string, salt: string) => scryptSync(password, salt, 32).toString("hex");

customer
  .apply("PasswordSet", (s, e: { hash: string; salt: string }) => ({ ...s, passwordHash: e.hash, salt: e.salt }))
  .apply("CustomerErased", () => ({ registered: false }))
  .command("SetPassword", (s, cmd: { password: string }, ctx) => {
    if (!s.registered) throw new Rejected("No such customer", "not_found");
    if ((cmd.password ?? "").length < 8) throw new Rejected("At least 8 characters", "invalid");
    const salt = ctx.id(); // deterministic: the command may run again
    return [{ type: "PasswordSet", data: { hash: hash(cmd.password, salt), salt } }];
  })
  .command("SignIn", (s, cmd: { password: string }) => {
    if (!s.passwordHash || hash(cmd.password ?? "", s.salt!) !== s.passwordHash) throw new Rejected("Wrong customer or password", "denied");
    return [{ type: "SignedIn" }];
  })
  .command("Erase", (s) => {
    if (!s.registered) throw new Rejected("No such customer", "not_found");
    return [{ type: "CustomerErased" }];
  });
// #endregion vault
