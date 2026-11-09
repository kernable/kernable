// The shop's domain: customers and their orders. Imported by every process of the shop.
import { scryptSync } from "node:crypto";

import { Rejected, domain } from "@kernable/sdk";

// #region domain
export const shopDomain = domain("shop");
// #endregion domain

// #region customer
type Customer = { registered: boolean; passwordHash?: string; salt?: string };

export const customerAggregate = shopDomain.aggregate<Customer>("customer", { registered: false })
  .apply("CustomerRegistered", (s) => { s.registered = true; })
  .command("Register", (s, data: { email: string; name: string }) => {
    if (s.registered) throw new Rejected("Customer already registered", "exists");
    if (!/^[^@\s]+@[^@\s]+$/.test(data.email ?? "")) throw new Rejected("Invalid email", "invalid");
    return [{ type: "CustomerRegistered", data: { email: data.email, name: data.name } }];
  });
// #endregion customer

// #region order
type Order = { status: "none" | "draft" | "submitted" | "confirmed"; items: number };

export const orderAggregate = shopDomain.aggregate<Order>("order", { status: "none", items: 0 })
  .apply("OrderPlaced", () => ({ status: "draft", items: 0 }))
  .apply("ItemAdded", (s, data: { qty: number }) => { s.items += data.qty; })
  .apply("OrderSubmitted", (s) => { s.status = "submitted"; })
  .apply("OrderConfirmed", (s) => { s.status = "confirmed"; })
  .command("Place", (s, data: { customerId: string }) => {
    if (s.status !== "none") throw new Rejected("Order already exists", "exists");
    return [{ type: "OrderPlaced", data: { customerId: data.customerId } }];
  })
  .command("AddItem", (s, data: { sku: string; qty: number }) => {
    if (s.status !== "draft") throw new Rejected(`Cannot add items to a ${s.status} order`, "invalid_state");
    if (!(data.qty > 0)) throw new Rejected("Quantity must be positive", "invalid");
    return [{ type: "ItemAdded", data: { sku: data.sku, qty: data.qty } }];
  })
  .command("Submit", (s) => {
    if (s.status !== "draft" || s.items === 0) throw new Rejected(`Cannot submit a ${s.status} order with ${s.items} items`, "invalid_state");
    return [{ type: "OrderSubmitted" }];
  });
// #endregion order

// #region confirm
orderAggregate.command("Confirm", (s, data: { customerId: string; email: string }) => {
  if (s.status !== "submitted") throw new Rejected(`Cannot confirm a ${s.status} order`, "invalid_state");
  return [{ type: "OrderConfirmed", data: { customerId: data.customerId, email: data.email } }];
});
// #endregion confirm

// #region vault
// Personal data goes to the kernel's vault; the signed log keeps references only.
shopDomain.command("Register").personal(["email", "name"], (_d, id) => id);
shopDomain.event("CustomerRegistered", 1).personal(["email", "name"], (_d, id) => id);
shopDomain.command("Confirm").personal(["email"], (d: { customerId: string }) => d.customerId);
shopDomain.event("OrderConfirmed", 1).personal(["email"], (d: { customerId: string }) => d.customerId);
// Passwords are secrets: the handler sees them once; the vault deletes them when it is done.
shopDomain.command("SetPassword").secret(["password"]);
shopDomain.command("SignIn").secret(["password"]);
shopDomain.event("PasswordSet", 1).personal(["hash", "salt"], (_d, id) => id);
// Erasing a customer deletes their personal data everywhere, in every stream.
shopDomain.event("CustomerErased", 1).erases((_d, id) => id);

const hash = (password: string, salt: string) => scryptSync(password, salt, 32).toString("hex");

customerAggregate
  .apply("PasswordSet", (s, data: { hash: string; salt: string }) => { s.passwordHash = data.hash; s.salt = data.salt; })
  .apply("CustomerErased", (s) => { delete s.passwordHash; delete s.salt; s.registered = false; })
  .command("SetPassword", (s, data: { password: string }, _cmd, ctx) => {
    if (!s.registered) throw new Rejected("No such customer", "not_found");
    if ((data.password ?? "").length < 8) throw new Rejected("At least 8 characters", "invalid");
    const salt = ctx.id(); // deterministic: the command may run again
    return [{ type: "PasswordSet", data: { hash: hash(data.password, salt), salt } }];
  })
  .command("SignIn", (s, data: { password: string }) => {
    if (!s.passwordHash || hash(data.password ?? "", s.salt!) !== s.passwordHash) throw new Rejected("Wrong customer or password", "denied");
    return [{ type: "SignedIn" }];
  })
  .command("Erase", (s) => {
    if (!s.registered) throw new Rejected("No such customer", "not_found");
    return [{ type: "CustomerErased" }];
  });
// #endregion vault

// #region target
// GetOrder { id } is about the order `id`: the SDK sends that order with the query, signed, and a
// middleware decides on it as on a command's aggregate (part 9).
shopDomain.query("GetOrder").target("order", "id");
// #endregion target
