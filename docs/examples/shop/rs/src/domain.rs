//! The shop's domain: customers and their orders. Used by every process of the shop.
use kernable::process::{Send, Start, Wait, value};
use kernable::{Aggregate, Domain, NewEvent, Process, Rejected, domain};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The domain with its aggregates, as every part of the tutorial leaves them.
pub struct Shop {
    pub domain: Domain,
    pub customer_aggregate: Aggregate<Customer>,
    pub order_aggregate: Aggregate<Order>,
    pub fulfilment: Process,
}

pub fn shop() -> Shop {
    // #region domain
    let shop_domain = domain("shop");
    // #endregion domain
    let customer_aggregate = customer(&shop_domain);
    let order_aggregate = order(&shop_domain);
    confirm(&order_aggregate);
    vault(&shop_domain, &customer_aggregate);
    target(&shop_domain);
    fulfil(&order_aggregate);
    let fulfilment = process(&shop_domain);
    Shop {
        domain: shop_domain,
        customer_aggregate,
        order_aggregate,
        fulfilment,
    }
}

// #region customer
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Customer {
    registered: bool,
    password_hash: Option<String>,
    salt: Option<String>,
}

#[derive(Deserialize)]
struct Register {
    email: Option<String>,
    name: Option<String>,
}

fn customer(shop_domain: &Domain) -> Aggregate<Customer> {
    let customer_aggregate = shop_domain.aggregate("customer", Customer::default());
    customer_aggregate.apply("CustomerRegistered", |s, _: Value, _evt, _ctx| {
        s.registered = true
    });
    customer_aggregate.command("Register", |s, data: Register, _cmd, _ctx| {
        if s.registered {
            return Err(Rejected::new("Customer already registered").code("exists"));
        }
        let email = data.email.unwrap_or_default();
        let valid = email.split_once('@').is_some_and(|(user, host)| {
            !user.is_empty()
                && !host.is_empty()
                && !email.contains(char::is_whitespace)
                && !host.contains('@')
        });
        if !valid {
            return Err(Rejected::new("Invalid email").code("invalid"));
        }
        Ok(vec![NewEvent::new(
            "CustomerRegistered",
            json!({ "email": email, "name": data.name }),
        )])
    });
    customer_aggregate
}
// #endregion customer

// #region order
#[derive(Clone, Serialize, Deserialize)]
pub struct Order {
    /// none, draft, submitted, confirmed, paid, shipped or cancelled.
    status: String,
    items: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Place {
    customer_id: String,
}

#[derive(Serialize, Deserialize)]
struct AddItem {
    sku: String,
    qty: Option<i64>,
}

fn order(shop_domain: &Domain) -> Aggregate<Order> {
    let order_aggregate = shop_domain.aggregate(
        "order",
        Order {
            status: "none".into(),
            items: 0,
        },
    );
    order_aggregate.apply("OrderPlaced", |s, _: Value, _evt, _ctx| {
        *s = Order {
            status: "draft".into(),
            items: 0,
        }
    });
    order_aggregate.apply("ItemAdded", |s, data: AddItem, _evt, _ctx| {
        s.items += data.qty.unwrap_or(0)
    });
    order_aggregate.apply("OrderSubmitted", |s, _: Value, _evt, _ctx| {
        s.status = "submitted".into()
    });
    order_aggregate.apply("OrderConfirmed", |s, _: Value, _evt, _ctx| {
        s.status = "confirmed".into()
    });
    order_aggregate.command("Place", |s, data: Place, _cmd, _ctx| {
        if s.status != "none" {
            return Err(Rejected::new("Order already exists").code("exists"));
        }
        Ok(vec![NewEvent::new("OrderPlaced", data)])
    });
    order_aggregate.command("AddItem", |s, data: AddItem, _cmd, _ctx| {
        if s.status != "draft" {
            let message = format!("Cannot add items to a {} order", s.status);
            return Err(Rejected::new(message).code("invalid_state"));
        }
        if data.qty.is_none_or(|qty| qty <= 0) {
            return Err(Rejected::new("Quantity must be positive").code("invalid"));
        }
        Ok(vec![NewEvent::new("ItemAdded", data)])
    });
    order_aggregate.command("Submit", |s, _: Value, _cmd, _ctx| {
        if s.status != "draft" || s.items == 0 {
            let message = format!("Cannot submit a {} order with {} items", s.status, s.items);
            return Err(Rejected::new(message).code("invalid_state"));
        }
        Ok(vec![NewEvent::new("OrderSubmitted", ())])
    });
    order_aggregate
}
// #endregion order

// #region confirm
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Confirm {
    customer_id: String,
    email: String,
}

fn confirm(order_aggregate: &Aggregate<Order>) {
    order_aggregate.command("Confirm", |s, data: Confirm, _cmd, _ctx| {
        if s.status != "submitted" {
            let message = format!("Cannot confirm a {} order", s.status);
            return Err(Rejected::new(message).code("invalid_state"));
        }
        Ok(vec![NewEvent::new("OrderConfirmed", data)])
    });
}
// #endregion confirm

// #region vault
#[derive(Deserialize)]
struct Password {
    password: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct PasswordSet {
    hash: String,
    salt: String,
}

fn vault(shop_domain: &Domain, customer_aggregate: &Aggregate<Customer>) {
    let aggregate = |_: &Value, id: &str| id.to_owned();
    let customer_id = |d: &Value, _: &str| d["customerId"].as_str().unwrap_or_default().to_owned();
    // Personal data goes to the kernel's vault; the signed log keeps references only.
    shop_domain
        .command("Register")
        .personal(&["email", "name"], aggregate);
    shop_domain
        .event("CustomerRegistered", 1)
        .personal(&["email", "name"], aggregate);
    shop_domain
        .command("Confirm")
        .personal(&["email"], customer_id);
    shop_domain
        .event("OrderConfirmed", 1)
        .personal(&["email"], customer_id);
    // Passwords are secrets: the handler sees them once; the vault deletes them when it is done.
    shop_domain.command("SetPassword").secret(&["password"]);
    shop_domain.command("SignIn").secret(&["password"]);
    shop_domain
        .event("PasswordSet", 1)
        .personal(&["hash", "salt"], aggregate);
    // Erasing a customer deletes their personal data everywhere, in every stream.
    shop_domain.event("CustomerErased", 1).erases(aggregate);

    customer_aggregate.apply("PasswordSet", |s, data: PasswordSet, _evt, _ctx| {
        s.password_hash = Some(data.hash);
        s.salt = Some(data.salt);
    });
    customer_aggregate.apply("CustomerErased", |s, _: Value, _evt, _ctx| {
        *s = Customer::default()
    });
    customer_aggregate.command("SetPassword", |s, data: Password, _cmd, ctx| {
        if !s.registered {
            return Err(Rejected::new("No such customer").code("not_found"));
        }
        let password = data.password.unwrap_or_default();
        if password.chars().count() < 8 {
            return Err(Rejected::new("At least 8 characters").code("invalid"));
        }
        let salt = ctx.id(); // deterministic: the command may run again
        Ok(vec![NewEvent::new(
            "PasswordSet",
            PasswordSet {
                hash: hash(&password, &salt),
                salt,
            },
        )])
    });
    customer_aggregate.command("SignIn", |s, data: Password, _cmd, _ctx| {
        let password = data.password.unwrap_or_default();
        match (&s.password_hash, &s.salt) {
            (Some(stored), Some(salt)) if hash(&password, salt) == *stored => {
                Ok(vec![NewEvent::new("SignedIn", ())])
            }
            _ => Err(Rejected::new("Wrong customer or password").code("denied")),
        }
    });
    customer_aggregate.command("Erase", |s, _: Value, _cmd, _ctx| {
        if !s.registered {
            return Err(Rejected::new("No such customer").code("not_found"));
        }
        Ok(vec![NewEvent::new("CustomerErased", ())])
    });
}

/// scrypt with the parameters of the TypeScript and Python shops, so their hashes match.
fn hash(password: &str, salt: &str) -> String {
    let params = scrypt::Params::new(14, 8, 1, 32).expect("valid scrypt parameters");
    let mut out = [0u8; 32];
    scrypt::scrypt(password.as_bytes(), salt.as_bytes(), &params, &mut out).expect("32 bytes fit");
    out.iter().map(|b| format!("{b:02x}")).collect()
}
// #endregion vault

// #region target
// GetOrder { id } is about the order `id`: the SDK sends that order with the query, signed, and a
// middleware decides on it as on a command's aggregate (part 9).
fn target(shop_domain: &Domain) {
    shop_domain.query("GetOrder").target("order", "id");
}
// #endregion target

// #region fulfil
// Part 10: a confirmed order is paid, then shipped; or cancelled when it is not paid in time.
#[derive(Serialize, Deserialize)]
struct Cancel {
    reason: String,
}

fn fulfil(order_aggregate: &Aggregate<Order>) {
    order_aggregate.apply("OrderPaid", |s, _: Value, _evt, _ctx| {
        s.status = "paid".into()
    });
    order_aggregate.apply("OrderShipped", |s, _: Value, _evt, _ctx| {
        s.status = "shipped".into()
    });
    order_aggregate.apply("OrderCancelled", |s, _: Value, _evt, _ctx| {
        s.status = "cancelled".into()
    });
    order_aggregate.command("Pay", |s, _: Value, _cmd, _ctx| {
        if s.status != "confirmed" {
            let message = format!("Cannot pay a {} order", s.status);
            return Err(Rejected::new(message).code("invalid_state"));
        }
        Ok(vec![NewEvent::new("OrderPaid", ())])
    });
    order_aggregate.command("Ship", |s, _: Value, _cmd, _ctx| {
        if s.status != "paid" {
            let message = format!("Cannot ship a {} order", s.status);
            return Err(Rejected::new(message).code("invalid_state"));
        }
        Ok(vec![NewEvent::new("OrderShipped", ())])
    });
    order_aggregate.command("Cancel", |s, data: Cancel, _cmd, _ctx| {
        if s.status != "confirmed" {
            let message = format!("Cannot cancel a {} order", s.status);
            return Err(Rejected::new(message).code("invalid_state"));
        }
        Ok(vec![NewEvent::new("OrderCancelled", data)])
    });
}
// #endregion fulfil

// #region process
fn process(shop_domain: &Domain) -> Process {
    // How long a customer has to pay; PAY_WITHIN=10s to try it.
    let pay_within = std::env::var("PAY_WITHIN").unwrap_or_else(|_| "3d".into());

    shop_domain
        .process("fulfilment")
        .start_named(
            "OrderConfirmed",
            Start::key("aggregate_id").data([("order_id", "aggregate_id")]),
        )
        .wait_named(
            "paid",
            "OrderPaid",
            Wait::key("aggregate_id").timeout(&pay_within, "cancel"),
        )
        .command_named(
            "ship",
            "order.Ship",
            Send::aggregate("case.order_id").next("shipped"),
        )
        .command_named(
            "cancel",
            "order.Cancel",
            Send::aggregate("case.order_id").data([("reason", value("unpaid"))]),
        )
        .end("unpaid")
        .end("shipped")
}
// #endregion process
