//! The demo's bank, written with the Rust SDK as any application would be. Lines ending in
//! `// stage` only record for the Stage what happened when, and what state a handler saw.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::Instant,
};

use kernable::{
    BoxError, Domain, Kernable, MiddlewareOptions, NewEvent, Outcome, Query, Rejected,
    RequestContext, Requirement, Submit, SubscribeOptions, Unique, domain,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::Recorder;

/// The read models' rows, by aggregate ID; the Stage shows them as they are.
#[derive(Clone, Default)]
pub(super) struct Rows {
    pub accounts: Arc<Mutex<BTreeMap<String, Row>>>,
    pub customers: Arc<Mutex<BTreeMap<String, CustomerRow>>>,
    pub people: Arc<Mutex<BTreeMap<String, PersonRow>>>,
}

impl Rows {
    /// The rows of read model `name`, as JSON.
    pub(super) fn view(&self, name: &str) -> Value {
        match name {
            "customers" => json!(*lock(&self.customers)),
            "people" => json!(*lock(&self.people)),
            _ => json!(*lock(&self.accounts)),
        }
    }
}

/// Which parts of the bank a scene's app runs.
#[derive(Clone, Copy, Default)]
pub(super) struct Parts {
    pub accounts: bool,
    pub customers: bool,
    pub people: bool,
    /// The `payments` reactor, which credits what an account pays.
    pub payments: bool,
    /// The `rbac` middleware, which lets only tellers withdraw.
    pub rbac: bool,
}

/// What the Stage may switch while a scene runs: the payments reactor's failure.
#[derive(Clone, Default)]
pub(super) struct Switches {
    pub payments_broken: Arc<AtomicBool>,
}

/// The rows, also after a handler panicked while holding them.
fn lock<T>(rows: &Mutex<T>) -> MutexGuard<'_, T> {
    rows.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Account {
    owner: String,
    balance: i64,
    open: bool,
}

#[derive(Serialize, Deserialize)]
struct Payment {
    to: String,
    amount: i64,
}

#[derive(Serialize, Deserialize)]
struct Opened {
    owner: String,
}

#[derive(Serialize, Deserialize)]
struct Amount {
    amount: i64,
}

#[derive(Clone, Serialize)]
pub(super) struct Row {
    owner: String,
    balance: i64,
    open: bool,
}

/// The `bank` domain with the parts a scene needs.
pub(super) fn bank(parts: Parts, stage: &Recorder, rows: &Rows, switches: &Switches) -> Domain {
    let bank_domain = domain("bank");
    if parts.accounts {
        accounts(&bank_domain, stage, rows);
    }
    if parts.customers {
        customers(&bank_domain, stage, rows);
    }
    if parts.people {
        people(&bank_domain, stage, rows);
    }
    if parts.payments {
        payments(&bank_domain, switches);
    }
    if parts.rbac {
        rbac(&bank_domain);
    }
    bank_domain
}

/// The `account` aggregate and the `accounts` read model.
fn accounts(bank_domain: &Domain, stage: &Recorder, rows: &Rows) {
    let account = bank_domain.aggregate("account", Account::default());

    account.apply("AccountOpened", |state, data: Opened, _evt, _ctx| {
        state.owner = data.owner;
        state.open = true;
    });
    account.apply("Deposited", |state, data: Amount, _evt, _ctx| {
        state.balance += data.amount
    });
    account.apply("Withdrawn", |state, data: Amount, _evt, _ctx| {
        state.balance -= data.amount
    });
    account.apply("Paid", |state, data: Payment, _evt, _ctx| {
        state.balance -= data.amount
    });
    account.apply("Credited", |state, data: Amount, _evt, _ctx| {
        state.balance += data.amount
    });
    account.apply("AccountClosed", |state, _: Value, _evt, _ctx| {
        state.open = false
    });

    // Other aggregates' commands can require it; the kernel checks it when they commit.
    account.fact("open", |state| state.open);

    let c = stage.clone(); // stage
    account.command("OpenAccount", move |state, data: Opened, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if state.open {
            return Err(Rejected::new("the account is already open"));
        }
        Ok(vec![NewEvent::new("AccountOpened", data)])
    });

    let c = stage.clone(); // stage
    account.command("Deposit", move |state, data: Amount, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if !state.open {
            return Err(Rejected::new("no such account").code("not_found"));
        }
        if data.amount <= 0 {
            return Err(Rejected::new("deposit a positive amount"));
        }
        Ok(vec![NewEvent::new("Deposited", data)])
    });

    let c = stage.clone(); // stage
    account.command("Withdraw", move |state, data: Amount, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if data.amount > state.balance {
            let message = format!("insufficient funds: the balance is {}", state.balance);
            return Err(Rejected::new(message).code("insufficient_funds"));
        }
        Ok(vec![NewEvent::new("Withdrawn", data)])
    });

    let c = stage.clone(); // stage
    account.command("Close", move |state, _: Value, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if !state.open {
            return Err(Rejected::new("the account is not open"));
        }
        Ok(vec![NewEvent::new("AccountClosed", json!({}))])
    });

    let c = stage.clone(); // stage
    account.command("Pay", move |state, data: Payment, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if data.amount > state.balance {
            let message = format!("insufficient funds: the balance is {}", state.balance);
            return Err(Rejected::new(message).code("insufficient_funds"));
        }
        Ok(vec![NewEvent::new("Paid", data)])
    });
    // The receiving account must be open when the payment commits: the kernel checks it.
    account.requires("Pay", |_state, data: Payment| {
        [Requirement::new("bank", "account", &data.to, "open")]
    });

    let c = stage.clone(); // stage
    account.command("Credit", move |state, data: Amount, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if !state.open {
            return Err(Rejected::new("the account is closed").code("closed"));
        }
        Ok(vec![NewEvent::new("Credited", data)])
    });

    let accounts_readmodel = bank_domain.read_model("accounts");

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    accounts_readmodel.on("AccountOpened", move |data: Opened, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        let row = Row {
            owner: data.owner,
            balance: 0,
            open: true,
        };
        lock(&r).insert(evt.aggregate_id, row);
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    accounts_readmodel.on("Deposited", move |data: Amount, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance += data.amount;
        }
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    accounts_readmodel.on("Withdrawn", move |data: Amount, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance -= data.amount;
        }
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    accounts_readmodel.on("Paid", move |data: Payment, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance -= data.amount;
        }
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    accounts_readmodel.on("Credited", move |data: Amount, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance += data.amount;
        }
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    accounts_readmodel.on("AccountClosed", move |_: Value, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.open = false;
        }
        async { Ok(()) }
    });

    #[derive(Deserialize)]
    struct GetAccount {
        id: String,
    }

    let r = Arc::clone(&rows.accounts);
    accounts_readmodel.query("GetAccount", move |data: GetAccount, _qry, _ctx| {
        let row = lock(&r).get(&data.id).cloned();
        async move { Ok(row.ok_or_else(|| Rejected::new("no such account").code("not_found"))?) }
    });
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Customer {
    name: String,
    username: Option<String>,
    registered: bool,
}

#[derive(Serialize, Deserialize)]
struct Registered {
    name: String,
    username: String,
}

#[derive(Serialize, Deserialize)]
struct Username {
    username: String,
}

#[derive(Clone, Serialize)]
pub(super) struct CustomerRow {
    name: String,
    username: String,
}

/// The `customer` aggregate, whose username is unique, and the `customers` read model.
fn customers(bank_domain: &Domain, stage: &Recorder, rows: &Rows) {
    let customer = bank_domain.aggregate("customer", Customer::default());

    // One customer per username in the tenant, whatever its case. The kernel checks it when it
    // commits; it gets a hash of the value, never the value.
    customer.unique(
        "username",
        |state| state.username.as_ref().map(|u| u.trim().to_lowercase()),
        Unique::tenant(),
    );

    customer.apply(
        "CustomerRegistered",
        |state, data: Registered, _evt, _ctx| {
            state.name = data.name;
            state.username = Some(data.username);
            state.registered = true;
        },
    );
    customer.apply("UsernameChanged", |state, data: Username, _evt, _ctx| {
        state.username = Some(data.username)
    });

    let c = stage.clone(); // stage
    customer.command("Register", move |state, data: Registered, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if state.registered {
            return Err(Rejected::new("this customer is registered already"));
        }
        // No lookup of the username here: the kernel refuses it at commit if it is taken.
        Ok(vec![NewEvent::new("CustomerRegistered", data)])
    });

    let c = stage.clone(); // stage
    customer.command("ChangeUsername", move |state, data: Username, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if !state.registered {
            return Err(Rejected::new("no such customer").code("not_found"));
        }
        // The old username is free once this commits.
        Ok(vec![NewEvent::new("UsernameChanged", data)])
    });

    let customers_readmodel = bank_domain.read_model("customers");

    let (r, c) = (Arc::clone(&rows.customers), stage.clone());
    customers_readmodel.on("CustomerRegistered", move |data: Registered, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        let row = CustomerRow {
            name: data.name,
            username: data.username,
        };
        lock(&r).insert(evt.aggregate_id, row);
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.customers), stage.clone());
    customers_readmodel.on("UsernameChanged", move |data: Username, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.username = data.username;
        }
        async { Ok(()) }
    });

    #[derive(Deserialize)]
    struct GetCustomer {
        id: String,
    }

    let r = Arc::clone(&rows.customers);
    customers_readmodel.query("GetCustomer", move |data: GetCustomer, _qry, _ctx| {
        let row = lock(&r).get(&data.id).cloned();
        async move { Ok(row.ok_or_else(|| Rejected::new("no such customer").code("not_found"))?) }
    });
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Person {
    onboarded: bool,
    forgotten: bool,
}

#[derive(Serialize, Deserialize)]
struct Onboarded {
    name: Option<String>,
    email: Option<String>,
}

#[derive(Clone, Serialize)]
pub(super) struct PersonRow {
    name: Option<String>,
    email: Option<String>,
}

/// The `person` aggregate, whose name and email are personal data in the vault, and the
/// `people` read model.
fn people(bank_domain: &Domain, stage: &Recorder, rows: &Rows) {
    // Name and email go to the vault, encrypted per person; commands and events keep only
    // references. Forgetting a person erases them everywhere at once.
    bank_domain
        .command("Onboard")
        .personal(&["name", "email"], |_, id| id.to_owned());
    bank_domain
        .event("PersonOnboarded", 1)
        .personal(&["name", "email"], |_, id| id.to_owned());
    bank_domain
        .event("PersonForgotten", 1)
        .erases(|_, id| id.to_owned());

    let person = bank_domain.aggregate("person", Person::default());
    person.apply("PersonOnboarded", |state, _: Value, _evt, _ctx| {
        state.onboarded = true
    });
    person.apply("PersonForgotten", |state, _: Value, _evt, _ctx| {
        state.forgotten = true
    });

    let c = stage.clone(); // stage
    person.command("Onboard", move |state, data: Onboarded, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if state.onboarded {
            return Err(Rejected::new("this person is onboarded already"));
        }
        // The handler gets the values in clear; the kernel stores them in the vault.
        Ok(vec![NewEvent::new("PersonOnboarded", data)])
    });

    let c = stage.clone(); // stage
    person.command("Forget", move |state, _: Value, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if !state.onboarded || state.forgotten {
            return Err(Rejected::new("no such person").code("not_found"));
        }
        Ok(vec![NewEvent::new("PersonForgotten", json!({}))])
    });

    let people_readmodel = bank_domain.read_model("people");

    let (r, c) = (Arc::clone(&rows.people), stage.clone());
    people_readmodel.on("PersonOnboarded", move |data: Onboarded, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        let row = PersonRow {
            name: data.name,
            email: data.email,
        };
        lock(&r).insert(evt.aggregate_id, row);
        async { Ok(()) }
    });

    let (r, c) = (Arc::clone(&rows.people), stage.clone());
    people_readmodel.on("PersonForgotten", move |_: Value, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        // Drop what the read model holds of the person; the vault forgot the rest.
        lock(&r).remove(&evt.aggregate_id);
        async { Ok(()) }
    });

    #[derive(Deserialize)]
    struct GetPerson {
        id: String,
    }

    let r = Arc::clone(&rows.people);
    people_readmodel.query("GetPerson", move |data: GetPerson, _qry, _ctx| {
        let row = lock(&r).get(&data.id).cloned();
        async move { Ok(row.ok_or_else(|| Rejected::new("no such person").code("not_found"))?) }
    });
}

/// The `payments` reactor: it credits the receiving account of every payment. The Stage can
/// break it, to show an event it cannot handle parked.
fn payments(bank_domain: &Domain, switches: &Switches) {
    let broken = Arc::clone(&switches.payments_broken); // stage
    let payments_reactor = bank_domain.reactor("payments").park_after(1);
    payments_reactor.on("Paid", move |data: Payment, evt, ctx| {
        let broken = broken.load(SeqCst); // stage
        async move {
            if broken {
                return Err("the payments service is down".into()); // stage
            }
            // A fixed ID: if the event comes again, the credit still happens once.
            let credit = Submit::new("bank", "account", &data.to, "Credit")
                .id(format!("credit-{}", evt.position))
                .data(json!({ "amount": data.amount }));
            ctx.submit(credit).await?;
            Ok(())
        }
    });
}

/// The `rbac` middleware: only tellers may withdraw.
fn rbac(bank_domain: &Domain) {
    let rbac = bank_domain.read_model("rbac");
    rbac.middleware(MiddlewareOptions::new(&["Withdraw"]), |check| async move {
        // Before the command is queued: a refused one is never stored.
        if check.request.sender_identity != "teller" {
            let message = format!("{} may not withdraw", check.request.sender_identity);
            return Err(Rejected::new(message).code("forbidden").into());
        }
        Ok(())
    });
}

/// The accounts read model built again from every event, by a subscription from position 0,
/// until it has seen `head`; with an answer to a question the live one never asked: how much
/// each account was paid in, in all.
pub(super) async fn rebuild(
    kernable: &Kernable,
    head: u64,
) -> Result<(BTreeMap<String, Row>, BTreeMap<String, i64>), BoxError> {
    let rows: Arc<Mutex<BTreeMap<String, Row>>> = Arc::default();
    let paid_in: Arc<Mutex<BTreeMap<String, i64>>> = Arc::default();
    let (r, p) = (Arc::clone(&rows), Arc::clone(&paid_in));
    let from_the_start = SubscribeOptions {
        after: 0,
        types: vec!["bank.*".into()],
        ..Default::default()
    };
    let subscription = kernable.subscribe(from_the_start, move |e| {
        let (r, p) = (Arc::clone(&r), Arc::clone(&p));
        async move {
            let (mut rows, id) = (lock(&r), e.aggregate_id.clone());
            match e.event_type.as_str() {
                "AccountOpened" => {
                    let owner = e.parse::<Opened>()?.owner;
                    let row = Row {
                        owner,
                        balance: 0,
                        open: true,
                    };
                    rows.insert(id, row);
                }
                "Deposited" | "Credited" => {
                    let amount = e.parse::<Amount>()?.amount;
                    rows.entry(id.clone())
                        .and_modify(|row| row.balance += amount);
                    *lock(&p).entry(id).or_default() += amount;
                }
                "Withdrawn" => {
                    let amount = e.parse::<Amount>()?.amount;
                    rows.entry(id).and_modify(|row| row.balance -= amount);
                }
                "Paid" => {
                    let amount = e.parse::<Payment>()?.amount;
                    rows.entry(id).and_modify(|row| row.balance -= amount);
                }
                "AccountClosed" => {
                    rows.entry(id).and_modify(|row| row.open = false);
                }
                _ => {}
            }
            Ok(())
        }
    });
    while subscription.position() < head {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    subscription.stop().await;
    let rows = lock(&rows).clone();
    let paid_in = lock(&paid_in).clone();
    Ok((rows, paid_in))
}

/// What a step of the Stage sends, and what came back.
pub(super) struct Sent {
    pub outcome: Result<Outcome, kernable::Error>,
    pub returned: Instant,
    pub answer: Result<Value, kernable::Error>,
    pub answered: Instant,
}

/// Sends one command as a client does, then asks the read model for its aggregate.
pub(super) async fn send(
    kernable: &Kernable,
    command_id: &str,
    (aggregate, id, command): (&str, &str, &str),
    data: Value,
    (query, who): (&str, &str),
) -> Sent {
    // Who sends it: the request context travels signed with the command.
    let who = RequestContext {
        sender_identity: who.to_owned(),
        ..Default::default()
    };
    let submit = Submit::new("bank", aggregate, id, command)
        .id(command_id)
        .context(who)
        .data(data);
    // Returns once every listener has the events: the query below already sees them.
    let outcome = kernable.submit(submit).await;
    let returned = Instant::now(); // stage
    let get = Query::new("bank", query).data(json!({ "id": id }));
    let answer: Result<Value, _> = kernable.query(get).await;
    Sent {
        outcome,
        returned,
        answer,
        answered: Instant::now(),
    }
}
