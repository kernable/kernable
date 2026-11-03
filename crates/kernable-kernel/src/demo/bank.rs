//! The demo's bank, written with the Rust SDK as any application would be. The Stage shows the
//! `#region`s of this file as the code that runs; lines ending in `// stage` only record for the
//! Stage what happened when (and what state a handler saw), and it shows them dimmed.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Instant,
};

use kernable::{Domain, Kernable, NewEvent, Outcome, Query, Rejected, Submit, Unique, domain};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::Recorder;

/// The read models' rows, by aggregate ID; the Stage shows them as they are.
#[derive(Clone, Default)]
pub(super) struct Rows {
    pub accounts: Arc<Mutex<BTreeMap<String, Row>>>,
    pub customers: Arc<Mutex<BTreeMap<String, CustomerRow>>>,
}

/// The rows, also after a handler panicked while holding them.
fn lock<T>(rows: &Mutex<T>) -> MutexGuard<'_, T> {
    rows.lock().unwrap_or_else(PoisonError::into_inner)
}

// #region state
#[derive(Clone, Default, Serialize, Deserialize)]
struct Account {
    owner: String,
    balance: i64,
    open: bool,
}

#[derive(Serialize, Deserialize)]
struct Opened {
    owner: String,
}

#[derive(Serialize, Deserialize)]
struct Amount {
    amount: i64,
}
// #endregion state

// #region row
#[derive(Clone, Serialize)]
pub(super) struct Row {
    owner: String,
    balance: i64,
}
// #endregion row

/// The `bank` domain: the `account` and `customer` aggregates and their read models.
pub(super) fn bank(stage: &Recorder, rows: &Rows) -> Domain {
    let bank_domain = domain("bank");
    let account = bank_domain.aggregate("account", Account::default());

    // #region events
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
    // #endregion events

    let c = stage.clone(); // stage
    // #region open
    account.command("OpenAccount", move |state, data: Opened, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if state.open {
            return Err(Rejected::new("the account is already open"));
        }
        Ok(vec![NewEvent::new("AccountOpened", data)])
    });
    // #endregion open

    let c = stage.clone(); // stage
    // #region deposit
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
    // #endregion deposit

    let c = stage.clone(); // stage
    // #region withdraw
    account.command("Withdraw", move |state, data: Amount, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if data.amount > state.balance {
            let message = format!("insufficient funds: the balance is {}", state.balance);
            return Err(Rejected::new(message).code("insufficient_funds"));
        }
        Ok(vec![NewEvent::new("Withdrawn", data)])
    });
    // #endregion withdraw

    let accounts_readmodel = bank_domain.read_model("accounts");

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    // #region project-opened
    accounts_readmodel.on("AccountOpened", move |data: Opened, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        let row = Row {
            owner: data.owner,
            balance: 0,
        };
        lock(&r).insert(evt.aggregate_id, row);
        async { Ok(()) }
    });
    // #endregion project-opened

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    // #region project-deposited
    accounts_readmodel.on("Deposited", move |data: Amount, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance += data.amount;
        }
        async { Ok(()) }
    });
    // #endregion project-deposited

    let (r, c) = (Arc::clone(&rows.accounts), stage.clone());
    // #region project-withdrawn
    accounts_readmodel.on("Withdrawn", move |data: Amount, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance -= data.amount;
        }
        async { Ok(()) }
    });
    // #endregion project-withdrawn

    #[derive(Deserialize)]
    struct GetAccount {
        id: String,
    }

    let r = Arc::clone(&rows.accounts);
    // #region query
    accounts_readmodel.query("GetAccount", move |data: GetAccount, _qry, _ctx| {
        let row = lock(&r).get(&data.id).cloned();
        async move { Ok(row.ok_or_else(|| Rejected::new("no such account").code("not_found"))?) }
    });
    // #endregion query

    customers(&bank_domain, stage, rows);
    bank_domain
}

// #region customer-state
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
// #endregion customer-state

#[derive(Clone, Serialize)]
pub(super) struct CustomerRow {
    name: String,
    username: String,
}

/// The `customer` aggregate, whose username is unique, and the `customers` read model.
fn customers(bank_domain: &Domain, stage: &Recorder, rows: &Rows) {
    let customer = bank_domain.aggregate("customer", Customer::default());

    // #region customer-key
    // One customer per username in the tenant, whatever its case. The kernel checks it when it
    // commits; it gets a hash of the value, never the value.
    customer.unique(
        "username",
        |state| state.username.as_ref().map(|u| u.trim().to_lowercase()),
        Unique::tenant(),
    );
    // #endregion customer-key

    // #region customer-events
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
    // #endregion customer-events

    let c = stage.clone(); // stage
    // #region register
    customer.command("Register", move |state, data: Registered, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if state.registered {
            return Err(Rejected::new("this customer is registered already"));
        }
        // No lookup of the username here: the kernel refuses it at commit if it is taken.
        Ok(vec![NewEvent::new("CustomerRegistered", data)])
    });
    // #endregion register

    let c = stage.clone(); // stage
    // #region rename
    customer.command("ChangeUsername", move |state, data: Username, cmd, _ctx| {
        c.worker(&cmd.command_id, state); // stage
        if !state.registered {
            return Err(Rejected::new("no such customer").code("not_found"));
        }
        // The old username is free once this commits.
        Ok(vec![NewEvent::new("UsernameChanged", data)])
    });
    // #endregion rename

    let customers_readmodel = bank_domain.read_model("customers");

    let (r, c) = (Arc::clone(&rows.customers), stage.clone());
    // #region project-registered
    customers_readmodel.on("CustomerRegistered", move |data: Registered, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        let row = CustomerRow {
            name: data.name,
            username: data.username,
        };
        lock(&r).insert(evt.aggregate_id, row);
        async { Ok(()) }
    });
    // #endregion project-registered

    let (r, c) = (Arc::clone(&rows.customers), stage.clone());
    // #region project-renamed
    customers_readmodel.on("UsernameChanged", move |data: Username, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.username = data.username;
        }
        async { Ok(()) }
    });
    // #endregion project-renamed

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
    query: &str,
) -> Sent {
    // #region client
    let submit = Submit::new("bank", aggregate, id, command)
        .id(command_id)
        .data(data);
    // Returns once every listener has the events: the query below already sees them.
    let outcome = kernable.submit(submit).await;
    let returned = Instant::now(); // stage
    let get = Query::new("bank", query).data(json!({ "id": id }));
    let answer: Result<Value, _> = kernable.query(get).await;
    // #endregion client
    Sent {
        outcome,
        returned,
        answer,
        answered: Instant::now(),
    }
}
