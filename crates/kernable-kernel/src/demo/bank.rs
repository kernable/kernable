//! The demo's bank, written with the Rust SDK as any application would be. The Stage shows the
//! `#region`s of this file as the code that runs; lines ending in `// stage` only record for the
//! Stage what happened when (and what state a handler saw), and it shows them dimmed.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Instant,
};

use kernable::{Domain, Kernable, NewEvent, Outcome, Query, Rejected, Submit, domain};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::Recorder;

/// The read model's rows, by account ID; the Stage shows them as they are.
pub(super) type Rows = Arc<Mutex<BTreeMap<String, Row>>>;

/// The rows, also after a handler panicked while holding them.
fn lock(rows: &Rows) -> MutexGuard<'_, BTreeMap<String, Row>> {
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

/// The `bank` domain: the `account` aggregate and the `accounts` read model.
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

    let (r, c) = (Arc::clone(rows), stage.clone());
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

    let (r, c) = (Arc::clone(rows), stage.clone());
    // #region project-deposited
    accounts_readmodel.on("Deposited", move |data: Amount, evt, _ctx| {
        c.read_model(&evt.command_id); // stage
        if let Some(row) = lock(&r).get_mut(&evt.aggregate_id) {
            row.balance += data.amount;
        }
        async { Ok(()) }
    });
    // #endregion project-deposited

    let (r, c) = (Arc::clone(rows), stage.clone());
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

    let r = Arc::clone(rows);
    // #region query
    accounts_readmodel.query("GetAccount", move |data: GetAccount, _qry, _ctx| {
        let row = lock(&r).get(&data.id).cloned();
        async move { Ok(row.ok_or_else(|| Rejected::new("no such account").code("not_found"))?) }
    });
    // #endregion query

    bank_domain
}

/// What a step of the Stage sends, and what came back.
pub(super) struct Sent {
    pub outcome: Result<Outcome, kernable::Error>,
    pub returned: Instant,
    pub answer: Result<Value, kernable::Error>,
    pub answered: Instant,
}

/// Sends one command as a client does, then asks the read model for the account.
pub(super) async fn send(
    kernable: &Kernable,
    command_id: &str,
    command: &str,
    id: &str,
    data: Value,
) -> Sent {
    // #region client
    let submit = Submit::new("bank", "account", id, command)
        .id(command_id)
        .data(data);
    // Returns once every listener has the events: the query below already sees them.
    let outcome = kernable.submit(submit).await;
    let returned = Instant::now(); // stage
    let get_account = Query::new("bank", "GetAccount").data(json!({ "id": id }));
    let answer: Result<Value, _> = kernable.query(get_account).await;
    // #endregion client
    Sent {
        outcome,
        returned,
        answer,
        answered: Instant::now(),
    }
}
