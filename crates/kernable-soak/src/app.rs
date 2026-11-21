//! The soak's Rust app: domain `bank_rs`, the same as `apps/bank.ts`, `apps/bank.py` and
//! `apps/bankgo`. Accounts take deposits and withdrawals (never below zero); a read model keeps
//! balances in a store; a reactor flags every withdrawal of 50 or more, once. An account holds
//! its `number` as a unique key and, while open, the fact `open`, which a withdrawal sent `to` an
//! account requires of that account. A `Note` stores its data as it came (edge-case payloads),
//! and the read model answers it back.
//!
//! It also runs half of `bank_x`, the domain two languages share: its accounts on workers here
//! and in the Go app at once (they take turns as the faults move them, through each other's
//! snapshots), its flagger here and its read model in the Go app.

use kernable::{
    Aggregate, App, BoxError, Domain, Kernable, Listener, NewEvent, Options, Rejected, Requirement,
    Store, Submit, Unique, Wait, domain,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Default, Serialize, Deserialize)]
struct Account {
    balance: i64,
    open: bool,
    number: Option<String>,
}

#[derive(Deserialize)]
struct Amount {
    amount: i64,
}

#[derive(Deserialize)]
struct Withdrawal {
    amount: i64,
    to: Option<String>,
}

#[derive(Deserialize)]
struct Number {
    number: Option<String>,
}

#[derive(Deserialize)]
struct Id {
    id: String,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Row {
    pub balance: i64,
    pub flags: i64,
}

fn row(store: &Option<Store>, id: &str) -> Result<(Store, Row), BoxError> {
    let store = store.clone().ok_or("no store")?;
    let row = store.get("accounts", id)?.unwrap_or_default();
    Ok((store, row))
}

/// The bank as domain `name`: its account aggregate, its read model `balances-<suffix>` (in
/// `store`) and its flagger `flagger-<suffix>`. Deposits say `by` which language decided them.
fn bank(name: &str, suffix: &str, store: &str) -> (Domain, Aggregate<Account>, Listener, Listener) {
    let bank = domain(name);
    let account = bank
        .aggregate("account", Account::default())
        .snapshot_every(5);
    account.apply("Deposited", |s, d: Amount, _evt, _ctx| {
        s.balance += d.amount
    });
    account.apply("Withdrawn", |s, d: Amount, _evt, _ctx| {
        s.balance -= d.amount
    });
    account.apply("Opened", |s, _: Value, _evt, _ctx| s.open = true);
    account.apply("Closed", |s, _: Value, _evt, _ctx| s.open = false);
    account.apply("Numbered", |s, d: Number, _evt, _ctx| s.number = d.number);
    account.unique("number", |s| s.number.clone(), Unique::tenant());
    account.fact("open", |s| s.open);
    account.command("Deposit", |_, d: Amount, _cmd, _ctx| {
        if d.amount <= 0 {
            return Err(Rejected::new("amount must be positive").code("invalid"));
        }
        Ok(vec![NewEvent::new(
            "Deposited",
            json!({ "amount": d.amount, "by": "rs" }),
        )])
    });
    account.command("Withdraw", |s, d: Withdrawal, _cmd, _ctx| {
        if s.balance < d.amount {
            return Err(Rejected::new("insufficient funds").code("insufficient"));
        }
        Ok(vec![NewEvent::new(
            "Withdrawn",
            json!({ "amount": d.amount, "to": d.to }),
        )])
    });
    let domain_name = name.to_owned();
    account.requires("Withdraw", move |_, d: Withdrawal| {
        d.to.map(|to| Requirement::new(&domain_name, "account", &to, "open"))
    });
    account.command("Open", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Opened", json!({}))])
    });
    account.command("Close", |_, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Closed", json!({}))])
    });
    account.command("Number", |_, d: Number, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "Numbered",
            json!({ "number": d.number }),
        )])
    });
    account.command("Flag", |_, d: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "Flagged",
            json!({ "position": d["position"] }),
        )])
    });
    // The data as it came: the soak checks it arrives in the log unchanged.
    account.command("Note", |_, d: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Noted", d)])
    });

    let change = |balance: i64, flags: i64| {
        move |d: Amount, evt: kernable::StoredEventMeta, ctx: kernable::ListenerContext| async move {
            let (store, r) = row(&ctx.store, &evt.aggregate_id)?;
            let changed = Row {
                balance: r.balance + balance * d.amount,
                flags: r.flags + flags,
            };
            store.set("accounts", &evt.aggregate_id, &changed)?;
            Ok::<(), BoxError>(())
        }
    };
    let balances = bank
        .read_model(&format!("balances-{suffix}"))
        .store(store, "1")
        .on("Deposited", change(1, 0))
        .on("Withdrawn", change(-1, 0))
        .on("Flagged", move |_: Value, evt, ctx| async move {
            let (store, r) = row(&ctx.store, &evt.aggregate_id)?;
            let changed = Row {
                flags: r.flags + 1,
                ..r
            };
            store.set("accounts", &evt.aggregate_id, &changed)?;
            Ok(())
        })
        .on("Noted", move |d: Value, evt, ctx| async move {
            let store = ctx.store.clone().ok_or("no store")?;
            store.set("notes", &evt.aggregate_id, &d)?;
            Ok(())
        })
        .query("GetAccount", |d: Id, _qry, ctx| async move {
            Ok(row(&ctx.store, &d.id)?.1)
        })
        .query("GetNote", |d: Id, _qry, ctx| async move {
            let store = ctx.store.clone().ok_or("no store")?;
            Ok(store.get::<Value>("notes", &d.id)?.unwrap_or(Value::Null))
        });
    let flagger_domain = name.to_owned();
    let flagger =
        bank.reactor(&format!("flagger-{suffix}"))
            .on("Withdrawn", move |d: Amount, evt, ctx| {
                let domain = flagger_domain.clone();
                async move {
                    if d.amount >= 50 {
                        // A rejection is stored for good under this ID: the reactor parks the
                        // event, and B7 checks that only such events are parked.
                        let flag = Submit::new(&domain, "account", &evt.aggregate_id, "Flag")
                            .data(json!({ "position": evt.position }))
                            .id(format!("flag-{}", evt.position))
                            .wait(Wait::Committed);
                        ctx.submit(flag).await?;
                    }
                    Ok(())
                }
            });
    (bank, account, balances, flagger)
}

pub async fn run() -> Result<(), BoxError> {
    let env = |name: &str| std::env::var(name).map_err(|_| format!("{name} is not set"));
    let options = || -> Result<Options, BoxError> {
        Ok(Options::new(env("KERNABLE_KEY")?).url(env("KERNABLE_URL")?))
    };
    let store = env("SOAK_STORE")?;
    let (bank_rs, _, _, _) = bank("bank_rs", "rs", &format!("sqlite:{store}"));
    // bank_x without a service: its workers here and in the Go app are both always waited for.
    let shared = Kernable::new(options()?)?;
    let (_x, account_x, _balances_x, flagger_x) = bank("bank_x", "x", &format!("sqlite:{store}-x"));
    let worker = shared.worker(&account_x);
    let flagger = shared.listener(&flagger_x);
    tokio::spawn(async move { worker.run().await });
    tokio::spawn(async move { flagger.run().await });
    Kernable::new(options()?)?
        .app(App::new().service("bank-rs").domain(&bank_rs))?
        .run()
        .await?;
    Ok(())
}
