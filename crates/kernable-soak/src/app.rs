//! The soak's Rust app: domain `bank_rs`, the same as `apps/bank.ts` and `apps/bank.py`.
//! Accounts take deposits and withdrawals (never below zero); a read model keeps balances in a
//! store; a reactor flags every withdrawal of 50 or more, once. An account holds its `number` as
//! a unique key and, while open, the fact `open`, which a withdrawal sent `to` an account
//! requires of that account.

use kernable::{
    App, BoxError, Kernable, NewEvent, Options, Rejected, Requirement, Store, Submit, Unique, Wait,
    domain,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

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

pub async fn run() -> Result<(), BoxError> {
    let env = |name: &str| std::env::var(name).map_err(|_| format!("{name} is not set"));
    let kernable = Kernable::new(Options::new(env("KERNABLE_KEY")?).url(env("KERNABLE_URL")?))?;
    let bank = domain("bank_rs");
    let account = bank
        .aggregate("account", Account::default())
        .snapshot_every(5);
    account.apply("Deposited", |s, d: Amount, _evt, _ctx| {
        s.balance += d.amount
    });
    account.apply("Withdrawn", |s, d: Amount, _evt, _ctx| {
        s.balance -= d.amount
    });
    account.apply("Opened", |s, _: serde_json::Value, _evt, _ctx| {
        s.open = true
    });
    account.apply("Closed", |s, _: serde_json::Value, _evt, _ctx| {
        s.open = false
    });
    account.apply("Numbered", |s, d: Number, _evt, _ctx| s.number = d.number);
    account.unique("number", |s| s.number.clone(), Unique::tenant());
    account.fact("open", |s| s.open);
    account.command("Deposit", |_, d: Amount, _cmd, _ctx| {
        if d.amount <= 0 {
            return Err(Rejected::new("amount must be positive").code("invalid"));
        }
        Ok(vec![NewEvent::new(
            "Deposited",
            json!({ "amount": d.amount }),
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
    account.requires("Withdraw", |_, d: Withdrawal| {
        d.to.map(|to| Requirement::new("bank_rs", "account", &to, "open"))
    });
    account.command("Open", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Opened", json!({}))])
    });
    account.command("Close", |_, _: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Closed", json!({}))])
    });
    account.command("Number", |_, d: Number, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "Numbered",
            json!({ "number": d.number }),
        )])
    });
    account.command("Flag", |_, d: serde_json::Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "Flagged",
            json!({ "position": d["position"] }),
        )])
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
    bank.read_model("balances-rs")
        .store(&format!("sqlite:{}", env("SOAK_STORE")?), "1")
        .on("Deposited", change(1, 0))
        .on("Withdrawn", change(-1, 0))
        .on(
            "Flagged",
            move |_: serde_json::Value, evt, ctx| async move {
                let (store, r) = row(&ctx.store, &evt.aggregate_id)?;
                let changed = Row {
                    flags: r.flags + 1,
                    ..r
                };
                store.set("accounts", &evt.aggregate_id, &changed)?;
                Ok(())
            },
        )
        .query("GetAccount", |d: Id, _qry, ctx| async move {
            Ok(row(&ctx.store, &d.id)?.1)
        });
    let k = kernable.clone();
    bank.reactor("flagger-rs")
        .on("Withdrawn", move |d: Amount, evt, ctx| {
            let k = k.clone();
            async move {
                if d.amount >= 50 {
                    let flag = Submit::new("bank_rs", "account", &evt.aggregate_id, "Flag")
                        .data(json!({ "position": evt.position }))
                        .id(format!("flag-{}", evt.position))
                        .wait(Wait::Committed)
                        .context(ctx.follow());
                    k.submit(flag).await?;
                }
                Ok(())
            }
        });
    kernable
        .app(App::new().service("bank-rs").domain(&bank))?
        .run()
        .await?;
    Ok(())
}
