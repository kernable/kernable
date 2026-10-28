//! The vault: personal fields and secrets are declared on the domain; the SDK keeps them out of
//! the signed log, handlers and allowed listeners see them in clear, and an erasing event
//! deletes them for good. Ported from the TypeScript SDK's tests.

mod common;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use common::TestKernel;
use kernable::{
    Aggregate, Domain, Error, NewEvent, Outcome, Query, Rejected, StoredEventMeta, Submit, domain,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn sha256(s: &str) -> String {
    Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn field(d: &Value, name: &str) -> String {
    d[name].as_str().unwrap_or_default().to_owned()
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Account {
    email: Option<String>,
    password_hash: String,
}

#[derive(Deserialize)]
struct Register {
    id: String,
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct Registered {
    email: Option<String>,
    #[serde(rename = "passwordHash")]
    password_hash: String,
}

#[derive(Deserialize)]
struct Password {
    password: String,
}

#[derive(Deserialize)]
struct Id {
    id: String,
}

fn identity_domain() -> (Domain, Aggregate<Account>) {
    let identity = domain("identity");
    identity
        .command("RegisterAccount")
        .personal(&["email"], |d, _| field(d, "id"))
        .secret(&["password"]);
    identity
        .event("AccountRegistered", 1)
        .personal(&["email"], |d, _| field(d, "accountId"));
    identity
        .event("AccountErased", 1)
        .erases(|d, _| field(d, "accountId"));
    let account = identity.aggregate("Account", Account::default());
    account.apply("AccountRegistered", |s, data: Registered, _evt, _ctx| {
        *s = Account {
            email: data.email,
            password_hash: data.password_hash,
        }
    });
    account.apply("AccountErased", |s, _: Value, _evt, _ctx| {
        *s = Account::default()
    });
    account.command("RegisterAccount", |_, data: Register, _cmd, _ctx| {
        // The handler gets the secret in clear and keeps only a hash.
        let registered = json!({
            "accountId": data.id,
            "email": data.email,
            "passwordHash": sha256(&data.password),
        });
        Ok(vec![NewEvent::new("AccountRegistered", registered)])
    });
    account.command("Login", |s, data: Password, _cmd, _ctx| {
        if sha256(&data.password) != s.password_hash {
            return Err(Rejected::new("wrong password").code("denied"));
        }
        Ok(vec![NewEvent::new("LoggedIn", ())])
    });
    account.command("EraseAccount", |_, data: Id, _cmd, _ctx| {
        Ok(vec![NewEvent::new(
            "AccountErased",
            json!({ "accountId": data.id }),
        )])
    });
    identity.command("Login").secret(&["password"]);
    (identity, account)
}

#[tokio::test(flavor = "multi_thread")]
async fn personal_fields_and_secrets_live_in_the_vault_and_an_erasing_event_deletes_them() {
    let kernel = TestKernel::new(&["identity.*"], &[]);
    kernel.start().await;
    let (identity, account) = identity_domain();
    let kernable = kernel.client().with_domain(&identity);
    let emails = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let e = Arc::clone(&emails);
    let accounts = identity.read_model("accounts").on(
        "identity.AccountRegistered",
        move |data: Value, evt, _ctx| {
            e.lock()
                .unwrap()
                .insert(evt.aggregate_id, field(&data, "email"));
            async { Ok(()) }
        },
    );
    let e = Arc::clone(&emails);
    accounts.on("identity.AccountErased", move |_: Value, evt, _ctx| {
        e.lock().unwrap().remove(&evt.aggregate_id);
        async { Ok(()) }
    });
    let e = Arc::clone(&emails);
    accounts.query("identity.GetEmail", move |data: Id, _qry, _ctx| {
        let email = e.lock().unwrap().get(&data.id).cloned();
        async move { Ok(email.ok_or_else(|| Rejected::new("no such account").code("not_found"))?) }
    });
    let worker = kernable.worker(&account);
    let accounts = kernable.listener(&accounts);
    let mut running = vec![
        {
            let w = worker.clone();
            tokio::spawn(async move { w.run().await })
        },
        {
            let a = accounts.clone();
            tokio::spawn(async move { a.run().await })
        },
    ];
    let send = |command_type: &str, data: Value| {
        kernable.submit(Submit::new("identity", "Account", "a1", command_type).data(data))
    };
    let get_email =
        || kernable.query::<String>(Query::new("identity", "GetEmail").data(json!({ "id": "a1" })));
    worker.ready().await;
    accounts.ready().await;

    let registered = send(
        "RegisterAccount",
        json!({ "id": "a1", "email": "max@firma.de", "password": "correct horse" }),
    )
    .await
    .unwrap();
    // The submitter is no reader of what the vault keeps: it gets references only.
    let event = &registered.events()[0];
    assert_eq!(
        event.data,
        json!({ "accountId": "a1", "email": null, "passwordHash": sha256("correct horse") })
    );
    assert_eq!(event.redacted, ["email"]);
    assert_eq!(get_email().await.unwrap(), "max@firma.de");
    assert_eq!(
        send("Login", json!({ "password": "wrong" })).await.unwrap(),
        Outcome::Rejected(Rejected::new("wrong password").code("denied"))
    );
    assert!(
        send("Login", json!({ "password": "correct horse" }))
            .await
            .unwrap()
            .is_committed()
    );

    // Neither the email nor the password is in any file of the kernel in clear.
    for entry in std::fs::read_dir(&kernel.dir).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        for secret in ["max@firma.de", "correct horse"] {
            let found = bytes.windows(secret.len()).any(|w| w == secret.as_bytes());
            assert!(!found, "{secret} is stored in clear in {}", path.display());
        }
    }

    // Erasing: the read model drops the account; a rebuilt one reads null where the email was.
    assert!(
        send("EraseAccount", json!({ "id": "a1" }))
            .await
            .unwrap()
            .is_committed()
    );
    assert!(matches!(get_email().await, Err(Error::Rejected(r)) if r.message == "no such account"));
    let replayed = Arc::new(Mutex::new(Vec::<(Value, StoredEventMeta)>::new()));
    let r = Arc::clone(&replayed);
    let rebuilt = identity.read_model("rebuilt").on(
        "identity.AccountRegistered",
        move |data: Value, evt, _ctx| {
            r.lock().unwrap().push((data, evt));
            async { Ok(()) }
        },
    );
    let rebuilt = kernable.listener(&rebuilt);
    running.push({
        let rebuilt = rebuilt.clone();
        tokio::spawn(async move { rebuilt.run().await })
    });
    rebuilt.ready().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let replayed = replayed.lock().unwrap().clone();
    assert_eq!(replayed.len(), 1);
    assert_eq!(
        (&replayed[0].0["email"], &replayed[0].1.redacted),
        (&Value::Null, &vec!["email".to_owned()])
    );
    for s in [&accounts, &rebuilt] {
        s.stop();
    }
    worker.stop();
    for r in running {
        r.await.unwrap();
    }
}

#[test]
#[should_panic(expected = "unqualified")]
fn command_definitions_take_unqualified_types() {
    identity_domain().0.command("shop.Register");
}
