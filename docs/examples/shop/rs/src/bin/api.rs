//! The shop's API (part 9): the kit's domains with the authorization middleware, and the kit's
//! example HTTP server. People sign in here; the kernel and the kit decide what they may do.
use std::sync::Arc;

use kernable::{App, BoxError, Kernable, Options};
use kit::api::{KitApi, new_signing_keys};
use kit::authorization::{ids, relation};
use kit::bootstrap::bootstrap;
use kit::shared::mail::{Mail, Mailer, use_mailer};
use kit::tenancy::{AccountSignUp, SignUpPolicy, TenantSignUp};
use shop::shop;

// The kit's example server (kits/rust/examples/server.rs), as the kit's own tests use it.
#[path = "../../../../../../kits/rust/examples/server.rs"]
#[allow(dead_code)]
mod server;

// #region mail
/// In development the kit's mails go to files in mail/, named by address, instead of a mail server.
struct FileMailer;

impl Mailer for FileMailer {
    fn send(&self, mail: Mail) {
        std::fs::write(format!("mail/{}", mail.to), mail.token).expect("mail/ is writable");
    }
}
// #endregion mail

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // #region relation
    // Who owns an order: the customer it was placed for. Declared before the kit's domains start.
    relation("shop.order", "owner").set("shop.OrderPlaced", |e| ids(&e.data["customerId"]));
    // #endregion relation

    std::fs::create_dir_all("mail")?;
    use_mailer(Arc::new(FileMailer));

    // #region run
    // The kit's domains: its aggregates, read models, reactors and the authorization middleware.
    let domains = kit::kit_domains();
    let mut definition = App::new().service("kit");
    for d in &domains {
        definition = definition.domain(d);
    }
    let kit = Kernable::new(Options::default())?.app(definition)?;
    let running = kit.clone();
    tokio::spawn(async move { running.run().await });
    kit.ready().await;

    // The API's client knows the shop's schemas too: GetOrder declares the order it is about.
    let kernable = Kernable::new(Options::default())?.with_domain(&shop().domain);
    for d in &domains {
        kernable.with_domain(d);
    }
    // Once: the platform tenant, the sign-up policy, and the API's own identity and key.
    let policy = SignUpPolicy {
        accounts: AccountSignUp::Open,
        tenants: TenantSignUp::Active,
    };
    let booted = bootstrap(&kernable, policy, "Platform", None).await?;
    let api = KitApi::new(
        kernable.clone(),
        new_signing_keys(),
        &booted.api_identity,
        &booted.api_key_id,
    );
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    axum::serve(listener, server::router(api, kernable)).await?;
    // #endregion run
    Ok(())
}
