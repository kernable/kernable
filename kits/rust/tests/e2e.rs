//! The thin slice: the kit end to end against a real kernel. A shop app declares a relation;
//! people register, verify, sign in, open a tenant, form groups, and the authorization middleware
//! decides. One test, in order, sharing the kernel:
//!
//! ```sh
//! cargo test -p kernable-kit --test e2e -- --ignored
//! ```

#[path = "e2e/authenticator.rs"]
mod authenticator;
#[path = "../../../sdks/rust/tests/common/mod.rs"]
mod common;
#[path = "e2e/oidc_provider.rs"]
mod oidc_provider;
#[path = "../examples/server.rs"]
#[allow(dead_code)]
mod server;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kernable::process::{Send, Start};
use kernable::{
    App, AppHandle, Error, Kernable, Options, Outcome, Query, RequestContext, Submit, command,
    event, query,
};
use kit::account::{
    AddSsoDomain, CreateSso, DeleteAccount, EnforceSso, FindAccountByEmail, GetSso, SuspendAccount,
};
use kit::api::NewInvitationLink;
use kit::api::{ApiError, Claims, KitApi, Provider, new_signing_keys};
use kit::authorization::{
    Explain, Target, Visibility, ids, relation, use_authorization_store, visibility,
};
use kit::bootstrap::{Admin, bootstrap};
use kit::group::{CreateGroup, Permission};
use kit::identity::{
    CreateServiceIdentity, DeleteIdentity, GetIdentity, GrantMembership, RevokeApiKey,
};
use kit::invitation::{
    CreateInvitation, EraseInvitation, InvitedGroup, ListInvitations, PeekedLink,
    RevokeInvitationLink,
};
use kit::shared::constants::PLATFORM_TENANT as PLATFORM;
use kit::shared::mail::{MemoryMailer, Purpose, use_mailer};
use kit::shared::secrets::{new_id, totp_code};
use kit::tenancy::{
    AccountSignUp, ChangeSignUpPolicy, CreateTenant, DeleteTenant, GetTenant, Owner,
    ReactivateTenant, RequireMfa, SignUpPolicy, SuspendTenant, SuspensionReason, TenantSignUp,
    TenantStatus,
};
use serde_json::{Value, json};

use authenticator::Authenticator;
use common::TestKernel;
use oidc_provider::OidcProvider;

// --- A small app: orders that belong to whoever placed them -------------------------------------------

#[event("shop")]
pub struct OrderPlaced {
    pub customer: String,
}

#[event("shop")]
pub struct OrderCancelled {}

#[command("shop", aggregate = "order")]
pub struct PlaceOrder {}

#[command("shop", aggregate = "order")]
pub struct CancelOrder {}

/// Starts the shop's process, which cancels the order as the process.
#[command("shop", aggregate = "order")]
pub struct FlagOrder {}

#[event("shop")]
pub struct OrderFlagged {}

#[query("shop", answer = Vec<String>)]
pub struct ListOrders {}

/// One order, for whoever may get it: the query names the order it is about (`target`), so the
/// middleware decides on that order, as on a command's aggregate.
#[query("shop", answer = OrderDetails, target(aggregate = "order", field = id))]
pub struct GetOrder {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderDetails {
    pub customer: String,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct Order {
    status: String,
}

fn shop() -> kernable::Domain {
    let shop = kernable::domain("shop");
    let order = shop.aggregate("order", Order::default());
    order.apply(OrderPlaced, |s, _, _, _| s.status = "placed".into());
    order.apply(OrderCancelled, |s, _, _, _| s.status = "cancelled".into());
    order.command(PlaceOrder, |s, _, _, ctx| {
        if !s.status.is_empty() {
            return Err(kernable::Rejected::new("The order exists").code("exists"));
        }
        Ok(vec![
            OrderPlaced {
                customer: ctx.request.sender_identity.clone(),
            }
            .into(),
        ])
    });
    order.apply(OrderFlagged, |_, _, _, _| {});
    order.command(FlagOrder, |_, _, _, _| Ok(vec![OrderFlagged {}.into()]));
    // A flagged order is cancelled by the shop's process, acting as its own identity.
    shop.process("flagged")
        .start::<OrderFlagged>(Start::key("aggregate_id").data([("order_id", "aggregate_id")]))
        .command::<CancelOrder>("cancel", Send::aggregate("case.order_id"))
        .end("done");
    order.command(CancelOrder, |s, _, _, _| {
        if s.status != "placed" {
            return Err(kernable::Rejected::new("Not placed").code("invalid_state"));
        }
        Ok(vec![OrderCancelled {}.into()])
    });
    // The app's list of orders, showing each asker only what the middleware would let them get.
    let rows: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let orders = shop.read_model("shop.orders");
    let r = Arc::clone(&rows);
    orders.on(OrderPlaced, move |_, evt, _| {
        r.lock()
            .unwrap()
            .push((evt.tenant.clone(), evt.aggregate_id.clone()));
        async { Ok(()) }
    });
    let visible: Visibility = visibility(&orders);
    orders.query(ListOrders, move |_, qry, ctx| {
        let mut mine: Vec<String> = rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, id)| {
                *t == qry.tenant
                    && visible.allows(
                        &ctx.store,
                        &qry.tenant,
                        &ctx.request,
                        "shop.GetOrder",
                        &Target::new("shop.order", id),
                    )
            })
            .map(|(_, id)| id.clone())
            .collect();
        mine.sort();
        async move { Ok(mine) }
    });
    let customers: Arc<Mutex<std::collections::HashMap<String, String>>> = Arc::default();
    let details = shop.read_model("shop.order-details");
    let c = Arc::clone(&customers);
    details.on(OrderPlaced, move |d, evt, _| {
        c.lock()
            .unwrap()
            .insert(format!("{}/{}", evt.tenant, evt.aggregate_id), d.customer);
        async { Ok(()) }
    });
    details.query(GetOrder, move |d, qry, _| {
        let customer = customers
            .lock()
            .unwrap()
            .get(&format!("{}/{}", qry.tenant, d.id))
            .cloned();
        async move {
            customer
                .map(|customer| OrderDetails { customer })
                .ok_or_else(|| {
                    kernable::Rejected::new("No such order")
                        .code("not_found")
                        .into()
                })
        }
    });
    shop
}

// --- Setup ---------------------------------------------------------------------------------------------

const PASSWORD: &str = "correct horse battery";

struct World {
    kernel: TestKernel,
    kit: Kernable,
    api: KitApi,
    mailer: Arc<MemoryMailer>,
    app: Mutex<Option<AppHandle>>,
    service: (String, String),
}

impl World {
    fn options(&self) -> Options {
        Options::new(self.kernel.dir.join("kit.key")).url(&self.kernel.url)
    }

    async fn start_app(&self) {
        let mut definition = App::new().service("kit");
        let domains = kit::kit_domains();
        let shop = shop();
        for d in domains.iter().chain([&shop]) {
            definition = definition.domain(d);
        }
        let app = Kernable::new(self.options())
            .unwrap()
            .app(definition)
            .unwrap();
        let running = app.clone();
        tokio::spawn(async move { running.run().await });
        app.ready().await;
        *self.app.lock().unwrap() = Some(app);
    }

    fn stop_app(&self) {
        if let Some(app) = self.app.lock().unwrap().take() {
            app.stop();
        }
    }

    async fn person(&self, email: &str) -> (Claims, String) {
        let name = email.split('@').next().unwrap();
        self.api
            .register(email, name, Some(PASSWORD))
            .await
            .unwrap();
        let mail = self
            .mailer
            .last(email)
            .unwrap_or_else(|| panic!("a verification mail to {email}"));
        assert!(self.api.verify_email(&mail.token).await.unwrap());
        let signed = self
            .api
            .sign_in(email, PASSWORD, Some("203.0.113.7"), None)
            .await
            .unwrap();
        (
            self.api.check(&signed.access_token).unwrap(),
            signed.account_id,
        )
    }

    async fn send<C: kernable::CommandType>(
        &self,
        command: C,
        aggregate_id: &str,
        tenant: &str,
        context: RequestContext,
    ) -> Outcome {
        self.kit
            .submit(
                Submit::command(command, aggregate_id)
                    .tenant(tenant)
                    .context(context),
            )
            .await
            .unwrap()
    }

    async fn context(&self, claims: &Claims, tenant: Option<&str>) -> RequestContext {
        self.api.context_for(claims, tenant).await.unwrap()
    }
}

fn rejected(outcome: &Outcome, code: &str) {
    match outcome {
        Outcome::Rejected(r) => assert_eq!(r.code, code, "{r:?}"),
        Outcome::Committed(_) => panic!("committed, expected {code}"),
    }
}

fn committed(outcome: &Outcome) {
    assert!(outcome.is_committed(), "{outcome:?}");
}

fn refused<T: std::fmt::Debug>(result: Result<T, ApiError>, code: &str) {
    match result {
        Err(ApiError::Refused(c)) => assert_eq!(c, code),
        other => panic!("expected {code}, got {other:?}"),
    }
}

fn owner(account: &str) -> Owner {
    Owner {
        account_id: Some(account.into()),
        email: None,
    }
}

fn tenant(slug: &str, name: &str, account: &str, status: Option<TenantStatus>) -> CreateTenant {
    CreateTenant {
        slug: slug.into(),
        name: name.into(),
        owner: owner(account),
        status,
    }
}

async fn query_code<T: serde::de::DeserializeOwned>(kit: &Kernable, q: Query) -> String {
    match kit.query::<T>(q).await {
        Err(Error::Rejected(r)) => r.code,
        Err(e) => format!("error: {e}"),
        Ok(_) => "allow".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the kernel: cargo test -p kernable-kit --test e2e -- --ignored"]
async fn the_kit_end_to_end() {
    relation("shop.order", "owner").set("shop.OrderPlaced", |e| ids(&e.data["customer"]));
    let mailer = Arc::new(MemoryMailer::default());
    use_mailer(mailer.clone());
    let kernel = TestKernel::new(
        &[
            "tenancy.*",
            "account.*",
            "identity.*",
            "group.*",
            "invitation.*",
            "shop.*",
        ],
        &[],
    );
    kernel.start().await;
    assert!(kernel.cli(&["keys", "new", "kit"]).0);
    let mut grant = vec![
        "grant",
        "root.key",
        "kit.pub",
        "run=tenancy.*,account.*,identity.*,group.*,invitation.*,shop.*",
    ];
    let all: Vec<String> = [
        "submit",
        "listen",
        "query",
        "answer",
        "check",
        "vault_read",
        "vault_erase",
        "manage",
        "reserved_order",
        "cache",
        "process",
    ]
    .iter()
    .map(|a| format!("{a}=*"))
    .collect();
    grant.extend(all.iter().map(String::as_str));
    grant.push("privileged=bypass_authorization");
    assert!(kernel.cli(&grant).0, "grant");
    use_authorization_store(&format!(
        "sqlite:{}",
        kernel.dir.join("authorization.db").display()
    ));
    let kit = Kernable::new(Options::new(kernel.dir.join("kit.key")).url(&kernel.url)).unwrap();
    let mut world = World {
        kernel,
        kit: kit.clone(),
        api: KitApi::new(kit.clone(), new_signing_keys(), "", ""),
        mailer,
        app: Mutex::new(None),
        service: (String::new(), String::new()),
    };
    world.start_app().await;
    let policy = SignUpPolicy {
        tenants: TenantSignUp::Active,
        accounts: AccountSignUp::Open,
    };
    let admin = Admin {
        email: "admin@example.com",
        name: "Admin",
        password: PASSWORD,
    };
    let booted = bootstrap(&kit, policy, "Platform", Some(admin))
        .await
        .unwrap();
    world.service = (booted.api_identity.clone(), booted.api_key_id.clone());
    world.api = KitApi::new(
        kit.clone(),
        new_signing_keys(),
        &booted.api_identity,
        &booted.api_key_id,
    );

    // Each step on the heap: their futures together outgrow a test thread's 2 MiB stack in a
    // debug build (a stack overflow, not an error of the kit).
    let acme = Box::pin(slice(&world)).await;
    Box::pin(suspended_and_deleted(&world, &acme)).await;
    Box::pin(resumes_after_a_restart(&world)).await;
    Box::pin(account_flows(&world)).await;
    Box::pin(second_factor_and_api_keys(&world)).await;
    Box::pin(passkeys(&world)).await;
    Box::pin(oauth_and_sso(&world)).await;
    Box::pin(lists_and_explain(&world)).await;
    Box::pin(example_server(&world)).await;
    Box::pin(invitation_links(&world)).await;
    Box::pin(joining_checks_the_tenant(&world)).await;
    Box::pin(a_process_acts_as_its_own_identity(&world)).await;
    world.stop_app();
}

struct Acme {
    tenant: String,
    alice: RequestContext,
    bob_identity: String,
}

/// Register, verify, sign in, open a tenant, and let groups and relations decide.
async fn slice(w: &World) -> Acme {
    let api = &w.api;
    // Signing in before verifying fails without saying more; an unknown address too.
    api.register("early@example.com", "Early", Some(PASSWORD))
        .await
        .unwrap();
    refused(
        api.sign_in("early@example.com", PASSWORD, None, None).await,
        "not_verified",
    );
    refused(
        api.sign_in("nobody@example.com", PASSWORD, None, None)
            .await,
        "wrong_password",
    );

    let (alice, alice_id) = w.person("alice@example.com").await;
    let (bob, bob_id) = w.person("bob@example.com").await;
    let (carol, _) = w.person("carol@example.com").await;

    // Alice opens a tenant herself (self-service sign-up is on); the kit makes her its owner.
    let t = new_id();
    committed(
        &w.send(
            tenant("acme", "Acme", &alice_id, Some(TenantStatus::Active)),
            &t,
            &t,
            w.context(&alice, None).await,
        )
        .await,
    );
    let as_alice = w.context(&alice, Some(&t)).await;
    assert!(!as_alice.sender_identity.is_empty());

    // A slug is taken across tenants.
    let other = new_id();
    assert!(
        !w.send(
            tenant("acme", "Acme 2", &bob_id, Some(TenantStatus::Active)),
            &other,
            &other,
            w.context(&bob, None).await
        )
        .await
        .is_committed()
    );

    // Alice invites Bob (signed in already), Carol, and Dana (new: she registers by accepting).
    let invite = |email: &'static str| {
        let as_alice = as_alice.clone();
        let t = t.clone();
        async move {
            committed(
                &w.send(
                    CreateInvitation {
                        email: email.into(),
                        groups: None,
                        owner: None,
                    },
                    &new_id(),
                    &t,
                    as_alice,
                )
                .await,
            );
            w.mailer.last(email).unwrap().token
        }
    };
    let bobs = invite("bob@example.com").await;
    // Carol may not use Bob's invitation: it is for another address.
    refused(
        api.accept_invitation(&bobs, Some(&carol), None).await,
        "wrong_email",
    );
    api.accept_invitation(&bobs, Some(&bob), None)
        .await
        .unwrap();
    // A used invitation is gone.
    assert!(
        api.accept_invitation(&bobs, Some(&bob), None)
            .await
            .is_err()
    );
    api.accept_invitation(&invite("carol@example.com").await, Some(&carol), None)
        .await
        .unwrap();
    let danas = invite("dana@example.com").await;
    // The token's secret is checked before the page learns the invited address.
    let (named, _) = danas.rsplit_once('.').unwrap();
    assert!(
        api.peek_invitation(&format!("{named}.not-the-secret"))
            .await
            .unwrap()
            .is_none()
    );
    // A refused registration does not spend the invitation.
    refused(
        api.accept_invitation(&danas, None, Some(("Dana", "short")))
            .await,
        "weak_password",
    );
    api.accept_invitation(&danas, None, Some(("Dana", PASSWORD)))
        .await
        .unwrap();
    let dana = api
        .sign_in("dana@example.com", PASSWORD, None, None)
        .await
        .expect("Dana signs in without verifying: the invitation proved her email");
    assert!(
        !w.context(&api.check(&dana.access_token).unwrap(), Some(&t))
            .await
            .sender_identity
            .is_empty()
    );
    // Members may not hand out owner invitations.
    let as_bob = w.context(&bob, Some(&t)).await;
    rejected(
        &w.send(
            CreateInvitation {
                email: "eve@example.com".into(),
                groups: None,
                owner: Some(true),
            },
            &new_id(),
            &t,
            as_bob.clone(),
        )
        .await,
        "forbidden",
    );
    let as_carol = w.context(&carol, Some(&t)).await;

    // Without a group, Bob may do nothing in the shop.
    rejected(
        &w.send(PlaceOrder {}, "o0", &t, as_bob.clone()).await,
        "forbidden",
    );

    // Alice (an owner) forms "customers": place orders, cancel only your own.
    let customers = new_id();
    let permissions = vec![
        Permission {
            pattern: "shop.PlaceOrder".into(),
            ..Permission::default()
        },
        Permission {
            pattern: "shop.CancelOrder".into(),
            when: Some("owner".into()),
            via_parent: None,
        },
    ];
    committed(
        &w.send(
            CreateGroup {
                name: "Customers".into(),
                description: None,
                permissions,
            },
            &customers,
            &t,
            as_alice.clone(),
        )
        .await,
    );
    for who in [&as_bob, &as_carol] {
        let grant = GrantMembership {
            membership_id: new_id(),
            group: customers.clone(),
            scope: None,
            expires_at: None,
        };
        committed(
            &w.send(grant, &who.sender_identity, &t, as_alice.clone())
                .await,
        );
    }

    // Bob places an order; Carol may not cancel it, Bob may.
    committed(&w.send(PlaceOrder {}, "o1", &t, as_bob.clone()).await);
    rejected(
        &w.send(CancelOrder {}, "o1", &t, as_carol.clone()).await,
        "forbidden",
    );
    committed(&w.send(CancelOrder {}, "o1", &t, as_bob.clone()).await);

    // No escalation: Bob may not grant memberships at all, nor hand out groups.
    let grant = GrantMembership {
        membership_id: new_id(),
        group: customers.clone(),
        scope: None,
        expires_at: None,
    };
    rejected(
        &w.send(grant, &as_carol.sender_identity, &t, as_bob.clone())
            .await,
        "forbidden",
    );

    // Nobody signed in: refused; another tenant's identity: refused.
    rejected(
        &w.send(PlaceOrder {}, "o2", &t, RequestContext::default())
            .await,
        "unauthenticated",
    );
    let wrong = RequestContext {
        sender_tenant: other.clone(),
        ..as_bob.clone()
    };
    rejected(&w.send(PlaceOrder {}, "o2", &t, wrong).await, "forbidden");

    // Signing out ends the session at once, although the access token is still valid.
    api.sign_out(&bob).await.unwrap();
    rejected(
        &w.send(PlaceOrder {}, "o3", &t, as_bob.clone()).await,
        "unauthenticated",
    );
    // Accepting an invitation checks the session like every signed-in call.
    refused(
        api.accept_invitation(&invite("bob@example.com").await, Some(&bob), None)
            .await,
        "unauthenticated",
    );

    // Lookups are the API's only: a signed-in person cannot ask for accounts by email.
    let q = Query::of(FindAccountByEmail {
        email: "alice@example.com".into(),
    })
    .tenant("platform")
    .context(as_alice.clone());
    assert_eq!(query_code::<Value>(&w.kit, q).await, "forbidden");
    Acme {
        tenant: t,
        alice: as_alice,
        bob_identity: as_bob.sender_identity,
    }
}

/// Sessions end at once, and a deleted tenant leaves no identity, invitation or slug behind.
async fn suspended_and_deleted(w: &World, acme: &Acme) {
    let api = &w.api;
    let admin = api
        .sign_in("admin@example.com", PASSWORD, None, None)
        .await
        .unwrap();
    let as_admin = w
        .context(&api.check(&admin.access_token).unwrap(), Some("platform"))
        .await;

    // An account the platform suspends is signed out at once, and cannot sign in.
    let (eve, eve_id) = w.person("eve@example.com").await;
    committed(
        &w.send(
            SuspendAccount {
                reason: kit::account::AccountSuspensionReason::Abuse,
            },
            &eve_id,
            "platform",
            as_admin.clone(),
        )
        .await,
    );
    let eves = new_id();
    rejected(
        &w.send(
            tenant("eves", "Eve's", &eve_id, None),
            &eves,
            &eves,
            w.context(&eve, None).await,
        )
        .await,
        "unauthenticated",
    );
    refused(
        api.sign_in("eve@example.com", PASSWORD, None, None).await,
        "not_active",
    );
    // Deleting it frees the address: Eve can register again.
    committed(
        &w.send(DeleteAccount {}, &eve_id, "platform", as_admin.clone())
            .await,
    );
    w.person("eve@example.com").await;

    // Alice suspends Acme for herself, the platform deletes it.
    let t = &acme.tenant;
    let open = new_id();
    committed(
        &w.send(
            CreateInvitation {
                email: "frank@example.com".into(),
                groups: None,
                owner: None,
            },
            &open,
            t,
            acme.alice.clone(),
        )
        .await,
    );
    committed(
        &w.send(
            SuspendTenant {
                reason: SuspensionReason::OwnerRequested,
            },
            t,
            t,
            acme.alice.clone(),
        )
        .await,
    );
    committed(&w.send(DeleteTenant {}, t, t, as_admin.clone()).await);

    // The cleanup erased Bob's identity and the open invitation (nothing left to delete or erase) ...
    let internal = RequestContext {
        bypass_authorization: true,
        ..RequestContext::default()
    };
    rejected(
        &w.send(DeleteIdentity {}, &acme.bob_identity, t, internal.clone())
            .await,
        "not_found",
    );
    rejected(
        &w.send(EraseInvitation {}, &open, t, internal.clone()).await,
        "not_found",
    );
    // ... the read side forgot the tenant, and its slug is free.
    let q = Query::of(GetTenant {
        tenant_id: t.clone(),
    })
    .tenant(t)
    .context(as_admin.clone());
    assert_eq!(query_code::<Value>(&w.kit, q).await, "not_found");
    let left: Vec<Value> = w
        .kit
        .ask(ListInvitations {}, |q| {
            q.tenant(t).context(internal.clone())
        })
        .await
        .unwrap()
        .into_iter()
        .map(|r| json!(r))
        .collect();
    assert!(left.is_empty());
    let bob = api
        .sign_in("bob@example.com", PASSWORD, None, None)
        .await
        .unwrap();
    let reuse = new_id();
    committed(
        &w.send(
            tenant("acme", "Acme again", &bob.account_id, None),
            &reuse,
            &reuse,
            w.context(&api.check(&bob.access_token).unwrap(), None)
                .await,
        )
        .await,
    );
}

/// The middleware resumes from its store after a restart, and decides as before.
async fn resumes_after_a_restart(w: &World) {
    let (grace, grace_id) = w.person("grace@example.com").await;
    let graces = new_id();
    committed(
        &w.send(
            tenant("graces", "Grace's", &grace_id, None),
            &graces,
            &graces,
            w.context(&grace, None).await,
        )
        .await,
    );
    let as_grace = w.context(&grace, Some(&graces)).await;
    committed(&w.send(PlaceOrder {}, "g1", &graces, as_grace.clone()).await);

    w.stop_app();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // The knowledge is in the store file, with the position it continues from.
    {
        let db = rusqlite::Connection::open_with_flags(
            w.kernel.dir.join("authorization.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let position: String = db
            .query_row("SELECT value FROM meta WHERE key = 'position'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(position.parse::<u64>().unwrap() > 0, "a stored position");
        let stored: String = db
            .query_row(
                "SELECT value FROM data WHERE ns = 'tenants' AND key = ?",
                [&graces],
                |r| r.get(0),
            )
            .unwrap();
        let tenant: Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(
            tenant["owners"].as_array().unwrap().len(),
            1,
            "Grace's tenant and its owner in the store"
        );
    }
    w.start_app().await;
    // The owner still may, an ended session still may not: both from the stored knowledge.
    committed(&w.send(PlaceOrder {}, "g2", &graces, as_grace.clone()).await);
    w.api.sign_out(&grace).await.unwrap();
    rejected(
        &w.send(PlaceOrder {}, "g3", &graces, as_grace).await,
        "unauthenticated",
    );
}

/// Refresh and reuse, password change and reset, email change, a taken address.
async fn account_flows(w: &World) {
    let api = &w.api;
    let email = "frank@example.com";
    w.person(email).await;
    let sign_in = |address: &'static str, password: &'static str| async move {
        api.sign_in(address, password, None, None).await.unwrap()
    };
    let (one, two, three) = (
        sign_in(email, PASSWORD).await,
        sign_in(email, PASSWORD).await,
        sign_in(email, PASSWORD).await,
    );

    // A refresh rotates the token; the old one again is a reuse and ends the session.
    let (access, rotated) = api.refresh(&one.refresh_token, None).await.unwrap();
    assert_eq!(api.check(&access).unwrap().sid, one.session_id);
    // Knowing the session's ID is not enough to end it: a made-up older token is refused, nothing more.
    refused(
        api.refresh(&format!("{}.1.made-up-secret", one.session_id), None)
            .await,
        "invalid_token",
    );
    refused(
        api.refresh(&format!("{}.x.made-up-secret", one.session_id), None)
            .await,
        "invalid_token",
    );
    refused(api.refresh(&one.refresh_token, None).await, "reuse");
    assert!(
        api.refresh(&rotated, None).await.is_err(),
        "the session ended with the reuse"
    );

    // A password change keeps its own session and ends the others.
    let claims_two = api.check(&two.access_token).unwrap();
    refused(
        api.change_password(
            &claims_two,
            Some("wrong password!"),
            "another horse battery",
        )
        .await,
        "wrong_password",
    );
    api.change_password(&claims_two, Some(PASSWORD), "another horse battery")
        .await
        .unwrap();
    let (_, two_again) = api
        .refresh(&two.refresh_token, None)
        .await
        .expect("the changing session stays");
    assert!(
        api.refresh(&three.refresh_token, None).await.is_err(),
        "the other session ended"
    );
    sign_in(email, "another horse battery").await;

    // A reset by mail sets a new password and ends every session; asking for nobody sends nothing.
    let before = w.mailer.sent.lock().unwrap().len();
    api.request_password_reset("nobody@example.com")
        .await
        .unwrap();
    assert_eq!(w.mailer.sent.lock().unwrap().len(), before);
    api.request_password_reset(email).await.unwrap();
    let reset = w.mailer.last(email).unwrap();
    assert_eq!(reset.purpose, Purpose::Reset);
    // Asking again at once mails nothing more: one mail per cooldown.
    let mailed = w.mailer.sent.lock().unwrap().len();
    api.request_password_reset(email).await.unwrap();
    assert_eq!(w.mailer.sent.lock().unwrap().len(), mailed);
    api.reset_password(&reset.token, "third horse battery")
        .await
        .unwrap();
    assert!(
        api.refresh(&two_again, None).await.is_err(),
        "every session ended"
    );
    refused(
        api.sign_in(email, "another horse battery", None, None)
            .await,
        "wrong_password",
    );
    let four = sign_in(email, "third horse battery").await;

    // A new address is confirmed from that address; the old one then signs in no more.
    api.request_email_change(&api.check(&four.access_token).unwrap(), "frank@new.example")
        .await
        .unwrap();
    let change = w.mailer.last("frank@new.example").unwrap();
    assert_eq!(change.purpose, Purpose::EmailChange);
    api.confirm_email_change(&change.token).await.unwrap();
    sign_in("frank@new.example", "third horse battery").await;
    refused(
        api.sign_in(email, "third horse battery", None, None).await,
        "wrong_password",
    );

    // Registering with a taken address answers as usual; the account's owner gets a mail.
    api.register("frank@new.example", "Not Frank", Some(PASSWORD))
        .await
        .unwrap();
    assert_eq!(
        w.mailer.last("frank@new.example").unwrap().purpose,
        Purpose::AccountExists
    );
    // Again at once: the same answer, and no second mail within the cooldown.
    let told = w.mailer.sent.lock().unwrap().len();
    api.register("frank@new.example", "Not Frank", Some(PASSWORD))
        .await
        .unwrap();
    assert_eq!(w.mailer.sent.lock().unwrap().len(), told);

    // Five wrong passwords lock the account; a locked account answers like a wrong password, even
    // for the right one, so the answer tells nobody that the account exists.
    w.person("gus@example.com").await;
    for _ in 0..5 {
        refused(
            api.sign_in("gus@example.com", "wrong horse battery", None, None)
                .await,
            "wrong_password",
        );
    }
    refused(
        api.sign_in("gus@example.com", PASSWORD, None, None).await,
        "wrong_password",
    );
    refused(
        api.sign_in("nobody-at-all@example.com", PASSWORD, None, None)
            .await,
        "wrong_password",
    );
}

/// A second factor: TOTP with recovery codes; and a service with an API key.
async fn second_factor_and_api_keys(w: &World) {
    let api = &w.api;
    let email = "henry@example.com";
    let (first, _) = w.person(email).await;
    let (secret, uri) = api.start_totp(&first, "Kit", email).await.unwrap();
    assert!(uri.starts_with("otpauth://totp/"));
    let step = kernable::Timestamp::now().millis() / 30_000;
    let codes = api
        .confirm_totp(&first, &totp_code(&secret, step))
        .await
        .unwrap();
    assert_eq!(codes.len(), 10);

    // Signing in now needs the second factor before anything else.
    let signed = api.sign_in(email, PASSWORD, None, None).await.unwrap();
    assert!(signed.second_factor);
    let claims = api.check(&signed.access_token).unwrap();
    let henrys = new_id();
    let open = || async {
        w.send(
            tenant("henrys", "Henry's", &claims.sub, None),
            &henrys,
            &henrys,
            w.context(&claims, None).await,
        )
        .await
    };
    rejected(&open().await, "mfa_required");
    // Joining through a link checks the session too: the token parses, the second factor is missing.
    refused(
        api.join_with_invitation_link(&claims, &format!("link_{henrys}.l1.{}", "A".repeat(32)))
            .await,
        "mfa_required",
    );
    // The confirming code counts once; the next step's code, or a recovery code, proves it.
    refused(
        api.prove_second_factor(&claims, &totp_code(&secret, step))
            .await,
        "wrong_code",
    );
    api.prove_second_factor(&claims, &codes[0]).await.unwrap();
    committed(&open().await);
    let again = api
        .check(
            &api.sign_in(email, PASSWORD, None, None)
                .await
                .unwrap()
                .access_token,
        )
        .unwrap();
    api.prove_second_factor(&again, &totp_code(&secret, step + 1))
        .await
        .unwrap();
    refused(
        api.prove_second_factor(&again, &codes[0]).await,
        "wrong_code",
    );

    // Henry, now an owner, gives a service an API key, and a group to act in.
    let as_henry = w.context(&claims, Some(&henrys)).await;
    let service = new_id();
    committed(
        &w.send(
            CreateServiceIdentity {
                display_name: "Billing".into(),
            },
            &service,
            &henrys,
            as_henry.clone(),
        )
        .await,
    );
    let orders = new_id();
    let permissions = vec![Permission {
        pattern: "shop.PlaceOrder".into(),
        ..Permission::default()
    }];
    committed(
        &w.send(
            CreateGroup {
                name: "Orders".into(),
                description: None,
                permissions,
            },
            &orders,
            &henrys,
            as_henry.clone(),
        )
        .await,
    );
    let grant = GrantMembership {
        membership_id: "svc".into(),
        group: orders,
        scope: None,
        expires_at: None,
    };
    committed(&w.send(grant, &service, &henrys, as_henry.clone()).await);
    let (key, key_id) = api
        .create_api_key(as_henry.clone(), &henrys, &service, "billing", None)
        .await
        .unwrap();
    let as_service = api.check_api_key(&key).await.unwrap().unwrap();
    assert_eq!(
        (
            as_service.sender_tenant.as_str(),
            as_service.sender_identity.as_str(),
            as_service.sender_session.as_str()
        ),
        (henrys.as_str(), service.as_str(), key_id.as_str())
    );
    assert!(
        api.check_api_key(&format!("{key}x"))
            .await
            .unwrap()
            .is_none(),
        "a wrong key"
    );
    committed(
        &w.send(PlaceOrder {}, "h1", &henrys, as_service.clone())
            .await,
    );

    // Revoked: refused at once, even with the API's cached context.
    committed(
        &w.send(RevokeApiKey { key_id }, &service, &henrys, as_henry)
            .await,
    );
    rejected(
        &w.send(PlaceOrder {}, "h2", &henrys, as_service).await,
        "unauthenticated",
    );
}

/// Passkeys: added against an issued challenge, signing in without a password, a challenge counting once.
async fn passkeys(w: &World) {
    let api = &w.api;
    let email = "ivy@example.com";
    let (ivy, ivy_id) = w.person(email).await;
    let mut device = Authenticator::new("localhost", "http://localhost:3000", &ivy_id);
    let creation = api.passkey_registration_options(&ivy, email).await.unwrap();
    refused(
        api.add_passkey(&ivy, device.register("bm90LWlzc3VlZA"), None)
            .await,
        "invalid_challenge",
    );
    api.add_passkey(
        &ivy,
        device.register(creation["challenge"].as_str().unwrap()),
        Some("Laptop"),
    )
    .await
    .unwrap();

    // Without a password: the passkey names the account and counts as two factors.
    let request = api.passkey_sign_in_options().await.unwrap();
    let answer = device.assert(request["challenge"].as_str().unwrap(), None);
    let signed = api
        .sign_in_with_passkey(answer.clone(), None)
        .await
        .unwrap();
    assert_eq!(signed.account_id, ivy_id);
    assert_eq!(
        api.check(&signed.access_token).unwrap().fac,
        vec!["passkey".to_owned()]
    );
    // The same answer again: its challenge is used up.
    refused(
        api.sign_in_with_passkey(answer, None).await,
        "wrong_password",
    );
    // A passkey answer from another site's page fails.
    let other = api.passkey_sign_in_options().await.unwrap();
    assert!(
        api.sign_in_with_passkey(
            device.assert(
                other["challenge"].as_str().unwrap(),
                Some("http://evil.example")
            ),
            None
        )
        .await
        .is_err()
    );

    // The passkey session acts like any other.
    let claims = api.check(&signed.access_token).unwrap();
    let ivys = new_id();
    committed(
        &w.send(
            tenant("ivys", "Ivy's", &claims.sub, None),
            &ivys,
            &ivys,
            w.context(&claims, None).await,
        )
        .await,
    );
}

/// OAuth and SSO: a social login registers or signs in, links on request; a tenant's SSO verified by DNS and enforced.
async fn oauth_and_sso(w: &World) {
    let social = OidcProvider::start().await;
    let company = OidcProvider::start().await;
    let txt: Arc<Mutex<HashMap<String, String>>> = Arc::default();
    let records = Arc::clone(&txt);
    let sso = KitApi::new(
        w.kit.clone(),
        new_signing_keys(),
        &w.service.0,
        &w.service.1,
    )
    .provider(
        "social",
        Provider {
            issuer: social.issuer.clone(),
            client_id: social.client_id.clone(),
            client_secret: social.client_secret.clone(),
            oauth2: None,
        },
    )
    .redirect_uri("http://localhost:3000/callback")
    .insecure_issuers()
    .resolve_txt(move |name| {
        let found: Vec<String> = records
            .lock()
            .unwrap()
            .get(&name)
            .cloned()
            .into_iter()
            .collect();
        async move { found }
    });
    let start =
        |provider: Option<&'static str>, email: Option<&'static str>, link: Option<Claims>| {
            let sso = sso.clone();
            async move {
                sso.oidc_start(provider, email, link.as_ref())
                    .await
                    .unwrap()
            }
        };

    // A social login of a new person registers and links them; the second time signs them in.
    let jack = json!({ "sub": "s-jack", "email": "jack@example.com", "email_verified": true });
    let first = sso
        .oidc_finish(
            &social.login(&start(Some("social"), None, None).await, jack.clone()),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let second = sso
        .oidc_finish(
            &social.login(&start(Some("social"), None, None).await, jack.clone()),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.account_id, first.account_id);
    // A callback used once does not work twice.
    let url = social.login(&start(Some("social"), None, None).await, jack.clone());
    sso.oidc_finish(&url, None).await.unwrap();
    refused(sso.oidc_finish(&url, None).await, "invalid_state");

    // A social login whose email has an account is not linked by itself: sign in and link it.
    let (kate, kate_id) = w.person("kate@example.com").await;
    let kate_social =
        json!({ "sub": "s-kate", "email": "kate@example.com", "email_verified": true });
    refused(
        sso.oidc_finish(
            &social.login(
                &start(Some("social"), None, None).await,
                kate_social.clone(),
            ),
            None,
        )
        .await,
        "not_linked",
    );
    assert!(
        sso.oidc_finish(
            &social.login(
                &start(Some("social"), None, Some(kate)).await,
                kate_social.clone()
            ),
            None
        )
        .await
        .unwrap()
        .is_none()
    );
    let kate_in = sso
        .oidc_finish(
            &social.login(&start(Some("social"), None, None).await, kate_social),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kate_in.account_id, kate_id);

    // With sign-up by invitation, a social login registers nobody; one already linked signs in.
    let policy = |accounts| {
        Submit::command(
            ChangeSignUpPolicy {
                tenants: TenantSignUp::Active,
                accounts,
            },
            PLATFORM,
        )
        .id(new_id())
        .tenant(PLATFORM)
        .context(RequestContext {
            bypass_authorization: true,
            ..RequestContext::default()
        })
    };
    committed(&w.kit.submit(policy(AccountSignUp::Off)).await.unwrap());
    let nora = json!({ "sub": "s-nora", "email": "nora@example.com", "email_verified": true });
    refused(
        sso.oidc_finish(
            &social.login(&start(Some("social"), None, None).await, nora),
            None,
        )
        .await,
        "sign_up_closed",
    );
    let again = sso
        .oidc_finish(
            &social.login(&start(Some("social"), None, None).await, jack.clone()),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.account_id, first.account_id);
    committed(&w.kit.submit(policy(AccountSignUp::Open)).await.unwrap());

    // Liam's tenant sets up SSO: the domain proven by DNS, then enforced.
    let (liam, liam_id) = w.person("liam@lumen.example").await;
    let lumen = new_id();
    committed(
        &w.send(
            tenant("lumen", "Lumen", &liam_id, None),
            &lumen,
            &lumen,
            w.context(&liam, None).await,
        )
        .await,
    );
    let as_liam = w.context(&liam, Some(&lumen)).await;
    let sso_id = new_id();
    let create = CreateSso {
        issuer: company.issuer.clone(),
        client_id: company.client_id.clone(),
        client_secret: company.client_secret.clone(),
    };
    committed(&w.send(create, &sso_id, &lumen, as_liam.clone()).await);
    committed(
        &w.send(
            AddSsoDomain {
                domain: "lumen.example".into(),
            },
            &sso_id,
            &lumen,
            as_liam.clone(),
        )
        .await,
    );
    refused(
        sso.verify_sso_domain(&lumen, &sso_id, "lumen.example")
            .await,
        "not_verified",
    );
    let config: Value = w
        .kit
        .ask(
            GetSso {
                sso_id: sso_id.clone(),
            },
            |q| q.tenant(&lumen).context(as_liam.clone()),
        )
        .await
        .unwrap();
    txt.lock().unwrap().insert(
        "_kernable-sso.lumen.example".into(),
        config["domains"][0]["txt"].as_str().unwrap().into(),
    );
    sso.verify_sso_domain(&lumen, &sso_id, "lumen.example")
        .await
        .unwrap();
    committed(
        &w.send(
            EnforceSso { enforced: true },
            &sso_id,
            &lumen,
            as_liam.clone(),
        )
        .await,
    );

    // Mia of lumen.example has a password; in Lumen only an SSO session counts, from accepting on.
    let (mia, mia_id) = w.person("mia@lumen.example").await;
    w.send(
        CreateInvitation {
            email: "mia@lumen.example".into(),
            groups: None,
            owner: None,
        },
        &new_id(),
        &lumen,
        as_liam.clone(),
    )
    .await;
    let mias_invitation = w.mailer.last("mia@lumen.example").unwrap().token;
    refused(
        w.api
            .accept_invitation(&mias_invitation, Some(&mia), None)
            .await,
        "sso_required",
    );
    // Through the company's provider: linked to her account by the verified domain and email.
    let mia_user = json!({ "sub": "c-mia", "email": "mia@lumen.example", "email_verified": true });
    let mia_sso = sso
        .oidc_finish(
            &company.login(
                &start(None, Some("mia@lumen.example"), None).await,
                mia_user,
            ),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mia_sso.account_id, mia_id);
    let mia_sso_claims = sso.check(&mia_sso.access_token).unwrap();
    w.api
        .accept_invitation(&mias_invitation, Some(&mia_sso_claims), None)
        .await
        .unwrap();
    let as_mia = w.context(&mia, Some(&lumen)).await;
    let own = |context: RequestContext| {
        let lumen = lumen.clone();
        async move {
            w.kit
                .ask(
                    GetIdentity {
                        identity_id: context.sender_identity.clone(),
                    },
                    |q| q.tenant(&lumen).context(context),
                )
                .await
        }
    };
    match own(as_mia.clone()).await {
        Err(Error::Rejected(r)) => assert_eq!(r.code, "sso_required"),
        other => panic!("expected sso_required, got {:?}", other.map(|r| r.id)),
    }
    let as_mia_sso = w
        .api
        .context_for(&mia_sso_claims, Some(&lumen))
        .await
        .unwrap();
    assert_eq!(own(as_mia_sso).await.unwrap().id, as_mia.sender_identity);
    // The owner keeps a password session, so a broken SSO can be mended.
    assert!(!own(as_liam.clone()).await.unwrap().id.is_empty());
    // So does a new owner: an owner invitation is accepted with a password session.
    let (nils, nils_id) = w.person("nils@lumen.example").await;
    committed(
        &w.send(
            CreateInvitation {
                email: "nils@lumen.example".into(),
                groups: None,
                owner: Some(true),
            },
            &new_id(),
            &lumen,
            as_liam.clone(),
        )
        .await,
    );
    let nils_invitation = w.mailer.last("nils@lumen.example").unwrap().token;
    assert_eq!(
        w.api
            .accept_invitation(&nils_invitation, Some(&nils), None)
            .await
            .unwrap(),
        (nils_id, lumen.clone())
    );

    // Lumen's provider speaks only for lumen.example: it cannot take over Kate's account by naming
    // her verified address, nor register someone of another domain.
    let takeover = json!({ "sub": "c-evil", "email": "kate@example.com", "email_verified": true });
    refused(
        sso.oidc_finish(
            &company.login(
                &start(None, Some("mia@lumen.example"), None).await,
                takeover,
            ),
            None,
        )
        .await,
        "email_not_in_domain",
    );
    let stranger =
        json!({ "sub": "c-zed", "email": "zed@elsewhere.example", "email_verified": true });
    refused(
        sso.oidc_finish(
            &company.login(
                &start(None, Some("mia@lumen.example"), None).await,
                stranger,
            ),
            None,
        )
        .await,
        "email_not_in_domain",
    );
    let zed = w
        .kit
        .ask(
            FindAccountByEmail {
                email: "zed@elsewhere.example".into(),
            },
            |q| q.tenant(PLATFORM).context(sso.service_context.clone()),
        )
        .await;
    match zed {
        Err(Error::Rejected(r)) => assert_eq!(r.code, "not_found"),
        other => panic!("expected not_found, got {other:?}"),
    }
}

/// Lists show what one may see; Explain says why a request is allowed or refused.
async fn lists_and_explain(w: &World) {
    let api = &w.api;
    let (nora, nora_id) = w.person("nora@example.com").await;
    let t = new_id();
    committed(
        &w.send(
            tenant("noras", "Nora's", &nora_id, None),
            &t,
            &t,
            w.context(&nora, None).await,
        )
        .await,
    );
    let as_nora = w.context(&nora, Some(&t)).await;
    let buyers = new_id();
    let permissions = vec![
        Permission {
            pattern: "shop.PlaceOrder".into(),
            ..Permission::default()
        },
        Permission {
            pattern: "shop.ListOrders".into(),
            ..Permission::default()
        },
        Permission {
            pattern: "shop.GetOrder".into(),
            when: Some("owner".into()),
            via_parent: None,
        },
    ];
    committed(
        &w.send(
            CreateGroup {
                name: "Buyers".into(),
                description: None,
                permissions,
            },
            &buyers,
            &t,
            as_nora.clone(),
        )
        .await,
    );
    let join = |email: &'static str| {
        let (as_nora, t, buyers) = (as_nora.clone(), t.clone(), buyers.clone());
        async move {
            let (who, _) = w.person(email).await;
            let invite = CreateInvitation {
                email: email.into(),
                groups: Some(vec![InvitedGroup {
                    group: buyers,
                    scope: None,
                }]),
                owner: None,
            };
            w.send(invite, &new_id(), &t, as_nora).await;
            api.accept_invitation(&w.mailer.last(email).unwrap().token, Some(&who), None)
                .await
                .unwrap();
            w.context(&who, Some(&t)).await
        }
    };
    let as_oscar = join("oscar@example.com").await;
    let as_pia = join("pia@example.com").await;
    for (id, who) in [("o-oscar", &as_oscar), ("o-pia", &as_pia)] {
        committed(&w.send(PlaceOrder {}, id, &t, who.clone()).await);
    }
    let list = |context: RequestContext| {
        let t = t.clone();
        async move {
            w.kit
                .ask(ListOrders {}, |q| q.tenant(&t).context(context))
                .await
                .unwrap()
        }
    };
    assert_eq!(list(as_oscar.clone()).await, vec!["o-oscar"]);
    assert_eq!(list(as_pia.clone()).await, vec!["o-pia"]);
    assert_eq!(
        list(as_nora.clone()).await,
        vec!["o-oscar", "o-pia"],
        "the owner sees all"
    );

    // Explain, for oneself: with one's own session; the deciding membership named.
    let explain = |identity: String,
                   context: RequestContext,
                   r#type: &'static str,
                   order: Option<&'static str>| {
        let t = t.clone();
        async move {
            let q = Explain {
                identity_id: identity,
                r#type: r#type.into(),
                aggregate_type: order.map(|_| "order".into()),
                aggregate_id: order.map(Into::into),
                data: None,
            };
            w.kit.ask(q, |q| q.tenant(&t).context(context)).await
        }
    };
    let mine = explain(
        as_oscar.sender_identity.clone(),
        as_oscar.clone(),
        "shop.GetOrder",
        Some("o-oscar"),
    )
    .await
    .unwrap();
    assert!(mine.allowed);
    let last = mine.steps.last().unwrap();
    assert!(
        last.starts_with("allowed: membership ") && last.contains(": shop.GetOrder when owner"),
        "{last}"
    );
    let theirs = explain(
        as_oscar.sender_identity.clone(),
        as_oscar.clone(),
        "shop.GetOrder",
        Some("o-pia"),
    )
    .await
    .unwrap();
    assert_eq!(
        (theirs.allowed, theirs.code.as_deref()),
        (false, Some("forbidden"))
    );
    // For someone else: the owner may ask, a member may not.
    let for_pia = explain(
        as_pia.sender_identity.clone(),
        as_nora.clone(),
        "shop.PlaceOrder",
        None,
    )
    .await
    .unwrap();
    assert!(
        for_pia.allowed && for_pia.steps[0].starts_with("sessions not checked"),
        "{for_pia:?}"
    );
    match explain(
        as_pia.sender_identity.clone(),
        as_oscar.clone(),
        "shop.PlaceOrder",
        None,
    )
    .await
    {
        Err(Error::Rejected(r)) => assert_eq!(r.code, "forbidden"),
        other => panic!("expected forbidden, got {other:?}"),
    }
}

/// The example server: register, verify, sign in, and a command as the caller.
async fn example_server(w: &World) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    // As the example's main does: its client knows what the kit's queries asked by name are about.
    let kernable = w.kit.clone();
    for d in &kit::kit_domains() {
        kernable.with_domain(d);
    }
    let router = server::router(w.api.clone(), kernable);
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let http = reqwest::Client::new();
    let post = |path: &'static str, body: Value, bearer: Option<String>| {
        let (http, base) = (http.clone(), base.clone());
        async move {
            let mut request = http.post(format!("{base}{path}")).json(&body);
            if let Some(token) = bearer {
                request = request.header("authorization", format!("Bearer {token}"));
            }
            let res = request.send().await.unwrap();
            let status = res.status().as_u16();
            let text = res.text().await.unwrap();
            (
                status,
                if text.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(&text).unwrap()
                },
            )
        }
    };
    assert_eq!(
        post(
            "/register",
            json!({ "email": "quinn@example.com", "name": "Quinn", "password": PASSWORD }),
            None
        )
        .await
        .0,
        202
    );
    assert_eq!(
        post(
            "/verify",
            json!({ "token": w.mailer.last("quinn@example.com").unwrap().token }),
            None
        )
        .await
        .0,
        200
    );
    let (status, tokens) = post(
        "/sign-in",
        json!({ "email": "quinn@example.com", "password": PASSWORD }),
        None,
    )
    .await;
    assert_eq!(status, 200);
    let access = tokens["accessToken"].as_str().unwrap().to_owned();
    // Outside a tenant, Quinn has no identity: the shop refuses.
    assert_eq!(
        post(
            "/commands/shop/order/PlaceOrder",
            json!({ "aggregateId": "q1" }),
            Some(access.clone())
        )
        .await,
        (403, json!({ "error": "forbidden" }))
    );
    assert_eq!(
        post(
            "/commands/shop/order/PlaceOrder",
            json!({ "aggregateId": "q1" }),
            None
        )
        .await
        .0,
        401
    );
    // Account requests go to the platform tenant, where accounts live, whatever tenant the caller is in.
    let account_id = w.api.check(&access).unwrap().sub;
    let (own, account): (u16, Value) = post(
        "/queries/account/GetAccount",
        json!({ "data": { "accountId": account_id } }),
        Some(access.clone()),
    )
    .await;
    assert_eq!(
        (own, account["email"].clone()),
        (200, json!("quinn@example.com"))
    );
    assert_eq!(post("/sign-out", json!({}), Some(access)).await.0, 204);
    assert_eq!(
        post(
            "/refresh",
            json!({ "refreshToken": tokens["refreshToken"] }),
            None
        )
        .await
        .0,
        401,
        "signed out"
    );
    task.abort();
}

/// Invitation links let people join with groups; a query about one order is decided on that order.
/// Accepting and joining check the tenant joined too: suspended, a second factor required.
async fn joining_checks_the_tenant(w: &World) {
    let api = &w.api;
    let (tara, tara_id) = w.person("join-tara@example.com").await;
    let taras = new_id();
    committed(
        &w.send(
            tenant("taras", "Tara's", &tara_id, Some(TenantStatus::Active)),
            &taras,
            &taras,
            w.context(&tara, None).await,
        )
        .await,
    );
    let as_tara = w.context(&tara, Some(&taras)).await;
    let (ugo, _) = w.person("join-ugo@example.com").await;
    let (link, _) = api
        .create_invitation_link(as_tara.clone(), &taras, NewInvitationLink::default())
        .await
        .unwrap();
    let invite = CreateInvitation {
        email: "join-ugo@example.com".into(),
        groups: None,
        owner: None,
    };
    committed(&w.send(invite, &new_id(), &taras, as_tara.clone()).await);
    let invitation = w.mailer.last("join-ugo@example.com").unwrap().token;

    // The tenant requires a second factor: Ugo's password session neither joins nor accepts.
    let mfa = |required| RequireMfa { required };
    committed(&w.send(mfa(true), &taras, &taras, as_tara.clone()).await);
    refused(
        api.join_with_invitation_link(&ugo, &link).await,
        "mfa_required",
    );
    refused(
        api.accept_invitation(&invitation, Some(&ugo), None).await,
        "mfa_required",
    );
    // (Tara herself has one factor only now: the platform lifts it.)
    let platform = RequestContext {
        bypass_authorization: true,
        ..RequestContext::default()
    };
    committed(&w.send(mfa(false), &taras, &taras, platform).await);

    // Suspended: nobody joins it, not even someone new, who would register in the same step.
    let invite_vera = CreateInvitation {
        email: "join-vera@example.com".into(),
        groups: None,
        owner: None,
    };
    committed(
        &w.send(invite_vera, &new_id(), &taras, as_tara.clone())
            .await,
    );
    let vera = w.mailer.last("join-vera@example.com").unwrap().token;
    let suspend = SuspendTenant {
        reason: SuspensionReason::OwnerRequested,
    };
    committed(&w.send(suspend, &taras, &taras, as_tara.clone()).await);
    refused(
        api.join_with_invitation_link(&ugo, &link).await,
        "tenant_suspended",
    );
    refused(
        api.accept_invitation(&invitation, Some(&ugo), None).await,
        "tenant_suspended",
    );
    refused(
        api.accept_invitation(&vera, None, Some(("Vera", PASSWORD)))
            .await,
        "tenant_not_active",
    );
    let nobody = Query::of(FindAccountByEmail {
        email: "join-vera@example.com".into(),
    })
    .tenant(kit::shared::constants::PLATFORM_TENANT)
    .context(api.service_context.clone());
    assert_eq!(
        query_code::<Value>(&w.kit, nobody).await,
        "not_found",
        "nobody registered"
    );
    committed(&w.send(ReactivateTenant {}, &taras, &taras, as_tara).await);

    // Active again, without the requirement: Ugo accepts; Vera's invitation is still open.
    api.accept_invitation(&invitation, Some(&ugo), None)
        .await
        .unwrap();
    api.accept_invitation(&vera, None, Some(("Vera", PASSWORD)))
        .await
        .unwrap();
}

async fn invitation_links(w: &World) {
    let api = &w.api;
    let (olga, olga_id) = w.person("link-olga@example.com").await;
    let shop_tenant = new_id();
    committed(
        &w.send(
            tenant(
                "olgas-shop",
                "Olga's shop",
                &olga_id,
                Some(TenantStatus::Active),
            ),
            &shop_tenant,
            &shop_tenant,
            w.context(&olga, None).await,
        )
        .await,
    );
    let as_olga = w.context(&olga, Some(&shop_tenant)).await;
    let customers = new_id();
    let permissions = vec![
        Permission {
            pattern: "shop.PlaceOrder".into(),
            ..Permission::default()
        },
        Permission {
            pattern: "shop.GetOrder".into(),
            when: Some("owner".into()),
            via_parent: None,
        },
    ];
    committed(
        &w.send(
            CreateGroup {
                name: "Customers".into(),
                description: None,
                permissions,
            },
            &customers,
            &shop_tenant,
            as_olga.clone(),
        )
        .await,
    );

    // Olga puts a link on her website: two people may join through it, as customers.
    let into_customers = || {
        vec![InvitedGroup {
            group: customers.clone(),
            scope: None,
        }]
    };
    let (token, link_id) = api
        .create_invitation_link(
            as_olga.clone(),
            &shop_tenant,
            NewInvitationLink {
                groups: into_customers(),
                label: Some("website".into()),
                max_uses: Some(2),
                ..NewInvitationLink::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        api.peek_invitation_link(&token).await.unwrap(),
        Some(PeekedLink {
            tenant: shop_tenant.clone(),
            link_id: link_id.clone(),
            label: "website".into(),
            usable: true,
        })
    );

    let (pia, _) = w.person("link-pia@example.com").await;
    let (quinn, _) = w.person("link-quinn@example.com").await;
    let (rosa, _) = w.person("link-rosa@example.com").await;
    assert_eq!(
        api.join_with_invitation_link(&pia, &token).await.unwrap(),
        shop_tenant
    );
    refused(api.join_with_invitation_link(&pia, &token).await, "exists");
    assert_eq!(
        api.join_with_invitation_link(&quinn, &token).await.unwrap(),
        shop_tenant
    );
    refused(
        api.join_with_invitation_link(&rosa, &token).await,
        "used_up",
    );
    // Joining checks the session like every signed-in call: an ended one joins nothing.
    let (sven, _) = w.person("link-sven@example.com").await;
    api.sign_out(&sven).await.unwrap();
    refused(
        api.join_with_invitation_link(&sven, &token).await,
        "unauthenticated",
    );
    assert!(
        !api.peek_invitation_link(&token)
            .await
            .unwrap()
            .unwrap()
            .usable
    );
    // A link's token is checked whole: another secret for the same link is refused.
    let mut other = token.clone();
    let last = other.pop().unwrap();
    other.push(if last == 'A' { 'B' } else { 'A' });
    assert_eq!(
        api.peek_invitation_link(&other).await.unwrap(),
        None,
        "only the whole token is peeked"
    );
    refused(
        api.join_with_invitation_link(&rosa, &other).await,
        "invalid_token",
    );

    // Pia and Quinn are customers now (the reactor made their identities): Pia places an order
    // and reads it; Quinn may not read it.
    let as_pia = w.context(&pia, Some(&shop_tenant)).await;
    let as_quinn = w.context(&quinn, Some(&shop_tenant)).await;
    committed(
        &w.send(PlaceOrder {}, "p1", &shop_tenant, as_pia.clone())
            .await,
    );
    let get = |context: RequestContext| {
        Query::of(GetOrder { id: "p1".into() })
            .tenant(&shop_tenant)
            .context(context)
    };
    assert_eq!(
        w.kit
            .query::<OrderDetails>(get(as_pia.clone()))
            .await
            .unwrap(),
        OrderDetails {
            customer: as_pia.sender_identity.clone()
        }
    );
    assert_eq!(
        query_code::<OrderDetails>(&w.kit, get(as_quinn.clone())).await,
        "forbidden"
    );

    // Explain says the same for a query about one order.
    let why = w
        .kit
        .ask(
            Explain {
                identity_id: as_quinn.sender_identity.clone(),
                r#type: "shop.GetOrder".into(),
                aggregate_type: Some("order".into()),
                aggregate_id: Some("p1".into()),
                data: None,
            },
            |q| q.tenant(&shop_tenant).context(as_olga.clone()),
        )
        .await
        .unwrap();
    assert!(!why.allowed);

    // Customers may not hand out links: no permission for it, and no escalation either.
    refused(
        api.create_invitation_link(
            as_pia.clone(),
            &shop_tenant,
            NewInvitationLink {
                groups: into_customers(),
                ..NewInvitationLink::default()
            },
        )
        .await,
        "forbidden",
    );

    // Revoked: the link is gone.
    let (second, second_id) = api
        .create_invitation_link(
            as_olga.clone(),
            &shop_tenant,
            NewInvitationLink {
                groups: into_customers(),
                ..NewInvitationLink::default()
            },
        )
        .await
        .unwrap();
    committed(
        &w.send(
            RevokeInvitationLink {},
            &second_id,
            &shop_tenant,
            as_olga.clone(),
        )
        .await,
    );
    assert_eq!(api.peek_invitation_link(&second).await.unwrap(), None);
    refused(
        api.join_with_invitation_link(&rosa, &second).await,
        "invalid_token",
    );
}

/// A registered process gets an identity in the platform tenant and an API key, and acts as them:
/// refused (an incident) until a platform group lets it, then a retry goes through.
async fn a_process_acts_as_its_own_identity(w: &World) {
    let kit = RequestContext {
        bypass_authorization: true,
        ..RequestContext::default()
    };
    let t = new_id();
    let order = new_id();
    // The kit makes the process's identity and key when the process is registered, a moment
    // later: a case started before that would be refused as unauthenticated, not forbidden.
    for _ in 0..500 {
        let row = w
            .kit
            .query::<Value>(
                Query::of(GetIdentity {
                    identity_id: "process-shop.flagged".into(),
                })
                .tenant(PLATFORM)
                .context(kit.clone()),
            )
            .await;
        if row.is_ok_and(|r| r["apiKeys"].as_array().map(Vec::len) == Some(1)) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // Settled: the reactors handled everything before it, the process's registration too.
    committed(&w.send(PlaceOrder {}, &order, &t, kit.clone()).await);
    committed(&w.send(FlagOrder {}, &order, &t, kit.clone()).await);
    let case = format!("{order}#1");
    let state = |state: &'static str| {
        let case = case.clone();
        async move {
            for _ in 0..500 {
                let (ok, out) = w.kernel.cli(&[
                    "processes",
                    "cases",
                    "{store}",
                    "shop.flagged",
                    "--state",
                    state,
                ]);
                if ok && out.contains(&case) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            panic!("case {case} never {state}");
        }
    };
    state("incident").await;
    let (_, shown) = w
        .kernel
        .cli(&["processes", "case", "{store}", "shop.flagged", &case]);
    assert!(
        shown.contains("IncidentRaised") && shown.contains("forbidden"),
        "refused by the kit's middleware, as the process's identity: {shown}"
    );
    // The identity the kit made, with its key.
    let identity = "process-shop.flagged";
    let row = w
        .kit
        .query::<Value>(
            Query::of(GetIdentity {
                identity_id: identity.into(),
            })
            .tenant(PLATFORM)
            .context(kit.clone()),
        )
        .await
        .unwrap();
    assert_eq!(row["kind"], "process", "{row}");
    assert_eq!(row["apiKeys"].as_array().map(Vec::len), Some(1), "{row}");

    // A platform group lets it cancel orders, in every tenant.
    let group = new_id();
    committed(
        &w.send(
            CreateGroup {
                name: "Flagged orders".into(),
                description: None,
                permissions: vec![Permission {
                    pattern: "shop.CancelOrder".into(),
                    ..Permission::default()
                }],
            },
            &group,
            PLATFORM,
            kit.clone(),
        )
        .await,
    );
    committed(
        &w.send(
            GrantMembership {
                membership_id: new_id(),
                group,
                scope: None,
                expires_at: None,
            },
            identity,
            PLATFORM,
            kit.clone(),
        )
        .await,
    );
    let (retried, out) = w.kernel.cli(&[
        "processes",
        "retry",
        "{url}",
        "kit.key",
        "shop.flagged",
        &case,
    ]);
    assert!(retried, "{out}");
    state("ended").await;
}
