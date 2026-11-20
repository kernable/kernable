//! The kit parity runner for Rust (see ../../parity/README.md). Run by ../../parity/run.ts:
//!
//! ```sh
//! cargo build -p kernable-kit --example parity
//! target/debug/examples/parity host     # the kit's domains and the parity shop, until SIGTERM
//! target/debug/examples/parity driver   # runs the scenario through KitApi, writes the transcript
//! ```
//!
//! Both read PARITY_URL, PARITY_KEY, PARITY_DIR; the driver also PARITY_SCENARIO, PARITY_OUT,
//! PARITY_KERNABLE (the kernel binary) and PARITY_STORE. Results are written in the shapes the
//! TypeScript kit answers in (`{ ok, reason }`, camelCase keys), so they compare.

#[path = "../tests/e2e/authenticator.rs"]
mod authenticator;
#[path = "../tests/e2e/oidc_provider.rs"]
mod oidc_provider;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kernable::process::{Send, Start};
use kernable::{
    App, Error, Kernable, Options, Outcome, Query, RequestContext, Submit, command, event, query,
};
use kit::api::{
    ApiError, ApiResult, Claims, KitApi, NewInvitationLink, Provider, new_signing_keys,
};
use kit::authorization::{Target, Visibility, use_authorization_store, visibility};
use kit::bootstrap::{Admin, bootstrap};
use kit::invitation::InvitedGroup;
use kit::shared::mail::{Mail, Mailer, Purpose, use_mailer};
use kit::shared::secrets::totp_code;
use kit::tenancy::SignUpPolicy;
use serde_json::{Map, Value, json};

use authenticator::Authenticator;
use oidc_provider::OidcProvider;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

// --- The parity shop: the same in every language -------------------------------------------------

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

#[query("shop", answer = Vec<String>)]
pub struct ListOrders {}

#[query("shop", answer = OrderDetails, target(aggregate = "order", field = id))]
pub struct GetOrder {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderDetails {
    pub customer: String,
}

#[event("shop")]
pub struct ParcelRequested {}

#[event("shop")]
pub struct ParcelShipped {}

#[command("shop", aggregate = "parcel")]
pub struct RequestParcel {}

#[command("shop", aggregate = "parcel")]
pub struct ShipParcel {}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Order {
    status: String,
}

impl Default for Order {
    fn default() -> Order {
        Order {
            status: "none".into(),
        }
    }
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct Parcel {
    requested: bool,
    shipped: bool,
}

fn shop() -> kernable::Domain {
    let shop = kernable::domain("shop");
    let order = shop.aggregate("order", Order::default());
    order.apply(OrderPlaced, |s, _, _, _| s.status = "placed".into());
    order.apply(OrderCancelled, |s, _, _, _| s.status = "cancelled".into());
    order.command(PlaceOrder, |s, _, _, ctx| {
        if s.status != "none" {
            return Err(kernable::Rejected::new("The order exists").code("exists"));
        }
        Ok(vec![
            OrderPlaced {
                customer: ctx.request.sender_identity.clone(),
            }
            .into(),
        ])
    });
    order.command(CancelOrder, |s, _, _, _| {
        if s.status != "placed" {
            return Err(kernable::Rejected::new("Not placed").code("invalid_state"));
        }
        Ok(vec![OrderCancelled {}.into()])
    });
    kit::authorization::relation("shop.order", "owner").set("shop.OrderPlaced", |e| {
        e.data
            .get("customer")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .into_iter()
            .collect()
    });

    let rows: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let orders = shop.read_model("shop.orders");
    let r = Arc::clone(&rows);
    orders.on(OrderPlaced, move |_, evt, _| {
        let mut rows = r.lock().unwrap();
        if !rows
            .iter()
            .any(|(t, id)| *t == evt.tenant && *id == evt.aggregate_id)
        {
            rows.push((evt.tenant.clone(), evt.aggregate_id.clone()));
        }
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
    let customers: Arc<Mutex<HashMap<String, String>>> = Arc::default();
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

    let parcel = shop.aggregate("parcel", Parcel::default());
    parcel.apply(ParcelRequested, |s, _, _, _| s.requested = true);
    parcel.apply(ParcelShipped, |s, _, _, _| s.shipped = true);
    parcel.command(RequestParcel, |_, _, _, _| {
        Ok(vec![ParcelRequested {}.into()])
    });
    parcel.command(ShipParcel, |s, _, _, _| {
        if s.shipped {
            return Err(kernable::Rejected::new("Shipped already").code("invalid_state"));
        }
        Ok(vec![ParcelShipped {}.into()])
    });
    shop.process("dispatch")
        .start::<ParcelRequested>(Start::key("aggregate_id").data([("parcel_id", "aggregate_id")]))
        .command::<ShipParcel>("ship", Send::aggregate("case.parcel_id"))
        .end("done");
    shop
}

// Mails go to a file both processes see: the host's reactors send, the driver reads.
struct FileMailer(String, Mutex<()>);

fn purpose(p: Purpose) -> &'static str {
    match p {
        Purpose::Verify => "verify",
        Purpose::Reset => "reset",
        Purpose::EmailChange => "email_change",
        Purpose::Invitation => "invitation",
        Purpose::AccountExists => "account_exists",
        Purpose::Notice => "notice",
    }
}

impl Mailer for FileMailer {
    fn send(&self, mail: Mail) {
        let _guard = self.1.lock().unwrap();
        let line = json!({ "to": mail.to, "subject": mail.subject, "text": mail.text, "purpose": purpose(mail.purpose), "token": mail.token });
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.0)
            .unwrap();
        writeln!(f, "{line}").unwrap();
    }
}

fn read_mails(path: &str) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn options() -> Options {
    Options::new(env("PARITY_KEY")).url(env("PARITY_URL"))
}

// --- Host ------------------------------------------------------------------------------------------

async fn host() {
    let dir = env("PARITY_DIR");
    use_mailer(Arc::new(FileMailer(
        format!("{dir}/mails.jsonl"),
        Mutex::new(()),
    )));
    use_authorization_store(&format!("sqlite:{dir}/authorization.db"));
    let mut definition = App::new().service("kit");
    // The app's relations first: the kit's middleware reads them when its domains are built.
    let shop = shop();
    let domains = kit::kit_domains();
    for d in domains.iter().chain([&shop]) {
        definition = definition.domain(d);
    }
    let app = Kernable::new(options()).unwrap().app(definition).unwrap();
    let running = app.clone();
    tokio::spawn(async move { running.run().await });
    app.ready().await;
    println!("ready");
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    app.stop();
    tokio::time::sleep(Duration::from_millis(200)).await;
    std::process::exit(0);
}

// --- Driver ----------------------------------------------------------------------------------------

fn context_json(c: &RequestContext) -> Value {
    let mut out = Map::new();
    for (k, v) in [
        ("senderTenant", &c.sender_tenant),
        ("senderIdentity", &c.sender_identity),
        ("senderAccount", &c.sender_account),
        ("senderSession", &c.sender_session),
        ("onBehalfOf", &c.on_behalf_of),
    ] {
        if !v.is_empty() {
            out.insert(k.into(), Value::String(v.clone()));
        }
    }
    if c.bypass_authorization {
        out.insert("bypassAuthorization".into(), Value::Bool(true));
    }
    Value::Object(out)
}

fn to_context(j: &Value) -> RequestContext {
    let s = |k: &str| {
        j.get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    RequestContext {
        sender_tenant: s("senderTenant"),
        sender_identity: s("senderIdentity"),
        sender_account: s("senderAccount"),
        sender_session: s("senderSession"),
        on_behalf_of: s("onBehalfOf"),
        bypass_authorization: j
            .get("bypassAuthorization")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        ..Default::default()
    }
}

fn camel(s: &str) -> String {
    let mut out = String::new();
    let mut up = false;
    for c in s.chars() {
        if c == '_' {
            up = true;
        } else if up {
            out.extend(c.to_uppercase());
            up = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Keys in camelCase, as the log and the other kits answer.
fn camel_keys(v: Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.into_iter()
                .map(|(k, v)| (camel(&k), camel_keys(v)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.into_iter().map(camel_keys).collect()),
        v => v,
    }
}

fn matches(expected: &Value, actual: &Value) -> bool {
    match expected {
        Value::Object(e) => match actual {
            Value::Object(a) => e
                .iter()
                .all(|(k, v)| matches(v, a.get(k).unwrap_or(&Value::Null))),
            _ => false,
        },
        _ => expected == actual,
    }
}

/// A KitApi result as the TypeScript kit answers: `ok` with `fields(value)`, or `{ ok: false, reason }`.
fn answer<T>(r: ApiResult<T>, fields: impl FnOnce(T) -> Value) -> Value {
    match r {
        Ok(v) => {
            let mut out = json!({ "ok": true });
            if let Value::Object(m) = fields(v) {
                out.as_object_mut().unwrap().extend(m);
            }
            out
        }
        Err(ApiError::Refused(reason)) => json!({ "ok": false, "reason": reason }),
        Err(ApiError::Kernel(e)) => error(&e),
    }
}

/// A result the TypeScript kit returns as it is, throwing on a refusal.
fn plain<T>(r: ApiResult<T>, value: impl FnOnce(T) -> Value) -> Value {
    match r {
        Ok(v) => value(v),
        Err(ApiError::Refused(reason)) => json!({ "error": reason }),
        Err(ApiError::Kernel(e)) => error(&e),
    }
}

fn error(e: &Error) -> Value {
    match e {
        Error::Rejected(r) => json!({ "error": r.code }),
        e => json!({ "error": e.to_string() }),
    }
}

fn signed_in(s: kit::api::SignedIn) -> Value {
    camel_keys(serde_json::to_value(s).unwrap())
}

struct Driver {
    kit: Kernable,
    vars: HashMap<String, Value>,
    apis: HashMap<String, KitApi>,
    devices: HashMap<String, Authenticator>,
    providers: HashMap<String, OidcProvider>,
    txt: Arc<Mutex<HashMap<String, String>>>,
    consumed: usize,
    service: (String, String),
    mails: String,
    kernable_bin: String,
    store: String,
    dir: String,
}

impl Driver {
    fn lookup(&self, path: &str) -> Value {
        let mut parts = path.split('.');
        let head = parts.next().unwrap();
        let mut v = self
            .vars
            .get(head)
            .unwrap_or_else(|| panic!("unknown variable {head}"))
            .clone();
        for p in parts {
            v = match &v {
                Value::Array(a) => a
                    .get(p.parse::<usize>().unwrap())
                    .cloned()
                    .unwrap_or(Value::Null),
                Value::Object(m) => m.get(p).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
        }
        v
    }

    fn resolve(&self, v: &Value) -> Value {
        match v {
            Value::String(s) if s.starts_with('$') => self.lookup(&s[1..]),
            Value::Array(a) => Value::Array(a.iter().map(|x| self.resolve(x)).collect()),
            Value::Object(m) => {
                if let Some(Value::Array(parts)) = m.get("concat") {
                    return Value::String(
                        parts
                            .iter()
                            .map(|x| match self.resolve(x) {
                                Value::String(s) => s,
                                other => other.to_string(),
                            })
                            .collect(),
                    );
                }
                if let Some(Value::Array(parts)) = m.get("merge") {
                    let mut out = Map::new();
                    for x in parts {
                        if let Value::Object(o) = self.resolve(x) {
                            out.extend(o);
                        }
                    }
                    return Value::Object(out);
                }
                if let Some(t) = m.get("tamper") {
                    let s = self.resolve(t).as_str().unwrap().to_owned();
                    return Value::String(match m["how"].as_str().unwrap() {
                        "lastChar" => format!(
                            "{}{}",
                            &s[..s.len() - 1],
                            if s.ends_with('A') { "B" } else { "A" }
                        ),
                        "secret" => format!("{}.not-the-secret", &s[..s.rfind('.').unwrap()]),
                        how => panic!("unknown tamper {how}"),
                    });
                }
                if let Some(secret) = m.get("totp") {
                    let secret = self.resolve(secret);
                    let at = self.resolve(&m["at"]).as_i64().unwrap()
                        + m.get("plus").and_then(Value::as_i64).unwrap_or(0);
                    return Value::String(totp_code(secret.as_str().unwrap(), at));
                }
                Value::Object(
                    m.iter()
                        .map(|(k, x)| (k.clone(), self.resolve(x)))
                        .collect(),
                )
            }
            v => v.clone(),
        }
    }

    fn api(&self, s: &Value) -> &KitApi {
        let name = s.get("api").and_then(Value::as_str).unwrap_or("api");
        self.apis
            .get(name)
            .unwrap_or_else(|| panic!("no KitApi {name}"))
    }

    fn claims(&self, s: &Value, key: &str) -> Option<Claims> {
        match self.resolve(&s[key]) {
            Value::String(token) => self.api(s).check(&token),
            Value::Null => None,
            v => serde_json::from_value(v).ok(),
        }
    }

    async fn run(&mut self, s: &Value) -> Value {
        let p = |k: &str| self.resolve(s.get(k).unwrap_or(&Value::Null));
        let ps = |k: &str| p(k).as_str().map(str::to_owned).unwrap_or_default();
        let op = s["op"].as_str().unwrap();
        // Claims a step names, for the calls that need them (a missing one fails the step).
        macro_rules! claims {
            ($k:expr) => {
                match self.claims(s, $k) {
                    Some(c) => c,
                    None => return json!({ "error": "no claims" }),
                }
            };
        }
        match op {
            "uuid" => Value::String(kit::shared::secrets::new_id()),
            "value" => p("value"),
            "bootstrap" => {
                let sign_up: SignUpPolicy = serde_json::from_value(p("signUp")).unwrap();
                let admin = p("admin");
                let booted = bootstrap(
                    &self.kit,
                    sign_up,
                    "Platform",
                    Some(Admin {
                        email: admin["email"].as_str().unwrap(),
                        name: admin["name"].as_str().unwrap(),
                        password: admin["password"].as_str().unwrap(),
                    }),
                )
                .await
                .unwrap();
                self.service = (booted.api_identity.clone(), booted.api_key_id.clone());
                self.apis.insert(
                    "api".into(),
                    KitApi::new(
                        self.kit.clone(),
                        new_signing_keys(),
                        &booted.api_identity,
                        &booted.api_key_id,
                    ),
                );
                let mut out = json!({ "api": { "identityId": booted.api_identity, "keyId": booted.api_key_id, "key": booted.api_key } });
                out["admin"] = match booted.admin {
                    Some((account, identity)) => {
                        json!({ "accountId": account, "identityId": identity })
                    }
                    None => Value::Null,
                };
                out
            }
            "kitApi" => {
                let mut api = KitApi::new(
                    self.kit.clone(),
                    new_signing_keys(),
                    &self.service.0,
                    &self.service.1,
                )
                .redirect_uri(&ps("redirectUri"))
                .insecure_issuers();
                if let Some(Value::Object(providers)) = s.get("providers") {
                    for (name, handle) in providers {
                        let pr = &self.providers[handle.as_str().unwrap()];
                        api = api.provider(
                            name,
                            Provider::oidc(&pr.issuer, &pr.client_id, &pr.client_secret),
                        );
                    }
                }
                let txt = Arc::clone(&self.txt);
                api = api.resolve_txt(move |name| {
                    let found = txt.lock().unwrap().get(&name).cloned();
                    async move { found.into_iter().collect() }
                });
                self.apis.insert(s["as"].as_str().unwrap().into(), api);
                Value::Null
            }
            "register" => {
                let password = ps("password");
                answer(
                    self.api(s)
                        .register(&ps("email"), &ps("name"), Some(&password))
                        .await,
                    |id| json!({ "accountId": id }),
                )
            }
            "mail" => {
                let to = ps("to");
                for _ in 0..400 {
                    let mails = read_mails(&self.mails);
                    if let Some(found) = mails.iter().rposition(|m| m["to"] == to)
                        && (s.get("old").is_some() || found >= self.consumed)
                    {
                        self.consumed = self.consumed.max(found + 1);
                        return mails[found].clone();
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                json!({ "error": format!("no new mail to {to}") })
            }
            "mailCount" => {
                let settle = s.get("settleMs").and_then(Value::as_u64).unwrap_or(300);
                tokio::time::sleep(Duration::from_millis(settle)).await;
                let n = read_mails(&self.mails).len();
                self.consumed = self.consumed.max(n);
                json!(n)
            }
            "verifyEmail" => plain(self.api(s).verify_email(&ps("token")).await, |b| json!(b)),
            "signIn" => {
                let ip = s.get("ip").map(|_| ps("ip"));
                answer(
                    self.api(s)
                        .sign_in(&ps("email"), &ps("password"), ip.as_deref(), None)
                        .await,
                    signed_in,
                )
            }
            "check" => self
                .api(s)
                .check(&ps("token"))
                .map(|c| serde_json::to_value(c).unwrap())
                .unwrap_or(Value::Null),
            "contextFor" => {
                let c = claims!("claims");
                let tenant = s.get("tenant").map(|_| ps("tenant"));
                plain(
                    self.api(s).context_for(&c, tenant.as_deref()).await,
                    |ctx| context_json(&ctx),
                )
            }
            "refresh" => answer(
                self.api(s).refresh(&ps("token"), None).await,
                |(a, r)| json!({ "accessToken": a, "refreshToken": r }),
            ),
            "signOut" => {
                let c = claims!("claims");
                plain(self.api(s).sign_out(&c).await, |_| Value::Null)
            }
            "startTotp" => {
                let c = claims!("claims");
                answer(
                    self.api(s)
                        .start_totp(&c, &ps("issuer"), &ps("label"))
                        .await,
                    |(secret, uri)| json!({ "secret": secret, "uri": uri }),
                )
            }
            "totpStep" => json!(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    / 30
            ),
            "confirmTotp" => {
                let c = claims!("claims");
                answer(
                    self.api(s).confirm_totp(&c, &ps("code")).await,
                    |codes| json!({ "recoveryCodes": codes }),
                )
            }
            "proveSecondFactor" => {
                let c = claims!("claims");
                answer(
                    self.api(s).prove_second_factor(&c, &ps("code")).await,
                    |t| json!({ "accessToken": t }),
                )
            }
            "createApiKey" => answer(
                self.api(s)
                    .create_api_key(
                        to_context(&p("context")),
                        &ps("tenant"),
                        &ps("identityId"),
                        &ps("label"),
                        None,
                    )
                    .await,
                |(key, key_id)| json!({ "key": key, "keyId": key_id }),
            ),
            "checkApiKey" => plain(self.api(s).check_api_key(&ps("key")).await, |c| {
                c.map(|c| context_json(&c)).unwrap_or(Value::Null)
            }),
            "changePassword" => {
                let c = claims!("claims");
                let current = ps("current");
                answer(
                    self.api(s)
                        .change_password(&c, Some(&current), &ps("next"))
                        .await,
                    |_| json!({}),
                )
            }
            "requestPasswordReset" => plain(
                self.api(s).request_password_reset(&ps("email")).await,
                |_| Value::Null,
            ),
            "resetPassword" => answer(
                self.api(s).reset_password(&ps("token"), &ps("next")).await,
                |_| json!({}),
            ),
            "requestEmailChange" => {
                let c = claims!("claims");
                answer(
                    self.api(s).request_email_change(&c, &ps("email")).await,
                    |_| json!({}),
                )
            }
            "confirmEmailChange" => {
                answer(self.api(s).confirm_email_change(&ps("token")).await, |_| {
                    json!({})
                })
            }
            "peekInvitation" => plain(self.api(s).peek_invitation(&ps("token")).await, |p| {
                p.map(|p| camel_keys(serde_json::to_value(p).unwrap()))
                    .unwrap_or(Value::Null)
            }),
            "acceptInvitation" => {
                let r = if s.get("claims").is_some() {
                    let c = claims!("claims");
                    self.api(s)
                        .accept_invitation(&ps("token"), Some(&c), None)
                        .await
                } else {
                    let (name, password) = (ps("name"), ps("password"));
                    self.api(s)
                        .accept_invitation(&ps("token"), None, Some((&name, &password)))
                        .await
                };
                answer(
                    r,
                    |(account, tenant)| json!({ "accountId": account, "tenant": tenant }),
                )
            }
            "createInvitationLink" => {
                let groups: Vec<InvitedGroup> =
                    serde_json::from_value(p("groups")).unwrap_or_default();
                let label = s.get("label").map(|_| ps("label"));
                let link = NewInvitationLink {
                    groups,
                    label,
                    expires_at: None,
                    max_uses: p("maxUses").as_i64(),
                };
                answer(
                    self.api(s)
                        .create_invitation_link(to_context(&p("context")), &ps("tenant"), link)
                        .await,
                    |(token, link_id)| json!({ "token": token, "linkId": link_id }),
                )
            }
            "peekInvitationLink" => {
                plain(self.api(s).peek_invitation_link(&ps("token")).await, |p| {
                    p.map(|p| camel_keys(serde_json::to_value(p).unwrap()))
                        .unwrap_or(Value::Null)
                })
            }
            "joinWithInvitationLink" => {
                let c = claims!("claims");
                answer(
                    self.api(s)
                        .join_with_invitation_link(&c, &ps("token"))
                        .await,
                    |tenant| json!({ "tenant": tenant }),
                )
            }
            "verifySsoDomain" => answer(
                self.api(s)
                    .verify_sso_domain(&ps("tenant"), &ps("ssoId"), &ps("domain"))
                    .await,
                |_| json!({}),
            ),
            "serviceContext" => context_json(&self.api(s).service_context),
            "authenticator" => {
                self.devices.insert(
                    s["as"].as_str().unwrap().into(),
                    Authenticator::new("localhost", "http://localhost:3000", &ps("userId")),
                );
                Value::Null
            }
            "passkeyRegistrationOptions" => {
                let c = claims!("claims");
                plain(
                    self.api(s)
                        .passkey_registration_options(&c, &ps("userName"))
                        .await,
                    |v| v,
                )
            }
            "passkeyRegister" => {
                self.devices[s["device"].as_str().unwrap()].register(&ps("challenge"))
            }
            "passkeyAssert" => {
                let origin = s.get("origin").map(|_| ps("origin"));
                let challenge = ps("challenge");
                self.devices
                    .get_mut(s["device"].as_str().unwrap())
                    .unwrap()
                    .assert(&challenge, origin.as_deref())
            }
            "addPasskey" => {
                let c = claims!("claims");
                let label = s.get("label").map(|_| ps("label"));
                answer(
                    self.api(s)
                        .add_passkey(&c, p("response"), label.as_deref())
                        .await,
                    |_| json!({}),
                )
            }
            "passkeySignInOptions" => plain(self.api(s).passkey_sign_in_options().await, |v| v),
            "signInWithPasskey" => answer(
                self.api(s).sign_in_with_passkey(p("response"), None).await,
                signed_in,
            ),
            "oidcProvider" => {
                self.providers.insert(
                    s["as"].as_str().unwrap().into(),
                    OidcProvider::start().await,
                );
                Value::Null
            }
            "oidcIssuer" => {
                let pr = &self.providers[s["provider"].as_str().unwrap()];
                json!({ "issuer": pr.issuer, "clientId": pr.client_id, "clientSecret": pr.client_secret })
            }
            "setTxt" => {
                self.txt.lock().unwrap().insert(ps("name"), ps("value"));
                Value::Null
            }
            "oidcStart" => {
                let by = p("by");
                let link = if s.get("link").is_some() {
                    Some(claims!("link"))
                } else {
                    None
                };
                let r = self
                    .api(s)
                    .oidc_start(by["provider"].as_str(), by["email"].as_str(), link.as_ref())
                    .await;
                answer(r, |url| json!({ "url": url }))
            }
            "oidcLogin" => Value::String(
                self.providers[s["provider"].as_str().unwrap()].login(&ps("url"), p("user")),
            ),
            "oidcFinish" => answer(
                self.api(s).oidc_finish(&ps("callback"), None).await,
                |r| match r {
                    Some(signed) => signed_in(signed),
                    None => json!({ "linked": true }),
                },
            ),
            "submit" => {
                let command = s["command"].as_str().unwrap();
                let mut parts = command.split('.');
                let (d, aggregate_type, type_) = (
                    parts.next().unwrap(),
                    parts.next().unwrap(),
                    parts.next().unwrap(),
                );
                let data = match p("data") {
                    Value::Null => json!({}),
                    v => v,
                };
                let submit = Submit::new(d, aggregate_type, &ps("aggregateId"), type_)
                    .data(data)
                    .tenant(ps("tenant"))
                    .context(to_context(&p("context")));
                match self.kit.submit(submit).await {
                    Ok(Outcome::Committed(events)) => json!({
                        "committed": true,
                        "events": events.iter().map(|e| json!({ "type": e.event_type, "data": e.data })).collect::<Vec<_>>(),
                    }),
                    Ok(Outcome::Rejected(r)) => {
                        json!({ "committed": false, "code": r.code, "message": r.message })
                    }
                    Err(e) => error(&e),
                }
            }
            "query" => {
                let q = s["query"].as_str().unwrap();
                let (d, type_) = q.split_once('.').unwrap();
                let data = match p("data") {
                    Value::Null => json!({}),
                    v => v,
                };
                let query = Query::new(d, type_)
                    .data(data)
                    .tenant(ps("tenant"))
                    .context(to_context(&p("context")));
                match self.kit.query::<Value>(query).await {
                    Ok(value) => json!({ "ok": true, "value": value }),
                    Err(Error::Rejected(r)) => {
                        json!({ "ok": false, "code": r.code, "message": r.message })
                    }
                    Err(e) => error(&e),
                }
            }
            "processIdentityId" => Value::String(format!("process-{}", ps("process"))),
            "processAct" => match self
                .kit
                .process_act(&ps("process"), to_context(&p("context")))
                .await
            {
                Ok(changed) => json!({ "changed": changed }),
                Err(e) => error(&e),
            },
            "waitIdentityKey" => {
                for _ in 0..400 {
                    let query = Query::new("identity", "GetIdentity")
                        .data(json!({ "identityId": ps("identityId") }))
                        .tenant("platform")
                        .context(RequestContext {
                            bypass_authorization: true,
                            ..Default::default()
                        });
                    if let Ok(identity) = self.kit.query::<Value>(query).await
                        && let Some(key) = identity["apiKeys"][0]["keyId"].as_str()
                    {
                        return Value::String(key.into());
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                json!({ "error": "no key for the identity" })
            }
            "waitCase" => {
                let (process, case, state) = (ps("process"), ps("case"), ps("state"));
                for _ in 0..400 {
                    let out = std::process::Command::new(&self.kernable_bin)
                        .args([
                            "processes",
                            "cases",
                            &self.store,
                            &process,
                            "--state",
                            &state,
                        ])
                        .output()
                        .unwrap();
                    if String::from_utf8_lossy(&out.stdout)
                        .lines()
                        .any(|l| l.contains(&format!(" {case} ")))
                    {
                        return json!({ "state": state });
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                json!({ "state": "not reached" })
            }
            "retryCase" => {
                let out = std::process::Command::new(&self.kernable_bin)
                    .args([
                        "processes",
                        "retry",
                        &env("PARITY_URL"),
                        &env("PARITY_KEY"),
                        &ps("process"),
                        &ps("case"),
                        "--tenant",
                        &ps("tenant"),
                    ])
                    .output()
                    .unwrap();
                if out.status.success() {
                    Value::Null
                } else {
                    json!({ "error": String::from_utf8_lossy(&out.stderr) })
                }
            }
            "restartHost" => {
                let flag = format!("{}/restart", self.dir);
                std::fs::write(&flag, "").unwrap();
                for _ in 0..1200 {
                    if !std::path::Path::new(&flag).exists() {
                        return Value::Null;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                json!({ "error": "the host did not restart" })
            }
            op => panic!("unknown op {op}"),
        }
    }
}

async fn driver() {
    let scenario: Value =
        serde_json::from_str(&std::fs::read_to_string(env("PARITY_SCENARIO")).unwrap()).unwrap();
    let kit = Kernable::new(options()).unwrap();
    let shop = shop();
    for d in kit::kit_domains().iter().chain([&shop]) {
        kit.with_domain(d);
    }
    let dir = env("PARITY_DIR");
    let mut driver = Driver {
        kit,
        vars: HashMap::new(),
        apis: HashMap::new(),
        devices: HashMap::new(),
        providers: HashMap::new(),
        txt: Arc::default(),
        consumed: 0,
        service: (String::new(), String::new()),
        mails: format!("{dir}/mails.jsonl"),
        kernable_bin: env("PARITY_KERNABLE"),
        store: env("PARITY_STORE"),
        dir,
    };
    let mut transcript = Vec::new();
    for (i, s) in scenario["steps"].as_array().unwrap().iter().enumerate() {
        let result = driver.run(s).await;
        if let Some(name) = s.get("as").and_then(Value::as_str) {
            driver.vars.insert(name.into(), result.clone());
        }
        let mut entry = json!({ "step": i, "op": s["op"], "result": result });
        if let Some(note) = s.get("note") {
            entry["note"] = note.clone();
        }
        if let Some(expect) = s.get("expect") {
            let expected = driver.resolve(expect);
            if !matches(&expected, &entry["result"]) {
                entry["unexpected"] = expected;
            }
        }
        transcript.push(entry);
    }
    std::fs::write(
        env("PARITY_OUT"),
        serde_json::to_string_pretty(&transcript).unwrap(),
    )
    .unwrap();
    std::process::exit(0);
}

#[tokio::main]
async fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("host") => host().await,
        Some("driver") => driver().await,
        _ => panic!("usage: parity host|driver"),
    }
}
