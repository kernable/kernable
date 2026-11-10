//! An example API on axum over the kit's API part: how a framework wires `KitApi` in. Carry
//! the routes over to whatever you use; the kit's functions stay.
//!
//! ```sh
//! KERNABLE_URL=... KERNABLE_KEY=kit.key cargo run --example server      # 127.0.0.1, port 3000 or PORT
//! ```
//!
//! Requests carry `Authorization: Bearer <access token>` (or `ApiKey <key>`) and, inside a tenant,
//! `X-Tenant: <tenant ID>`; the server turns them into the request context the middleware decides on.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use kernable::{Error, Kernable, Outcome, Query, RequestContext, Submit};
use kit::api::{ApiError, KitApi, NewInvitationLink};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct Server {
    pub api: KitApi,
    pub kernable: Kernable,
}

fn reply(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn refusal(e: ApiError, status: StatusCode) -> Response {
    match e.refused() {
        Some(code) => reply(status, json!({ "error": code })),
        None => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": e.to_string() }),
        ),
    }
}

/// A refusal in the shape the TypeScript and Python examples answer: `{ "ok": false, "reason" }`.
fn not_ok(e: ApiError, status: StatusCode) -> Response {
    match e.refused() {
        Some(code) => reply(status, json!({ "ok": false, "reason": code })),
        None => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": e.to_string() }),
        ),
    }
}

fn text<'a>(b: &'a Value, field: &str) -> &'a str {
    b[field].as_str().unwrap_or_default()
}

fn bearer(s: &Server, headers: &HeaderMap) -> Option<kit::api::Claims> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    s.api.check(auth.strip_prefix("Bearer ")?)
}

/// The caller's request context, from the bearer token or API key and the tenant header.
async fn caller(s: &Server, headers: &HeaderMap) -> Option<RequestContext> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    if let Some(key) = auth.strip_prefix("ApiKey ") {
        return s.api.check_api_key(key).await.ok().flatten();
    }
    let claims = bearer(s, headers)?;
    let tenant = headers.get("x-tenant").and_then(|t| t.to_str().ok());
    // No identity in that tenant: none.
    s.api.context_for(&claims, tenant).await.ok()
}

async fn register(State(s): State<Server>, Json(b): Json<Value>) -> Response {
    // The same whether or not the address was taken.
    let password = b["password"].as_str();
    match s
        .api
        .register(text(&b, "email"), text(&b, "name"), password)
        .await
    {
        Err(ApiError::Kernel(e)) => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": e.to_string() }),
        ),
        _ => reply(
            StatusCode::ACCEPTED,
            json!({ "message": "Check your email" }),
        ),
    }
}

async fn verify(State(s): State<Server>, Json(b): Json<Value>) -> Response {
    match s.api.verify_email(text(&b, "token")).await {
        Ok(true) => reply(StatusCode::OK, json!({})),
        _ => reply(StatusCode::BAD_REQUEST, json!({})),
    }
}

async fn sign_in(
    State(s): State<Server>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let agent = headers.get("user-agent").and_then(|a| a.to_str().ok());
    let ip = peer.ip().to_string();
    match s
        .api
        .sign_in(text(&b, "email"), text(&b, "password"), Some(&ip), agent)
        .await
    {
        Ok(signed) => reply(
            StatusCode::OK,
            json!({ "accessToken": signed.access_token, "refreshToken": signed.refresh_token, "secondFactor": signed.second_factor }),
        ),
        Err(e) => refusal(e, StatusCode::UNAUTHORIZED),
    }
}

async fn refresh(
    State(s): State<Server>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(b): Json<Value>,
) -> Response {
    let ip = peer.ip().to_string();
    match s.api.refresh(text(&b, "refreshToken"), Some(&ip)).await {
        Ok((access, refresh)) => reply(
            StatusCode::OK,
            json!({ "ok": true, "accessToken": access, "refreshToken": refresh }),
        ),
        Err(e) => not_ok(e, StatusCode::UNAUTHORIZED),
    }
}

async fn password_reset(State(s): State<Server>, Json(b): Json<Value>) -> Response {
    let _ = s.api.request_password_reset(text(&b, "email")).await;
    reply(
        StatusCode::ACCEPTED,
        json!({ "message": "Check your email" }),
    )
}

async fn password_reset_confirm(State(s): State<Server>, Json(b): Json<Value>) -> Response {
    match s
        .api
        .reset_password(text(&b, "token"), text(&b, "password"))
        .await
    {
        Ok(()) => reply(StatusCode::OK, json!({})),
        Err(e) => refusal(e, StatusCode::BAD_REQUEST),
    }
}

async fn accept(State(s): State<Server>, headers: HeaderMap, Json(b): Json<Value>) -> Response {
    let claims = bearer(&s, &headers);
    let new_person = claims
        .is_none()
        .then(|| (text(&b, "name"), text(&b, "password")));
    match s
        .api
        .accept_invitation(text(&b, "token"), claims.as_ref(), new_person)
        .await
    {
        Ok((account, tenant)) => reply(
            StatusCode::OK,
            json!({ "ok": true, "accountId": account, "tenant": tenant }),
        ),
        Err(e) => not_ok(e, StatusCode::BAD_REQUEST),
    }
}

async fn second_factor(
    State(s): State<Server>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let Some(claims) = bearer(&s, &headers) else {
        return reply(
            StatusCode::UNAUTHORIZED,
            json!({ "error": "unauthenticated" }),
        );
    };
    match s.api.prove_second_factor(&claims, text(&b, "code")).await {
        Ok(access) => reply(StatusCode::OK, json!({ "ok": true, "accessToken": access })),
        Err(e) => not_ok(e, StatusCode::UNAUTHORIZED),
    }
}

async fn sign_out(State(s): State<Server>, headers: HeaderMap) -> Response {
    if let Some(claims) = bearer(&s, &headers) {
        let _ = s.api.sign_out(&claims).await;
    }
    StatusCode::NO_CONTENT.into_response()
}

fn unauthenticated() -> Response {
    reply(
        StatusCode::UNAUTHORIZED,
        json!({ "error": "unauthenticated" }),
    )
}

fn failed(e: impl std::fmt::Display) -> Response {
    reply(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({ "error": e.to_string() }),
    )
}

/// The request's `data`, `{}` if it has none.
fn data(b: &Value) -> Value {
    if b["data"].is_null() {
        json!({})
    } else {
        b["data"].clone()
    }
}

/// POST /tenants: opens a tenant the caller owns (self-service, as the sign-up policy allows).
async fn open_tenant(
    State(s): State<Server>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let Some(claims) = bearer(&s, &headers) else {
        return unauthenticated();
    };
    let context = match s.api.context_for(&claims, None).await {
        Ok(context) => context,
        Err(e) => return refusal(e, StatusCode::FORBIDDEN),
    };
    let tenant = kit::shared::secrets::new_id();
    let submit = Submit::new("tenancy", "tenant", &tenant, "CreateTenant")
        .data(json!({ "slug": b["slug"], "name": b["name"], "owner": { "accountId": claims.sub } }))
        .tenant(tenant.clone())
        .context(context);
    match s.kernable.submit(submit).await {
        Ok(Outcome::Committed(_)) => reply(StatusCode::CREATED, json!({ "tenant": tenant })),
        Ok(Outcome::Rejected(r)) => reply(StatusCode::FORBIDDEN, json!({ "error": r.code })),
        Err(e) => failed(e),
    }
}

/// POST /invitation-links/join: the signed-in caller joins a tenant through an invitation link.
async fn join_link(State(s): State<Server>, headers: HeaderMap, Json(b): Json<Value>) -> Response {
    let Some(claims) = bearer(&s, &headers) else {
        return unauthenticated();
    };
    match s
        .api
        .join_with_invitation_link(&claims, text(&b, "token"))
        .await
    {
        Ok(tenant) => reply(StatusCode::OK, json!({ "ok": true, "tenant": tenant })),
        Err(e) => not_ok(e, StatusCode::BAD_REQUEST),
    }
}

/// The request context as JSON: its fields that are set.
fn context_json(c: &RequestContext) -> Value {
    let mut out = serde_json::Map::new();
    for (name, value) in [
        ("senderAccount", &c.sender_account),
        ("senderSession", &c.sender_session),
        ("senderTenant", &c.sender_tenant),
        ("senderIdentity", &c.sender_identity),
        ("onBehalfOf", &c.on_behalf_of),
    ] {
        if !value.is_empty() {
            out.insert(name.into(), json!(value));
        }
    }
    if c.bypass_authorization {
        out.insert("bypassAuthorization".into(), json!(true));
    }
    if !c.attributes.is_empty() {
        out.insert("attributes".into(), json!(c.attributes));
    }
    Value::Object(out)
}

/// GET /me: the caller's request context in the tenant of `X-Tenant`.
async fn me(State(s): State<Server>, headers: HeaderMap) -> Response {
    match caller(&s, &headers).await {
        Some(context) => reply(StatusCode::OK, context_json(&context)),
        None => unauthenticated(),
    }
}

/// POST /invitation-links: a reusable invitation link into groups of the caller's tenant.
async fn create_link(
    State(s): State<Server>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let Some(context) = caller(&s, &headers).await else {
        return unauthenticated();
    };
    let link = NewInvitationLink {
        groups: serde_json::from_value(b["groups"].clone()).unwrap_or_default(),
        label: b["label"].as_str().map(str::to_owned),
        expires_at: b["expiresAt"].as_i64(),
        max_uses: b["maxUses"].as_i64(),
    };
    let tenant = context.sender_tenant.clone();
    match s.api.create_invitation_link(context, &tenant, link).await {
        Ok((token, link_id)) => reply(
            StatusCode::CREATED,
            json!({ "token": token, "linkId": link_id }),
        ),
        Err(e) => refusal(e, StatusCode::FORBIDDEN),
    }
}

/// Where a request of `domain` goes: accounts and sessions live in the platform tenant, the rest
/// in the caller's.
fn tenant_for(domain: &str, context: &RequestContext) -> String {
    if domain == "account" {
        kit::shared::constants::PLATFORM_TENANT.to_owned()
    } else {
        context.sender_tenant.clone()
    }
}

/// POST /commands/{domain}/{aggregate}/{type}: the app's commands, as the caller; `account` ones
/// go to the platform tenant, where every account lives.
async fn command(
    State(s): State<Server>,
    Path((domain, aggregate, r#type)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let Some(context) = caller(&s, &headers).await else {
        return unauthenticated();
    };
    let id = b["aggregateId"]
        .as_str()
        .map_or_else(kit::shared::secrets::new_id, str::to_owned);
    let submit = Submit::new(&domain, &aggregate, &id, &r#type)
        .data(data(&b))
        .tenant(tenant_for(&domain, &context))
        .context(context);
    match s.kernable.submit(submit).await {
        Ok(Outcome::Committed(events)) => reply(
            StatusCode::OK,
            json!({ "events": events.iter().map(|e| e.event_type.clone()).collect::<Vec<_>>() }),
        ),
        Ok(Outcome::Rejected(r)) => reply(StatusCode::FORBIDDEN, json!({ "error": r.code })),
        Err(e) => failed(e),
    }
}

/// POST /queries/{domain}/{type}: the app's queries, as the caller. A query given by name
/// carries the aggregate it is about if `kernable` knows its domain (`with_domain`).
async fn query(
    State(s): State<Server>,
    Path((domain, r#type)): Path<(String, String)>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let Some(context) = caller(&s, &headers).await else {
        return unauthenticated();
    };
    let q = Query::new(&domain, &r#type)
        .data(data(&b))
        .tenant(tenant_for(&domain, &context))
        .context(context);
    match s.kernable.query::<Value>(q).await {
        Ok(answer) => reply(StatusCode::OK, answer),
        Err(Error::Rejected(r)) if r.code == "not_found" => {
            reply(StatusCode::NOT_FOUND, json!({ "error": r.code }))
        }
        Err(Error::Rejected(r)) => reply(StatusCode::FORBIDDEN, json!({ "error": r.code })),
        Err(e) => failed(e),
    }
}

/// The routes; `kernable` submits the app's own commands with the caller's context. Serve them
/// with `into_make_service_with_connect_info::<SocketAddr>()`: sign-in and refresh read the
/// client's address.
pub fn router(api: KitApi, kernable: Kernable) -> Router {
    Router::new()
        .route("/register", post(register))
        .route("/verify", post(verify))
        .route("/sign-in", post(sign_in))
        .route("/refresh", post(refresh))
        .route("/password-reset", post(password_reset))
        .route("/password-reset/confirm", post(password_reset_confirm))
        .route("/invitations/accept", post(accept))
        .route("/second-factor", post(second_factor))
        .route("/sign-out", post(sign_out))
        .route("/tenants", post(open_tenant))
        .route("/invitation-links/join", post(join_link))
        .route("/me", get(me))
        .route("/invitation-links", post(create_link))
        .route("/commands/{domain}/{aggregate}/{type}", post(command))
        .route("/queries/{domain}/{type}", post(query))
        .fallback(|| async { reply(StatusCode::NOT_FOUND, json!({ "error": "not_found" })) })
        .with_state(Server { api, kernable })
}

/// The kit as one service, the bootstrap (it may run again), and the server.
#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = std::env::var("KERNABLE_KEY").unwrap_or_else(|_| "app.key".into());
    let options = || {
        let o = kernable::Options::new(&key);
        match std::env::var("KERNABLE_URL") {
            Ok(url) => o.url(url),
            Err(_) => o,
        }
    };
    let app = kernable::Kernable::new(options())?;
    let domains = kit::kit_domains();
    let mut definition = kernable::App::new().service("kit");
    for d in &domains {
        definition = definition.domain(d);
    }
    let handle = app.app(definition)?;
    let running = handle.clone();
    tokio::spawn(async move { running.run().await });
    handle.ready().await;
    // The API's client knows the kit's schemas: vault fields, and what queries asked by name are about.
    let kernable = Kernable::new(options())?;
    for d in &domains {
        kernable.with_domain(d);
    }
    let policy = kit::tenancy::SignUpPolicy {
        tenants: kit::tenancy::TenantSignUp::Active,
        accounts: kit::tenancy::AccountSignUp::Open,
    };
    let booted = kit::bootstrap::bootstrap(&kernable, policy, "Platform", None).await?;
    // The access token key: a PEM file in production (KIT_SIGNING_KEY), new on every start otherwise.
    let keys = match std::env::var("KIT_SIGNING_KEY") {
        Ok(path) => kit::api::signing_keys_from(&path)?,
        Err(_) => kit::api::new_signing_keys(),
    };
    let api = KitApi::new(
        kernable.clone(),
        keys,
        &booted.api_identity,
        &booted.api_key_id,
    );
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".into());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    eprintln!("example server on 127.0.0.1:{port}");
    // The client's address reaches the handlers (sign-in and refresh keep it with the session).
    axum::serve(
        listener,
        router(api, kernable).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
