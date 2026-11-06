//! An example API on axum over the kit's API part: how a framework wires `KitApi` in. Carry
//! the routes over to whatever you use; the kit's functions stay.
//!
//! ```sh
//! KERNABLE_URL=... KERNABLE_KEY=kit.key cargo run --example server      # port 3000, or PORT
//! ```
//!
//! Requests carry `Authorization: Bearer <access token>` (or `ApiKey <key>`) and, inside a tenant,
//! `X-Tenant: <tenant ID>`; the server turns them into the request context the middleware decides on.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use kernable::{Kernable, Outcome, RequestContext, Submit};
use kit::api::{ApiError, KitApi};
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

async fn sign_in(State(s): State<Server>, headers: HeaderMap, Json(b): Json<Value>) -> Response {
    let agent = headers.get("user-agent").and_then(|a| a.to_str().ok());
    match s
        .api
        .sign_in(text(&b, "email"), text(&b, "password"), None, agent)
        .await
    {
        Ok(signed) => reply(
            StatusCode::OK,
            json!({ "accessToken": signed.access_token, "refreshToken": signed.refresh_token, "secondFactor": signed.second_factor }),
        ),
        Err(e) => refusal(e, StatusCode::UNAUTHORIZED),
    }
}

async fn refresh(State(s): State<Server>, Json(b): Json<Value>) -> Response {
    match s.api.refresh(text(&b, "refreshToken"), None).await {
        Ok((access, refresh)) => reply(
            StatusCode::OK,
            json!({ "accessToken": access, "refreshToken": refresh }),
        ),
        Err(e) => refusal(e, StatusCode::UNAUTHORIZED),
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
            json!({ "accountId": account, "tenant": tenant }),
        ),
        Err(e) => refusal(e, StatusCode::BAD_REQUEST),
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
        Ok(access) => reply(StatusCode::OK, json!({ "accessToken": access })),
        Err(e) => refusal(e, StatusCode::UNAUTHORIZED),
    }
}

async fn sign_out(State(s): State<Server>, headers: HeaderMap) -> Response {
    if let Some(claims) = bearer(&s, &headers) {
        let _ = s.api.sign_out(&claims).await;
    }
    StatusCode::NO_CONTENT.into_response()
}

/// POST /commands/{domain}/{aggregate}/{type}: the app's commands, as the caller.
async fn command(
    State(s): State<Server>,
    Path((domain, aggregate, r#type)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(b): Json<Value>,
) -> Response {
    let Some(context) = caller(&s, &headers).await else {
        return reply(
            StatusCode::UNAUTHORIZED,
            json!({ "error": "unauthenticated" }),
        );
    };
    let id = b["aggregateId"]
        .as_str()
        .map_or_else(kit::shared::secrets::new_id, str::to_owned);
    let submit = Submit::new(&domain, &aggregate, &id, &r#type)
        .data(if b["data"].is_null() {
            json!({})
        } else {
            b["data"].clone()
        })
        .tenant(context.sender_tenant.clone())
        .context(context);
    match s.kernable.submit(submit).await {
        Ok(Outcome::Committed(events)) => reply(
            StatusCode::OK,
            json!({ "events": events.iter().map(|e| e.event_type.clone()).collect::<Vec<_>>() }),
        ),
        Ok(Outcome::Rejected(r)) => reply(StatusCode::FORBIDDEN, json!({ "error": r.code })),
        Err(e) => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": e.to_string() }),
        ),
    }
}

/// The routes; `kernable` submits the app's own commands with the caller's context.
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
        .route("/commands/{domain}/{aggregate}/{type}", post(command))
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
    let mut definition = kernable::App::new().service("kit");
    for d in kit::kit_domains() {
        definition = definition.domain(&d);
    }
    let handle = app.app(definition)?;
    let running = handle.clone();
    tokio::spawn(async move { running.run().await });
    handle.ready().await;
    let kernable = Kernable::new(options())?;
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
    eprintln!("example server on :{port}");
    axum::serve(listener, router(api, kernable)).await?;
    Ok(())
}
