//! A minimal OpenID Connect provider for tests: discovery, JWKS, a token endpoint with PKCE and an
//! ES256 ID token. `login` plays the user who signs in at the provider and is sent back with a code.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use data_encoding::{BASE64, BASE64URL_NOPAD};
use p256::ecdsa::{Signature, SigningKey, signature::Signer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone)]
struct Grant {
    user: Value,
    challenge: String,
    nonce: String,
    redirect_uri: String,
}

#[derive(Clone)]
struct Shared {
    issuer: String,
    client_id: String,
    client_secret: String,
    key: SigningKey,
    codes: Arc<Mutex<HashMap<String, Grant>>>,
}

pub struct OidcProvider {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    shared: Shared,
    task: tokio::task::JoinHandle<()>,
}

fn b64(raw: &[u8]) -> String {
    BASE64URL_NOPAD.encode(raw)
}

async fn discovery(State(s): State<Shared>) -> Json<Value> {
    let i = &s.issuer;
    Json(json!({
        "issuer": i, "authorization_endpoint": format!("{i}/authorize"), "token_endpoint": format!("{i}/token"), "jwks_uri": format!("{i}/jwks"),
        "response_types_supported": ["code"], "id_token_signing_alg_values_supported": ["ES256"], "code_challenge_methods_supported": ["S256"],
    }))
}

async fn jwks(State(s): State<Shared>) -> Json<Value> {
    let point = s.key.verifying_key().to_encoded_point(false);
    Json(
        json!({ "keys": [{ "kty": "EC", "crv": "P-256", "x": b64(point.x().unwrap()), "y": b64(point.y().unwrap()), "kid": "k1", "alg": "ES256", "use": "sig" }] }),
    )
}

async fn token(
    State(s): State<Shared>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> (axum::http::StatusCode, Json<Value>) {
    let basic = headers
        .get("authorization")
        .and_then(|a| a.to_str().ok())
        .and_then(|a| a.strip_prefix("Basic "))
        .unwrap_or_default();
    let decoded =
        String::from_utf8(BASE64.decode(basic.as_bytes()).unwrap_or_default()).unwrap_or_default();
    let (id, secret) = decoded.split_once(':').unwrap_or_default();
    let grant = s
        .codes
        .lock()
        .unwrap()
        .remove(form.get("code").map(String::as_str).unwrap_or_default());
    if id != s.client_id || secret != s.client_secret {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid_client" })),
        );
    }
    let verifier = form.get("code_verifier").cloned().unwrap_or_default();
    let ok = grant.as_ref().is_some_and(|g| {
        Some(&g.redirect_uri) == form.get("redirect_uri")
            && b64(&Sha256::digest(verifier.as_bytes())) == g.challenge
    });
    let Some(grant) = grant.filter(|_| ok) else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_grant" })),
        );
    };
    let now = kernable::Timestamp::now().millis() / 1000;
    let header = b64(br#"{"alg":"ES256","kid":"k1","typ":"JWT"}"#);
    let mut claims = json!({ "iss": s.issuer, "aud": s.client_id, "iat": now, "exp": now + 300, "nonce": grant.nonce });
    for (k, v) in grant.user.as_object().unwrap() {
        claims[k] = v.clone();
    }
    let payload = b64(&serde_json::to_vec(&claims).unwrap());
    let signature: Signature = s.key.sign(format!("{header}.{payload}").as_bytes());
    let id_token = format!("{header}.{payload}.{}", b64(&signature.to_bytes()));
    (
        axum::http::StatusCode::OK,
        Json(
            json!({ "access_token": "at", "token_type": "Bearer", "expires_in": 300, "id_token": id_token }),
        ),
    )
}

impl OidcProvider {
    pub async fn start() -> OidcProvider {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let shared = Shared {
            issuer: issuer.clone(),
            client_id: "kit-client".into(),
            client_secret: kit::shared::secrets::random_secret(16),
            key: SigningKey::from_slice(&kit::shared::secrets::random_bytes(32)).unwrap(),
            codes: Arc::default(),
        };
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/token", post(token))
            .with_state(shared.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        OidcProvider {
            issuer,
            client_id: shared.client_id.clone(),
            client_secret: shared.client_secret.clone(),
            shared,
            task,
        }
    }

    /// The user signs in at the provider for this authorization URL: the URL the browser comes back to.
    pub fn login(&self, authorization_url: &str, user: Value) -> String {
        let url = reqwest::Url::parse(authorization_url).unwrap();
        let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let code = kit::shared::secrets::random_secret(16);
        let grant = Grant {
            user,
            challenge: q["code_challenge"].clone(),
            nonce: q["nonce"].clone(),
            redirect_uri: q["redirect_uri"].clone(),
        };
        self.shared
            .codes
            .lock()
            .unwrap()
            .insert(code.clone(), grant);
        let mut back = reqwest::Url::parse(&q["redirect_uri"]).unwrap();
        back.query_pairs_mut()
            .append_pair("code", &code)
            .append_pair("state", &q["state"]);
        back.to_string()
    }
}

impl Drop for OidcProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}
