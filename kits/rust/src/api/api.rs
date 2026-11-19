//! The API part: framework-neutral, async functions for what only the API can do. Wire
//! them into your HTTP framework; `examples/server.rs` shows one with axum.
//!
//! A refusal the caller shows (a wrong password, an expired link) is [`ApiError::Refused`] with
//! its code; anything else (the kernel unreachable) is [`ApiError::Kernel`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use data_encoding::BASE64URL_NOPAD;
use kernable::{
    CommandType, Error, Kernable, Outcome, Query, QueryType, RequestContext, StoredEvent, Submit,
    Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::tokens::{Claims, SigningKeys, check_access_token, issue_access_token};
use crate::account::AccountRow;
use crate::account::{
    AddPasskey, ChangePassword, ConfirmEmailChange, ConfirmTotp, EndSession, FindAccountByEmail,
    FindAccountByLink, FindSsoByDomain, GetAccount, GetSsoSecret, LinkProvider, ListSessions,
    NoteRegistrationAttempt, ProvePasskey, ProveSecondFactor, RefreshSession, RegisterAccount,
    RequestEmailChange, RequestPasswordReset, ResetPassword, SessionEnd, SignIn, SignInWithPasskey,
    SignInWithProvider, StartTotp, VerifyEmail, VerifySsoDomain,
};
use crate::identity::{CreateApiKey, FindApiKey, FindIdentityByAccount, api_key, parse_api_key};
use crate::invitation::{
    AcceptInvitation, CreateInvitationLink, InvitedGroup, JoinWithInvitationLink, PeekInvitation,
    PeekInvitationLink, Peeked, PeekedLink, link_token, parse_link_token,
};
use crate::shared::constants::{CHALLENGE_SECONDS, PLATFORM_TENANT, passkeys};
use crate::shared::secrets::{
    check_password, email_domain, equal_secrets, new_id, parse_invitation_token, parse_token,
    random_bytes, random_secret, token, totp_secret, trim_space, valid_password,
};
use crate::tenancy::{AccountSignUp, GetSignUpPolicy};

/// Why a call did not do what was asked.
#[derive(Debug)]
pub enum ApiError {
    /// A refusal with a code the caller can show or act on.
    Refused(String),
    /// The kernel (or the connection) failed.
    Kernel(Error),
}

impl From<Error> for ApiError {
    fn from(e: Error) -> ApiError {
        ApiError::Kernel(e)
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Refused(code) => write!(f, "refused: {code}"),
            ApiError::Kernel(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    /// The refusal's code, if it is one.
    pub fn refused(&self) -> Option<&str> {
        match self {
            ApiError::Refused(code) => Some(code),
            ApiError::Kernel(_) => None,
        }
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

fn refused<T>(code: &str) -> ApiResult<T> {
    Err(ApiError::Refused(code.to_owned()))
}

/// An invitation link to create: [`KitApi::create_invitation_link`].
#[derive(Clone, Debug, Default)]
pub struct NewInvitationLink {
    /// The groups the people who join get.
    pub groups: Vec<InvitedGroup>,
    pub label: Option<String>,
    /// Until when it works (ms since epoch); `None`: until revoked.
    pub expires_at: Option<i64>,
    /// How many people may join through it; `None`: no limit.
    pub max_uses: Option<i64>,
}

/// A signed-in session: its tokens and who it is.
#[derive(Clone, Debug, Serialize)]
pub struct SignedIn {
    pub access_token: String,
    pub refresh_token: String,
    pub account_id: String,
    pub session_id: String,
    /// The account has a second factor on: prove it before anything else.
    pub second_factor: bool,
}

/// A social OAuth/OIDC provider; a tenant's SSO is configured as `account.sso`. An OpenID
/// Connect provider is found by its `issuer`; a plain OAuth2 one (GitHub) names its endpoints in
/// `oauth2` ([`Provider::github`]).
#[derive(Clone, Debug)]
pub struct Provider {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    pub oauth2: Option<OAuth2>,
}

/// The endpoints of an OAuth2 provider without OpenID Connect: where the browser signs in, where
/// the code becomes an access token, and where the person (`user`: its `id` is the subject) and
/// their email addresses (`emails`: the primary verified one counts) are read.
#[derive(Clone, Debug)]
pub struct OAuth2 {
    pub authorize: String,
    pub token: String,
    pub user: String,
    pub emails: String,
    pub scope: String,
}

impl Provider {
    /// An OpenID Connect provider at `issuer` (discovery at `<issuer>/.well-known/openid-configuration`).
    pub fn oidc(issuer: &str, client_id: &str, client_secret: &str) -> Provider {
        Provider {
            issuer: issuer.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            oauth2: None,
        }
    }

    /// GitHub (an OAuth app's client): `web` `https://github.com` and `api`
    /// `https://api.github.com`, or a GitHub Enterprise Server's.
    pub fn github(client_id: &str, client_secret: &str, web: &str, api: &str) -> Provider {
        let (web, api) = (web.trim_end_matches('/'), api.trim_end_matches('/'));
        Provider {
            issuer: web.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            oauth2: Some(OAuth2 {
                authorize: format!("{web}/login/oauth/authorize"),
                token: format!("{web}/login/oauth/access_token"),
                user: format!("{api}/user"),
                emails: format!("{api}/user/emails"),
                scope: "read:user user:email".into(),
            }),
        }
    }
}

type Txt = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Vec<String>> + Send>> + Send + Sync>;

/// What `oidc_start` keeps in the cache until the browser comes back.
#[derive(Serialize, Deserialize)]
struct OidcState {
    provider: String,
    tenant: Option<String>,
    sso_id: Option<String>,
    verifier: String,
    nonce: String,
    link: Option<String>,
}

/// What only your API does. Make it with [`KitApi::new`], then set providers and the redirect URI.
#[derive(Clone)]
pub struct KitApi {
    kernable: Kernable,
    keys: SigningKeys,
    /// The request context of the API's own requests: lookups no one else may make.
    pub service_context: RequestContext,
    providers: HashMap<String, Provider>,
    redirect_uri: Option<String>,
    insecure_issuers: bool,
    resolve_txt: Option<Txt>,
    http: reqwest::Client,
    configs: Arc<Mutex<HashMap<String, Value>>>,
    api_keys: Arc<Mutex<HashMap<String, (RequestContext, Instant)>>>,
}

fn events(outcome: &Outcome) -> &[StoredEvent] {
    outcome.events()
}

fn first<'a>(outcome: &'a Outcome, r#type: &str) -> Option<&'a Value> {
    events(outcome)
        .iter()
        .find(|e| e.event_type == r#type)
        .map(|e| &e.data)
}

/// Why a sign-in that committed did not sign in, as the caller hears it: its `SignInFailed`
/// reason, except that a locked account answers `wrong_password` like an unknown address, so
/// nobody learns from it that an account exists (the event keeps `locked`).
fn failed_reason(outcome: &Outcome) -> String {
    match events(outcome)
        .first()
        .and_then(|e| e.data["reason"].as_str())
        .unwrap_or("wrong_password")
    {
        "locked" => "wrong_password".to_owned(),
        reason => reason.to_owned(),
    }
}

/// A refusal of a command that requires the tenant active (its fact): missing, the tenant is
/// pending, suspended or gone, which the requirement does not tell apart.
fn joined(outcome: Outcome) -> ApiResult<()> {
    match outcome {
        Outcome::Rejected(r) if r.code == "requirement_missing" => refused("tenant_not_active"),
        outcome => done(outcome),
    }
}

fn done(outcome: Outcome) -> ApiResult<()> {
    match outcome {
        Outcome::Committed(_) => Ok(()),
        Outcome::Rejected(r) => refused(&r.code),
    }
}

fn factors(data: &Value) -> Vec<String> {
    data["factors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f.as_str().map(str::to_owned))
        .collect()
}

impl KitApi {
    /// `kernable`: a client whose key may submit and query (it signs every request). `keys`: what
    /// access tokens are signed with. `service`: the API's own identity and key ID, from the bootstrap.
    pub fn new(
        kernable: Kernable,
        keys: SigningKeys,
        service_identity: &str,
        service_key_id: &str,
    ) -> KitApi {
        KitApi {
            kernable,
            keys,
            service_context: RequestContext {
                sender_tenant: PLATFORM_TENANT.into(),
                sender_identity: service_identity.into(),
                sender_session: service_key_id.into(),
                ..RequestContext::default()
            },
            providers: HashMap::new(),
            redirect_uri: None,
            insecure_issuers: false,
            resolve_txt: None,
            http: reqwest::Client::new(),
            configs: Arc::default(),
            api_keys: Arc::default(),
        }
    }

    /// A social OAuth/OIDC provider by name (`google`).
    pub fn provider(mut self, name: &str, provider: Provider) -> KitApi {
        self.providers.insert(name.to_owned(), provider);
        self
    }

    /// Where providers send the browser back to; your route calls `oidc_finish` with the full URL.
    pub fn redirect_uri(mut self, uri: &str) -> KitApi {
        self.redirect_uri = Some(uri.to_owned());
        self
    }

    /// Allows http:// issuers: for development and tests only.
    pub fn insecure_issuers(mut self) -> KitApi {
        self.insecure_issuers = true;
        self
    }

    /// How DNS TXT records are read (tests replace it); default: the system's resolver.
    pub fn resolve_txt<F, Fut>(mut self, f: F) -> KitApi
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Vec<String>> + Send + 'static,
    {
        self.resolve_txt = Some(Arc::new(move |name| Box::pin(f(name))));
        self
    }

    // --- small helpers ------------------------------------------------------------------------------

    async fn submit<C: CommandType>(
        &self,
        command: C,
        aggregate_id: &str,
        tenant: &str,
        context: RequestContext,
    ) -> ApiResult<Outcome> {
        Ok(self
            .kernable
            .submit(
                Submit::command(command, aggregate_id)
                    .tenant(tenant)
                    .context(context),
            )
            .await?)
    }

    async fn platform<C: CommandType>(
        &self,
        command: C,
        aggregate_id: &str,
        context: RequestContext,
    ) -> ApiResult<Outcome> {
        self.submit(command, aggregate_id, PLATFORM_TENANT, context)
            .await
    }

    /// The API's own lookup; `Ok(None)` when the read model refuses (not found).
    async fn ask<Q: QueryType>(
        &self,
        query: Q,
        tenant: &str,
        all_partitions: bool,
    ) -> ApiResult<Option<Q::Answer>> {
        let mut q = Query::of(query)
            .tenant(tenant)
            .context(self.service_context.clone());
        if all_partitions {
            q = q.all_partitions();
        }
        match self.kernable.query(q).await {
            Ok(answer) => Ok(Some(answer)),
            Err(Error::Rejected(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The context of the token's account acting on itself.
    fn own(claims: &Claims) -> RequestContext {
        RequestContext {
            sender_account: claims.sub.clone(),
            sender_session: claims.sid.clone(),
            ..RequestContext::default()
        }
    }

    /// The token's account, read as itself: the middleware checks the session as for every
    /// signed-in request (active, the account active, a second factor proved if it has one). For
    /// what the API then does with its own context (accepting an invitation, joining through a link).
    async fn signed_in_account(&self, claims: &Claims) -> ApiResult<AccountRow> {
        let q = Query::of(GetAccount {
            account_id: claims.sub.clone(),
        })
        .tenant(PLATFORM_TENANT)
        .context(Self::own(claims));
        match self.kernable.query(q).await {
            Ok(account) => Ok(account),
            Err(Error::Rejected(r)) => refused(&r.code),
            Err(e) => Err(e.into()),
        }
    }

    /// The token's account checked against a tenant it is about to join, where it holds no identity
    /// yet: the middleware applies that tenant's checks to the account acting on itself (the tenant
    /// active, its second-factor requirement, its enforced SSO). A cheap query of one's own
    /// sessions in that tenant carries the check; its answer is not used.
    async fn may_join(&self, claims: &Claims, tenant: &str) -> ApiResult<()> {
        let q = Query::of(ListSessions {
            account_id: claims.sub.clone(),
        })
        .tenant(tenant)
        .context(Self::own(claims));
        match self.kernable.query::<serde::de::IgnoredAny>(q).await {
            Ok(_) => Ok(()),
            Err(Error::Rejected(r)) => refused(&r.code),
            Err(e) => Err(e.into()),
        }
    }

    fn signed_in(
        &self,
        account_id: &str,
        session_id: &str,
        refresh_token: String,
        data: &Value,
        second_factor: Option<bool>,
    ) -> SignedIn {
        SignedIn {
            access_token: issue_access_token(
                &self.keys,
                account_id,
                session_id,
                &factors(data),
                None,
            ),
            refresh_token,
            account_id: account_id.to_owned(),
            session_id: session_id.to_owned(),
            second_factor: second_factor.unwrap_or(data["mfa"] == Value::Bool(true)),
        }
    }

    // --- registration and sign-in ---------------------------------------------------------------------

    /// Registers a person. Answers the same whether or not the email was taken: the caller
    /// shows "check your email" either way. The account ID it returns is the new one, or a decoy.
    pub async fn register(
        &self,
        email: &str,
        name: &str,
        password: Option<&str>,
    ) -> ApiResult<String> {
        let command = || RegisterAccount {
            email: email.into(),
            name: name.into(),
            password: password.map(Into::into),
            verified: None,
        };
        let account_id = new_id();
        match self
            .platform(command(), &account_id, RequestContext::default())
            .await?
        {
            Outcome::Committed(_) => return Ok(account_id),
            Outcome::Rejected(r) if r.code != "unique_taken" => return refused(&r.code),
            Outcome::Rejected(_) => {}
        }
        // The address is held: a registration that expired unverified is taken over (same
        // aggregate); otherwise the same answer as success, so nobody learns who has an account.
        if let Some(held) = self
            .ask(
                FindAccountByEmail {
                    email: email.into(),
                },
                PLATFORM_TENANT,
                false,
            )
            .await?
        {
            if held.status == crate::account::AccountStatus::Pending {
                if self
                    .platform(command(), &held.id, RequestContext::default())
                    .await?
                    .is_committed()
                {
                    return Ok(held.id);
                }
            } else {
                // The account's owner learns that someone tried; the caller learns nothing.
                self.platform(
                    NoteRegistrationAttempt {},
                    &held.id,
                    self.service_context.clone(),
                )
                .await?;
            }
        }
        Ok(new_id())
    }

    /// Confirms an email address with the token from the mail.
    pub async fn verify_email(&self, email_token: &str) -> ApiResult<bool> {
        let Some((account_id, _, _)) = parse_token(email_token) else {
            return Ok(false);
        };
        Ok(self
            .platform(
                VerifyEmail {
                    token: email_token.into(),
                },
                &account_id,
                RequestContext::default(),
            )
            .await?
            .is_committed())
    }

    /// Signs in with email and password: an access token and a refresh token, or why not. An
    /// unknown address and a locked account answer `wrong_password`, and an unknown address takes
    /// as long (a password check against a dummy hash), so nobody learns whether an account exists.
    pub async fn sign_in(
        &self,
        email: &str,
        password: &str,
        ip: Option<&str>,
        user_agent: Option<&str>,
    ) -> ApiResult<SignedIn> {
        // Never say whether the account exists.
        let Some(account) = self
            .ask(
                FindAccountByEmail {
                    email: email.into(),
                },
                PLATFORM_TENANT,
                false,
            )
            .await?
        else {
            // As slow as a wrong password; argon2 runs off the async workers, like the
            // handlers' own checks (the SDK runs command handlers on blocking threads).
            let password = password.to_owned();
            let _ = tokio::task::spawn_blocking(move || check_password(None, &password)).await;
            return refused("wrong_password");
        };
        let session_id = new_id();
        let refresh_token = token(&session_id, 1, &random_secret(32));
        let command = SignIn {
            session_id: session_id.clone(),
            password: password.into(),
            refresh_token: refresh_token.clone(),
            ip: ip.map(Into::into),
            user_agent: user_agent.map(Into::into),
        };
        let outcome = self
            .platform(command, &account.id, RequestContext::default())
            .await?;
        if let Outcome::Rejected(r) = &outcome {
            return refused(&r.code);
        }
        match first(&outcome, "SignedIn") {
            Some(data) => Ok(self.signed_in(&account.id, &session_id, refresh_token, data, None)),
            None => refused(&failed_reason(&outcome)),
        }
    }

    /// The claims of a valid access token, else `None`.
    pub fn check(&self, access_token: &str) -> Option<Claims> {
        check_access_token(&self.keys, access_token, None)
    }

    /// The request context for the token's holder, in `tenant` (its identity there) or, without a
    /// tenant, only as a signed-in account (for `authenticated` requests). `not_found` without an
    /// identity in the tenant.
    pub async fn context_for(
        &self,
        claims: &Claims,
        tenant: Option<&str>,
    ) -> ApiResult<RequestContext> {
        let Some(tenant) = tenant else {
            return Ok(Self::own(claims));
        };
        let Some(found) = self
            .ask(
                FindIdentityByAccount {
                    account_id: claims.sub.clone(),
                },
                tenant,
                false,
            )
            .await?
        else {
            return refused("not_found");
        };
        Ok(RequestContext {
            sender_tenant: tenant.into(),
            sender_identity: found.identity_id,
            ..Self::own(claims)
        })
    }

    /// A new access token and a rotated refresh token, in that order. An older refresh
    /// token is a reuse: the session ends, and so does every token of it.
    pub async fn refresh(
        &self,
        refresh_token: &str,
        ip: Option<&str>,
    ) -> ApiResult<(String, String)> {
        let Some((session_id, generation, _)) = parse_token(refresh_token) else {
            return refused("invalid_token");
        };
        let Some(generation) = generation
            .bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| generation.parse::<u64>().ok())
            .flatten()
        else {
            return refused("invalid_token");
        };
        let next = token(&session_id, generation + 1, &random_secret(32));
        let command = RefreshSession {
            refresh_token: refresh_token.into(),
            new_refresh_token: next.clone(),
            ip: ip.map(Into::into),
        };
        let outcome = self
            .platform(command, &session_id, RequestContext::default())
            .await?;
        if let Outcome::Rejected(r) = &outcome {
            return refused(&r.code);
        }
        let Some(d) = first(&outcome, "SessionRefreshed") else {
            return refused(
                events(&outcome)
                    .first()
                    .and_then(|e| e.data["reason"].as_str())
                    .unwrap_or("invalid_token"),
            );
        };
        let account_id = d["accountId"].as_str().unwrap_or_default();
        Ok((
            issue_access_token(&self.keys, account_id, &session_id, &factors(d), None),
            next,
        ))
    }

    /// Proves the second factor (a TOTP or recovery code) for the session: a new access token with it.
    pub async fn prove_second_factor(&self, claims: &Claims, code: &str) -> ApiResult<String> {
        let outcome = self
            .platform(
                ProveSecondFactor {
                    session_id: claims.sid.clone(),
                    code: code.into(),
                },
                &claims.sub,
                RequestContext::default(),
            )
            .await?;
        if let Outcome::Rejected(r) = &outcome {
            return refused(&r.code);
        }
        let Some(proved) = first(&outcome, "SecondFactorProved") else {
            return refused("wrong_code");
        };
        let factor = if proved["method"] == "recovery" {
            "recovery"
        } else {
            "totp"
        };
        let mut fac = claims.fac.clone();
        fac.push(factor.into());
        Ok(issue_access_token(
            &self.keys,
            &claims.sub,
            &claims.sid,
            &fac,
            None,
        ))
    }

    /// Starts TOTP: a new secret, and the `otpauth:` URI an authenticator app reads (as a QR code).
    pub async fn start_totp(
        &self,
        claims: &Claims,
        issuer: &str,
        label: &str,
    ) -> ApiResult<(String, String)> {
        let secret = totp_secret();
        done(
            self.platform(
                StartTotp {
                    secret: secret.clone(),
                },
                &claims.sub,
                Self::own(claims),
            )
            .await?,
        )?;
        let uri = format!(
            "otpauth://totp/{}?secret={secret}&issuer={}",
            percent(&format!("{issuer}:{label}")),
            percent(issuer)
        );
        Ok((secret, uri))
    }

    /// Turns TOTP on with its first code: the recovery codes, shown this once.
    pub async fn confirm_totp(&self, claims: &Claims, code: &str) -> ApiResult<Vec<String>> {
        let recovery_codes: Vec<String> = (0..10).map(|_| random_secret(8)).collect();
        let command = ConfirmTotp {
            code: code.into(),
            recovery_codes: recovery_codes.clone(),
        };
        done(
            self.platform(command, &claims.sub, Self::own(claims))
                .await?,
        )?;
        Ok(recovery_codes)
    }

    // --- API keys ----------------------------------------------------------------------------------------

    /// A new API key for a service identity, shown this once: the key and its ID.
    pub async fn create_api_key(
        &self,
        context: RequestContext,
        tenant: &str,
        identity_id: &str,
        label: &str,
        expires_at: Option<i64>,
    ) -> ApiResult<(String, String)> {
        let key_id: String = random_secret(9)
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { 'x' })
            .collect();
        let secret = random_secret(32);
        let command = CreateApiKey {
            key_id: key_id.clone(),
            secret: secret.clone(),
            label: Some(label.into()),
            expires_at,
        };
        done(self.submit(command, identity_id, tenant, context).await?)?;
        Ok((api_key(&key_id, &secret), key_id))
    }

    /// The request context of an API key, or `None`: checked against its hash, cached for a
    /// minute. A revoked key is refused by the middleware at once, cache or not.
    pub async fn check_api_key(&self, key: &str) -> ApiResult<Option<RequestContext>> {
        let Some((key_id, hash)) = parse_api_key(key) else {
            return Ok(None);
        };
        if let Some((context, until)) = self.api_keys.lock().unwrap().get(&hash).cloned()
            && until > Instant::now()
        {
            return Ok(Some(context));
        }
        let Some(found) = self
            .ask(
                FindApiKey {
                    key_id: key_id.clone(),
                },
                PLATFORM_TENANT,
                true,
            )
            .await?
        else {
            return Ok(None);
        };
        let expired = found
            .expires_at
            .is_some_and(|e| Timestamp::now().millis() > e);
        if found.revoked || expired || !equal_secrets(&found.hash, &hash) {
            return Ok(None);
        }
        let context = RequestContext {
            sender_tenant: found.tenant,
            sender_identity: found.identity_id,
            sender_session: key_id,
            ..RequestContext::default()
        };
        self.api_keys.lock().unwrap().insert(
            hash,
            (context.clone(), Instant::now() + Duration::from_secs(60)),
        );
        Ok(Some(context))
    }

    // --- Passkeys: challenges live a few minutes in the kernable cache and count once. --------------

    async fn keep(&self, key: &str, value: &str) -> ApiResult<()> {
        let cache = self
            .kernable
            .cache()
            .tenant(PLATFORM_TENANT)
            .context(self.service_context.clone());
        cache
            .set(
                "account",
                key,
                value,
                Some(Duration::from_secs(CHALLENGE_SECONDS as u64)),
            )
            .await?;
        Ok(())
    }

    /// The cached value, deleted: it counts once.
    async fn take(&self, key: &str) -> ApiResult<Option<Vec<u8>>> {
        let cache = self
            .kernable
            .cache()
            .tenant(PLATFORM_TENANT)
            .context(self.service_context.clone());
        let Some(value) = cache.get("account", key).await? else {
            return Ok(None);
        };
        Ok(cache.delete("account", key).await?.then_some(value))
    }

    /// The challenge a browser response answers, if it is one the API issued and nobody used yet.
    async fn take_challenge(&self, response: &Value) -> ApiResult<Option<String>> {
        let challenge = response["response"]["clientDataJSON"]
            .as_str()
            .and_then(|c| {
                BASE64URL_NOPAD
                    .decode(c.trim_end_matches('=').as_bytes())
                    .ok()
            })
            .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
            .and_then(|d| d["challenge"].as_str().map(str::to_owned));
        let Some(challenge) = challenge else {
            return Ok(None);
        };
        Ok(self
            .take(&format!("challenge:{challenge}"))
            .await?
            .map(|_| challenge))
    }

    async fn new_challenge(&self) -> ApiResult<String> {
        let challenge = BASE64URL_NOPAD.encode(&random_bytes(32));
        self.keep(&format!("challenge:{challenge}"), "1").await?;
        Ok(challenge)
    }

    /// What the browser needs to create a passkey for the signed-in account (JSON for
    /// `navigator.credentials.create`, as `PublicKeyCredentialCreationOptionsJSON`).
    pub async fn passkey_registration_options(
        &self,
        claims: &Claims,
        user_name: &str,
    ) -> ApiResult<Value> {
        let rp = passkeys();
        Ok(json!({
            "challenge": self.new_challenge().await?,
            "rp": { "name": rp.rp_name, "id": rp.rp_id },
            "user": { "id": BASE64URL_NOPAD.encode(claims.sub.as_bytes()), "name": user_name, "displayName": user_name },
            "pubKeyCredParams": [{ "alg": -7, "type": "public-key" }],
            "timeout": CHALLENGE_SECONDS * 1000,
            "attestation": "none",
            "authenticatorSelection": { "residentKey": "required", "requireResidentKey": true, "userVerification": "required" },
        }))
    }

    /// Adds the passkey the browser created.
    pub async fn add_passkey(
        &self,
        claims: &Claims,
        response: Value,
        label: Option<&str>,
    ) -> ApiResult<()> {
        let Some(challenge) = self.take_challenge(&response).await? else {
            return refused("invalid_challenge");
        };
        done(
            self.platform(
                AddPasskey {
                    challenge,
                    response,
                    label: label.map(Into::into),
                },
                &claims.sub,
                Self::own(claims),
            )
            .await?,
        )
    }

    /// What the browser needs to sign in with a passkey (or prove one): any of the user's passkeys.
    pub async fn passkey_sign_in_options(&self) -> ApiResult<Value> {
        Ok(
            json!({ "challenge": self.new_challenge().await?, "rpId": passkeys().rp_id, "timeout": CHALLENGE_SECONDS * 1000, "userVerification": "required" }),
        )
    }

    /// Signs in with a passkey, without a password: the passkey names its account (its user handle).
    pub async fn sign_in_with_passkey(
        &self,
        response: Value,
        ip: Option<&str>,
    ) -> ApiResult<SignedIn> {
        let challenge = self.take_challenge(&response).await?;
        let account_id = response["response"]["userHandle"]
            .as_str()
            .and_then(|h| {
                BASE64URL_NOPAD
                    .decode(h.trim_end_matches('=').as_bytes())
                    .ok()
            })
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let Some(challenge) = challenge.filter(|_| !account_id.is_empty()) else {
            return refused("wrong_password");
        };
        let session_id = new_id();
        let refresh_token = token(&session_id, 1, &random_secret(32));
        let command = SignInWithPasskey {
            session_id: session_id.clone(),
            challenge,
            response,
            refresh_token: refresh_token.clone(),
            ip: ip.map(Into::into),
            user_agent: None,
        };
        let outcome = self
            .platform(command, &account_id, self.service_context.clone())
            .await?;
        if !outcome.is_committed() {
            return refused("wrong_password");
        }
        match first(&outcome, "SignedIn") {
            Some(data) => {
                Ok(self.signed_in(&account_id, &session_id, refresh_token, data, Some(false)))
            }
            None => refused(&failed_reason(&outcome)),
        }
    }

    /// Proves a passkey as the second factor of a password session: a new access token with it.
    pub async fn prove_passkey(&self, claims: &Claims, response: Value) -> ApiResult<String> {
        let Some(challenge) = self.take_challenge(&response).await? else {
            return refused("invalid_challenge");
        };
        let outcome = self
            .platform(
                ProvePasskey {
                    session_id: claims.sid.clone(),
                    challenge,
                    response,
                },
                &claims.sub,
                self.service_context.clone(),
            )
            .await?;
        if first(&outcome, "SecondFactorProved").is_none() {
            return refused("wrong_passkey");
        }
        let mut fac = claims.fac.clone();
        fac.push("passkey".into());
        Ok(issue_access_token(
            &self.keys,
            &claims.sub,
            &claims.sid,
            &fac,
            None,
        ))
    }

    // --- OAuth/OIDC and SSO: the API does the exchange; the account decides. ------------------------

    /// The provider's discovery document with its keys, kept once fetched.
    async fn config(&self, issuer: &str) -> Result<Value, String> {
        if !self.insecure_issuers && !issuer.starts_with("https://") {
            return Err(format!("{issuer} is not https"));
        }
        if let Some(c) = self.configs.lock().unwrap().get(issuer) {
            return Ok(c.clone());
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );
        let mut config: Value = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        // The endpoints it names carry the client secret, codes and tokens: https too.
        for key in ["jwks_uri", "token_endpoint", "authorization_endpoint"] {
            if let Some(url) = config[key].as_str()
                && !self.secure(url)
            {
                return Err(format!("{key} {url} is not https"));
            }
        }
        let jwks_uri = config["jwks_uri"].as_str().ok_or("no jwks_uri")?.to_owned();
        config["jwks"] = self
            .http
            .get(jwks_uri)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        self.configs
            .lock()
            .unwrap()
            .insert(issuer.to_owned(), config.clone());
        Ok(config)
    }

    /// A tenant's SSO: its provider name, what the API signs in with, and the domains it verified.
    async fn sso_settings(
        &self,
        tenant: &str,
        sso_id: &str,
    ) -> ApiResult<Option<(String, Provider, Vec<String>)>> {
        let found = self
            .ask(
                GetSsoSecret {
                    sso_id: sso_id.into(),
                },
                tenant,
                false,
            )
            .await?;
        Ok(found.and_then(|s| {
            let client_secret = s.client_secret.filter(|c| !c.is_empty())?;
            Some((
                format!("sso:{sso_id}"),
                Provider {
                    issuer: s.issuer,
                    client_id: s.client_id,
                    client_secret,
                    oauth2: None,
                },
                s.domains,
            ))
        }))
    }

    /// Where to send the browser: a social provider by name, or a person's tenant SSO by their
    /// email. With `link`, the signed-in account links the login instead of signing in.
    pub async fn oidc_start(
        &self,
        provider: Option<&str>,
        email: Option<&str>,
        link: Option<&Claims>,
    ) -> ApiResult<String> {
        let (name, p, tenant, sso_id) = if let Some(name) = provider {
            let Some(p) = self.providers.get(name) else {
                return refused("no_provider");
            };
            (name.to_owned(), p.clone(), None, None)
        } else {
            let domain = email_domain(email.unwrap_or_default());
            let Some(found) = self
                .ask(FindSsoByDomain { domain }, PLATFORM_TENANT, true)
                .await?
            else {
                return refused("no_provider");
            };
            let Some((name, p, _)) = self.sso_settings(&found.tenant, &found.sso_id).await? else {
                return refused("no_provider");
            };
            (name, p, Some(found.tenant), Some(found.sso_id))
        };
        let Some(redirect_uri) = &self.redirect_uri else {
            return refused("no_provider");
        };
        let (verifier, state, nonce) = (random_secret(48), random_secret(24), random_secret(24));
        let (authorize, scope) = match &p.oauth2 {
            Some(o) => {
                if !self.secure(&o.authorize) {
                    return refused("no_provider");
                }
                (o.authorize.clone(), o.scope.clone())
            }
            None => {
                let config = self
                    .config(&p.issuer)
                    .await
                    .map_err(|_| ApiError::Refused("no_provider".into()))?;
                (
                    config["authorization_endpoint"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    "openid email".to_owned(),
                )
            }
        };
        let oauth2 = p.oauth2.is_some();
        let kept = OidcState {
            provider: name,
            tenant,
            sso_id,
            verifier: verifier.clone(),
            nonce: nonce.clone(),
            link: link.map(|c| c.sub.clone()),
        };
        self.keep(
            &format!("oidc:{state}"),
            &serde_json::to_string(&kept).expect("JSON"),
        )
        .await?;
        let challenge = BASE64URL_NOPAD.encode(&Sha256::digest(verifier.as_bytes()));
        let mut url =
            reqwest::Url::parse(&authorize).map_err(|_| ApiError::Refused("no_provider".into()))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("response_type", "code")
                .append_pair("client_id", &p.client_id)
                .append_pair("redirect_uri", redirect_uri)
                .append_pair("scope", &scope)
                .append_pair("state", &state);
            // Only OpenID Connect knows a nonce (it comes back in the ID token).
            if !oauth2 {
                q.append_pair("nonce", &nonce);
            }
            q.append_pair("code_challenge_method", "S256")
                .append_pair("code_challenge", &challenge);
        }
        Ok(url.to_string())
    }

    /// Whether `url` may be used: https, or anything with `insecure_issuers` (tests).
    fn secure(&self, url: &str) -> bool {
        self.insecure_issuers || url.starts_with("https://")
    }

    /// For an OAuth2 provider: the person behind the code in `callback`, as claims an ID token
    /// would hold (`sub`: their `id`; `email` and `email_verified`: their primary verified address,
    /// else none).
    async fn exchange_oauth2(
        &self,
        p: &Provider,
        o: &OAuth2,
        code: &str,
        kept: &OidcState,
    ) -> Result<Value, String> {
        for url in [&o.token, &o.user, &o.emails] {
            if !self.secure(url) {
                return Err(format!("{url} is not https"));
            }
        }
        let form = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", p.client_id.as_str()),
            ("client_secret", p.client_secret.as_str()),
            (
                "redirect_uri",
                self.redirect_uri.as_deref().unwrap_or_default(),
            ),
            ("code_verifier", kept.verifier.as_str()),
        ];
        let tokens: Value = self
            .http
            .post(&o.token)
            .header("accept", "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        let access = tokens["access_token"]
            .as_str()
            .ok_or_else(|| format!("no access_token: {}", tokens["error"]))?;
        let get = |url: &str| {
            self.http
                .get(url)
                .bearer_auth(access)
                .header("accept", "application/json")
                .header("user-agent", "kernable-kit")
                .send()
        };
        let user: Value = get(&o.user)
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        let sub = match &user["id"] {
            Value::Number(n) => n.to_string(),
            Value::String(s) if !s.is_empty() => s.clone(),
            _ => return Err("no id".into()),
        };
        let emails: Value = get(&o.emails)
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        let email = oauth2_email(&emails);
        Ok(serde_json::json!({
            "sub": sub,
            "email": email.clone().unwrap_or_default(),
            "email_verified": email.is_some(),
        }))
    }

    /// The ID token's claims for the code in `callback`, checked (issuer, audience, nonce, expiry);
    /// for an OAuth2 provider, the same claims read from it ([`KitApi::exchange_oauth2`]).
    async fn exchange(
        &self,
        p: &Provider,
        callback: &reqwest::Url,
        kept: &OidcState,
    ) -> Result<Value, String> {
        let code = callback
            .query_pairs()
            .find(|(k, _)| k == "code")
            .map(|(_, v)| v.into_owned())
            .ok_or("no code")?;
        if let Some(o) = &p.oauth2 {
            return self.exchange_oauth2(p, o, &code, kept).await;
        }
        let config = self.config(&p.issuer).await?;
        let form = [
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            (
                "redirect_uri",
                self.redirect_uri.as_deref().unwrap_or_default(),
            ),
            ("code_verifier", kept.verifier.as_str()),
        ];
        let res = self
            .http
            .post(
                config["token_endpoint"]
                    .as_str()
                    .ok_or("no token_endpoint")?,
            )
            .basic_auth(percent(&p.client_id), Some(percent(&p.client_secret)))
            .form(&form)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            return Err(format!("token endpoint: {}", res.status()));
        }
        let tokens: Value = res.json().await.map_err(|e| e.to_string())?;
        let id_token = tokens["id_token"].as_str().ok_or("no id_token")?;
        let header = jsonwebtoken::decode_header(id_token).map_err(|e| e.to_string())?;
        let jwks: jsonwebtoken::jwk::JwkSet =
            serde_json::from_value(config["jwks"].clone()).map_err(|e| e.to_string())?;
        let jwk = match &header.kid {
            Some(kid) => jwks.find(kid),
            None => jwks.keys.first(),
        }
        .ok_or("no key for the token")?;
        let key = jsonwebtoken::DecodingKey::from_jwk(jwk).map_err(|e| e.to_string())?;
        let mut validation = jsonwebtoken::Validation::new(header.alg);
        validation.set_audience(&[&p.client_id]);
        validation.set_issuer(&[config["issuer"].as_str().unwrap_or_default()]);
        let claims = jsonwebtoken::decode::<Value>(id_token, &key, &validation)
            .map_err(|e| e.to_string())?
            .claims;
        if claims["nonce"].as_str() != Some(kept.nonce.as_str()) {
            return Err("another nonce".into());
        }
        Ok(claims)
    }

    /// The browser came back (`callback` is the full URL). Signs in a linked login (`Some`); links
    /// a tenant SSO login to the account with the same verified email; registers a new person
    /// whose email nobody has; refuses a social login whose email has an account (`not_linked`:
    /// sign in and link it). A tenant's SSO answers only for its verified domains: an unlinked
    /// login with another email is refused (`email_not_in_domain`), so a tenant's provider cannot
    /// take over anyone else's account. `Ok(None)`: the signed-in account linked the login
    /// (`oidc_start` with `link`).
    pub async fn oidc_finish(
        &self,
        callback: &str,
        ip: Option<&str>,
    ) -> ApiResult<Option<SignedIn>> {
        let Ok(url) = reqwest::Url::parse(callback) else {
            return refused("invalid_state");
        };
        let state = url
            .query_pairs()
            .find(|(k, _)| k == "state")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();
        let raw = if state.is_empty() {
            None
        } else {
            self.take(&format!("oidc:{state}")).await?
        };
        let Some(kept) = raw.and_then(|r| serde_json::from_slice::<OidcState>(&r).ok()) else {
            return refused("invalid_state");
        };
        let p = match (&kept.tenant, &kept.sso_id) {
            (Some(tenant), Some(sso_id)) => self
                .sso_settings(tenant, sso_id)
                .await?
                .map(|(_, p, domains)| (p, Some(domains))),
            _ => self
                .providers
                .get(&kept.provider)
                .cloned()
                .map(|p| (p, None)),
        };
        let Some((p, domains)) = p else {
            return refused("no_provider");
        };
        let Ok(claims) = self.exchange(&p, &url, &kept).await else {
            return refused("provider_refused");
        };
        let email = claims["email"].as_str().unwrap_or_default().to_owned();
        let email_verified = claims["email_verified"] == Value::Bool(true);
        let subject = claims["sub"].as_str().unwrap_or_default().to_owned();
        let provider = kept.provider.clone();
        let link = |account: &str| {
            let command = LinkProvider {
                provider: provider.clone(),
                subject: subject.clone(),
                email: Some(email.clone()),
            };
            let account = account.to_owned();
            async move {
                self.platform(command, &account, self.service_context.clone())
                    .await
            }
        };
        if let Some(account) = &kept.link {
            done(link(account).await?)?;
            return Ok(None);
        }
        let by_link = self
            .ask(
                FindAccountByLink {
                    provider: provider.clone(),
                    subject: subject.clone(),
                },
                PLATFORM_TENANT,
                false,
            )
            .await?;
        let sso = provider.starts_with("sso:");
        // Not linked yet: a tenant's SSO speaks only for the domains its tenant verified.
        if by_link.is_none()
            && sso
            && !domains
                .as_deref()
                .unwrap_or_default()
                .contains(&email_domain(&email))
        {
            return refused("email_not_in_domain");
        }
        let by_email = if by_link.is_none() && !email.is_empty() {
            self.ask(
                FindAccountByEmail {
                    email: email.clone(),
                },
                PLATFORM_TENANT,
                false,
            )
            .await?
        } else {
            None
        };
        let account_id = match by_link.or(by_email) {
            Some(found) => found.id,
            None => {
                // A new person: registered (verified if the provider says so), the login linked.
                if email.is_empty() {
                    return refused("no_email");
                }
                // A social login registers only where anyone may: with sign-up by invitation,
                // the person accepts an invitation first and links the login after. A tenant's
                // SSO speaks for its own verified domains, so it registers either way.
                if !sso {
                    let policy = self.ask(GetSignUpPolicy {}, PLATFORM_TENANT, false).await?;
                    if policy.map(|p| p.accounts) != Some(AccountSignUp::Open) {
                        return refused("sign_up_closed");
                    }
                }
                let account_id = new_id();
                let name = email.split('@').next().unwrap_or_default().to_owned();
                let register = RegisterAccount {
                    email: email.clone(),
                    name,
                    password: None,
                    verified: Some(email_verified),
                };
                done(
                    self.platform(register, &account_id, self.service_context.clone())
                        .await?,
                )?;
                if !sso {
                    link(&account_id).await?;
                }
                account_id
            }
        };
        let session_id = new_id();
        let refresh_token = token(&session_id, 1, &random_secret(32));
        let command = SignInWithProvider {
            session_id: session_id.clone(),
            provider,
            subject,
            email: Some(email),
            email_verified: Some(email_verified),
            auto_link: Some(sso),
            domains,
            refresh_token: refresh_token.clone(),
            ip: ip.map(Into::into),
            user_agent: None,
        };
        let outcome = self
            .platform(command, &account_id, self.service_context.clone())
            .await?;
        if let Outcome::Rejected(r) = &outcome {
            return refused(&r.code);
        }
        match first(&outcome, "SignedIn") {
            Some(data) => Ok(Some(self.signed_in(
                &account_id,
                &session_id,
                refresh_token,
                data,
                None,
            ))),
            None => refused(&failed_reason(&outcome)),
        }
    }

    /// Looks for the SSO domain's TXT record (`_kernable-sso.<domain>`) and verifies the domain if it is there.
    pub async fn verify_sso_domain(
        &self,
        tenant: &str,
        sso_id: &str,
        domain: &str,
    ) -> ApiResult<()> {
        let name = format!("_kernable-sso.{domain}");
        let found = match &self.resolve_txt {
            Some(f) => f(name).await,
            None => system_txt(&name).await,
        };
        done(
            self.submit(
                VerifySsoDomain {
                    domain: domain.into(),
                    found,
                },
                sso_id,
                tenant,
                self.service_context.clone(),
            )
            .await?,
        )
    }

    // --- passwords and email -----------------------------------------------------------------------------

    /// Changes the password; the other sessions end, this one stays.
    pub async fn change_password(
        &self,
        claims: &Claims,
        current: Option<&str>,
        new: &str,
    ) -> ApiResult<()> {
        let command = ChangePassword {
            current: current.map(Into::into),
            new: new.into(),
            keep_session_id: Some(claims.sid.clone()),
        };
        done(
            self.platform(command, &claims.sub, Self::own(claims))
                .await?,
        )
    }

    /// Sends a reset mail if the address has an active account; answers the same either way. At
    /// most one mail per account in `MAIL_COOLDOWN_MINUTES` (the command sends nothing within it).
    pub async fn request_password_reset(&self, email: &str) -> ApiResult<()> {
        if let Some(held) = self
            .ask(
                FindAccountByEmail {
                    email: email.into(),
                },
                PLATFORM_TENANT,
                false,
            )
            .await?
        {
            self.platform(
                RequestPasswordReset {},
                &held.id,
                self.service_context.clone(),
            )
            .await?;
        }
        Ok(())
    }

    /// Sets a new password with the mailed token; every session ends.
    pub async fn reset_password(&self, reset_token: &str, new: &str) -> ApiResult<()> {
        let Some((account_id, _, _)) = parse_token(reset_token) else {
            return refused("invalid_token");
        };
        done(
            self.platform(
                ResetPassword {
                    token: reset_token.into(),
                    new: new.into(),
                },
                &account_id,
                RequestContext::default(),
            )
            .await?,
        )
    }

    /// Asks to change the account's address: a token goes to the new one.
    pub async fn request_email_change(&self, claims: &Claims, email: &str) -> ApiResult<()> {
        done(
            self.platform(
                RequestEmailChange {
                    email: email.into(),
                },
                &claims.sub,
                Self::own(claims),
            )
            .await?,
        )
    }

    /// Confirms the new address with its token; a taken address is refused (`unique_taken`).
    pub async fn confirm_email_change(&self, change_token: &str) -> ApiResult<()> {
        let Some((account_id, _, _)) = parse_token(change_token) else {
            return refused("invalid_token");
        };
        done(
            self.platform(
                ConfirmEmailChange {
                    token: change_token.into(),
                },
                &account_id,
                RequestContext::default(),
            )
            .await?,
        )
    }

    // --- invitations -------------------------------------------------------------------------------------

    /// What the acceptance page shows for an invitation token.
    pub async fn peek_invitation(&self, invitation_token: &str) -> ApiResult<Option<Peeked>> {
        let Some(t) = parse_invitation_token(invitation_token) else {
            return Ok(None);
        };
        self.ask(
            PeekInvitation {
                token: invitation_token.into(),
            },
            &t.tenant,
            false,
        )
        .await
    }

    /// Accepts an invitation: as the signed-in account (`claims`, its session checked as for every
    /// signed-in call, and against the invitation's tenant: active, its second factor, its enforced
    /// SSO, except for an owner invitation: owners are exempt from it, so they can mend it), or as a
    /// new person with a name and password (`new_person`), registered in the same step (active:
    /// the token proved the email). The accepting account's email must be the invited one. A new
    /// person's name and password are checked before the invitation is used, so a refused
    /// registration does not spend it. Either way a tenant that is not active takes nobody in:
    /// `tenant_suspended` or `tenant_not_active` (pending) from the middleware's check of a
    /// signed-in account, else `tenant_not_active` (the command's requirement cannot tell
    /// suspended from pending). The account and tenant.
    pub async fn accept_invitation(
        &self,
        invitation_token: &str,
        claims: Option<&Claims>,
        new_person: Option<(&str, &str)>,
    ) -> ApiResult<(String, String)> {
        let Some(peek) = self.peek_invitation(invitation_token).await? else {
            return refused("invalid_token");
        };
        let Some(invited) = peek.email.clone().filter(|e| !e.is_empty()) else {
            return refused("invalid_token");
        };
        let (account_id, email, person) = match claims {
            Some(c) => {
                let account = self.signed_in_account(c).await?;
                // The middleware checks the tenant (exists, active), then its second factor, then
                // its enforced SSO: `sso_required` means every other check passed, and an owner is
                // exempt from it.
                match self.may_join(c, &peek.tenant).await {
                    Err(ApiError::Refused(code)) if peek.owner && code == "sso_required" => {}
                    other => other?,
                }
                (c.sub.clone(), account.email.unwrap_or_default(), None)
            }
            None => {
                let (name, password) = new_person.unwrap_or_default();
                if trim_space(name).is_empty() || name.chars().count() > 200 {
                    return refused("invalid_name");
                }
                if !valid_password(password) {
                    return refused("weak_password");
                }
                // Someone with an account signs in first, so nobody gets an account they could not use.
                if self
                    .ask(
                        FindAccountByEmail {
                            email: invited.clone(),
                        },
                        PLATFORM_TENANT,
                        false,
                    )
                    .await?
                    .is_some()
                {
                    return refused("sign_in_first");
                }
                (new_id(), invited, Some((name, password)))
            }
        };
        let accept = AcceptInvitation {
            token: invitation_token.into(),
            account_id: account_id.clone(),
            email: email.clone(),
            new_account: claims.is_none(),
        };
        joined(
            self.submit(
                accept,
                &peek.invitation_id,
                &peek.tenant,
                self.service_context.clone(),
            )
            .await?,
        )?;
        if let Some((name, password)) = person {
            let register = RegisterAccount {
                email,
                name: name.into(),
                password: Some(password.into()),
                verified: Some(true),
            };
            done(
                self.platform(register, &account_id, self.service_context.clone())
                    .await?,
            )?;
        }
        Ok((account_id, peek.tenant))
    }

    /// A reusable invitation link into `groups` of the tenant, as `context` (whose groups the
    /// middleware checks): the token, shown this once, for a URL such as `/join?token=…`, and the
    /// link's ID.
    pub async fn create_invitation_link(
        &self,
        context: RequestContext,
        tenant: &str,
        link: NewInvitationLink,
    ) -> ApiResult<(String, String)> {
        let link_id = new_id();
        let secret = random_secret(32);
        let command = CreateInvitationLink {
            secret: secret.clone(),
            groups: Some(link.groups),
            label: link.label,
            expires_at: link.expires_at,
            max_uses: link.max_uses,
        };
        done(self.submit(command, &link_id, tenant, context).await?)?;
        Ok((link_token(tenant, &link_id, &secret), link_id))
    }

    /// What the join page shows for a link token: the tenant, the link's label, whether it can be
    /// used.
    pub async fn peek_invitation_link(&self, link: &str) -> ApiResult<Option<PeekedLink>> {
        let Some(t) = parse_link_token(link) else {
            return Ok(None);
        };
        self.ask(PeekInvitationLink { token: link.into() }, &t.tenant, false)
            .await
    }

    /// The signed-in account joins the link's tenant with its groups (its session checked as for
    /// every signed-in call, and against the link's tenant: active, its second factor, its enforced
    /// SSO): the tenant. Someone without an account registers and verifies first:
    /// a link proves no email.
    pub async fn join_with_invitation_link(
        &self,
        claims: &Claims,
        link: &str,
    ) -> ApiResult<String> {
        let Some(t) = parse_link_token(link) else {
            return refused("invalid_token");
        };
        self.signed_in_account(claims).await?;
        self.may_join(claims, &t.tenant).await?;
        let join = JoinWithInvitationLink {
            token: link.into(),
            account_id: claims.sub.clone(),
        };
        joined(
            self.submit(join, &t.link_id, &t.tenant, self.service_context.clone())
                .await?,
        )?;
        Ok(t.tenant)
    }

    /// Signs out: ends the session.
    pub async fn sign_out(&self, claims: &Claims) -> ApiResult<()> {
        done(
            self.platform(
                EndSession {
                    reason: Some(SessionEnd::SignedOut),
                },
                &claims.sid,
                Self::own(claims),
            )
            .await?,
        )
    }
}

/// Percent-encoded, every byte but letters, digits and `-._~` (RFC 3986's unreserved).
fn percent(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// TXT records through the system's resolver; none on any failure.
async fn system_txt(name: &str) -> Vec<String> {
    let Some(resolver) = hickory_resolver::Resolver::builder_tokio()
        .ok()
        .and_then(|b| b.build().ok())
    else {
        return vec![];
    };
    match resolver.txt_lookup(name).await {
        Ok(found) => found
            .answers()
            .iter()
            .filter_map(|record| match &record.data {
                hickory_resolver::proto::rr::RData::TXT(txt) => Some(
                    txt.txt_data
                        .iter()
                        .map(|part| String::from_utf8_lossy(part).into_owned())
                        .collect(),
                ),
                _ => None,
            })
            .collect(),
        Err(_) => vec![],
    }
}

/// The address an OAuth2 provider's email list (`[{email, primary, verified}]`) says is the
/// person's: the primary one if verified, else the first verified; none if none is verified.
pub fn oauth2_email(emails: &Value) -> Option<String> {
    let list = emails.as_array()?;
    let verified = |e: &&Value| e["verified"] == Value::Bool(true);
    list.iter()
        .filter(verified)
        .find(|e| e["primary"] == Value::Bool(true))
        .or_else(|| list.iter().find(verified))
        .and_then(|e| e["email"].as_str())
        .filter(|e| !e.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod oauth2_tests {
    use super::*;

    #[test]
    fn the_primary_verified_address_is_the_persons() {
        let list = serde_json::json!([
            { "email": "old@example.com", "primary": false, "verified": true },
            { "email": "ada@example.com", "primary": true, "verified": true },
        ]);
        assert_eq!(oauth2_email(&list).as_deref(), Some("ada@example.com"));
        // A primary address that is not verified does not count; another verified one does.
        let list = serde_json::json!([
            { "email": "ada@example.com", "primary": true, "verified": false },
            { "email": "work@example.com", "primary": false, "verified": true },
        ]);
        assert_eq!(oauth2_email(&list).as_deref(), Some("work@example.com"));
        let none =
            serde_json::json!([{ "email": "ada@example.com", "primary": true, "verified": false }]);
        assert_eq!(oauth2_email(&none), None);
        assert_eq!(
            oauth2_email(&serde_json::json!({ "message": "Bad credentials" })),
            None
        );
    }

    #[test]
    fn github_names_its_endpoints_on_its_web_and_api_hosts() {
        let p = Provider::github(
            "id",
            "secret",
            "https://github.com/",
            "https://api.github.com",
        );
        let o = p.oauth2.unwrap();
        assert_eq!(o.authorize, "https://github.com/login/oauth/authorize");
        assert_eq!(o.token, "https://github.com/login/oauth/access_token");
        assert_eq!(o.user, "https://api.github.com/user");
        assert_eq!(o.emails, "https://api.github.com/user/emails");
        assert!(
            Provider::oidc("https://accounts.example.com", "id", "s")
                .oauth2
                .is_none()
        );
    }
}
