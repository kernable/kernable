"""The API part: framework-neutral functions for what only the API can do. Wire them into
your HTTP framework; `example/server.py` shows one with `http.server`. `KitApi` blocks, like the
SDK's client; `AsyncKitApi` (`kit/api/aio.py`) offers the same for asyncio frameworks.

Every method answers a dict: `{"ok": True, ...}`, or `{"ok": False, "reason": code}`."""

import base64
import hashlib
import json
import re
import secrets
import time
import uuid
from urllib.parse import parse_qs, quote, urlencode, urlparse

import requests
from joserfc import jwt
from joserfc.jwk import KeySet
from kernable import Kernable, Outcome, Rejected, RequestContext
from kernable.messages import from_wire
from webauthn import generate_authentication_options, generate_registration_options, options_to_json
from webauthn.helpers.structs import AuthenticatorSelectionCriteria, ResidentKeyRequirement, UserVerificationRequirement

from ..account.commands import (AddPasskey, ChangePassword, ConfirmEmailChange, ConfirmTotp, EndSession, LinkProvider,
                                NoteRegistrationAttempt, ProvePasskey, ProveSecondFactor, RefreshSession, RegisterAccount,
                                RequestEmailChange, RequestPasswordReset, ResetPassword, SignIn, SignInWithPasskey,
                                SignInWithProvider, StartTotp, VerifyEmail, VerifySsoDomain)
from ..account.queries import FindAccountByEmail, FindAccountByLink, FindSsoByDomain, GetAccount, GetSsoSecret, ListSessions
from ..identity.commands import CreateApiKey
from ..identity.domain import api_key, parse_api_key
from ..identity.queries import FindApiKey, FindIdentityByAccount
from ..invitation.commands import AcceptInvitation, CreateInvitationLink, JoinWithInvitationLink
from ..invitation.domain import InvitedGroup, link_token, parse_link_token
from ..invitation.queries import PeekInvitation, PeekInvitationLink
from ..shared.constants import CHALLENGE_SECONDS, PASSKEYS, PLATFORM_TENANT
from ..tenancy.queries import GetSignUpPolicy
from ..shared.secrets import (trim_space, check_password, email_domain, equal_secrets, parse_invitation_token, parse_token, random_secret, token, totp_secret,
                              valid_password)
from .tokens import Claims, SigningKeys, check_access_token, issue_access_token


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _events(outcome: Outcome) -> list[dict]:
    """The events of an outcome as `{"type", "data"}` with snake_case data."""
    return [{"type": e.type, "data": from_wire(e.data or {})} for e in outcome.events]


def _first(outcome: Outcome, type_: str) -> dict | None:
    return next((e["data"] for e in _events(outcome) if e["type"] == type_), None)


def _failed(outcome: Outcome) -> str:
    """Why a sign-in that committed did not sign in, as the caller hears it: its `SignInFailed`
    reason, except that a locked account answers `wrong_password` like an unknown address, so
    nobody learns from it that an account exists (the event keeps `locked`)."""
    events = _events(outcome)
    reason = (events[0]["data"].get("reason") if events else None) or "wrong_password"
    return "wrong_password" if reason == "locked" else reason


def _done(outcome: Outcome) -> dict:
    return {"ok": True} if outcome.committed else {"ok": False, "reason": outcome.code}


def _resolve_txt(name: str) -> list[str]:
    import dns.resolver

    return [b"".join(r.strings).decode() for r in dns.resolver.resolve(name, "TXT")]


def github(client_id: str, client_secret: str, web: str = "https://github.com", api: str = "https://api.github.com") -> dict:
    """GitHub (an OAuth app's client) as a provider: plain OAuth2, its endpoints on `web` and `api`
    (github.com's, or a GitHub Enterprise Server's)."""
    web, api = web.rstrip("/"), api.rstrip("/")
    return {"issuer": web, "client_id": client_id, "client_secret": client_secret,
            "oauth2": {"authorize": f"{web}/login/oauth/authorize", "token": f"{web}/login/oauth/access_token",
                       "user": f"{api}/user", "emails": f"{api}/user/emails", "scope": "read:user user:email"}}


def oauth2_email(emails) -> str | None:
    """The address an OAuth2 provider's email list says is the person's: the primary one if verified,
    else the first verified; none if none is."""
    if not isinstance(emails, list):
        return None
    verified = [e for e in emails if isinstance(e, dict) and e.get("verified") is True and e.get("email")]
    primary = next((e for e in verified if e.get("primary") is True), None)
    chosen = primary or (verified[0] if verified else None)
    return chosen["email"] if chosen else None


class KitApi:
    """What only your API does. `kernable`: a client whose key may submit and query (it signs every
    request). `keys`: what access tokens are signed with. `service`: the API's own identity, a
    service of the platform tenant with an API key, from the bootstrap (`{"identity_id", "key_id"}`).
    `providers`: social OAuth/OIDC providers by name (`{"google": {"issuer", "client_id",
    "client_secret"}}`; a plain OAuth2 one names its endpoints in `oauth2`, `github()` makes one);
    tenants' SSO is configured as `account.sso`. `redirect_uri`: where providers
    send the browser back to; your route calls `oidc_finish` with the full URL. `insecure_issuers`
    allows http:// issuers, for development and tests only. `resolve_txt` reads DNS TXT records
    (tests replace it; the default needs `dnspython`)."""

    def __init__(self, kernable: Kernable, keys: SigningKeys, service: dict, *, providers: dict | None = None,
                 redirect_uri: str | None = None, insecure_issuers: bool = False, resolve_txt=None) -> None:
        self.kernable = kernable
        self.keys = keys
        self.providers = providers or {}
        self.redirect_uri = redirect_uri
        self.insecure_issuers = insecure_issuers
        self.resolve_txt = resolve_txt or _resolve_txt
        self._configs: dict[str, dict] = {}
        self._api_keys: dict[str, tuple[RequestContext, float]] = {}
        #: The request context of the API's own requests: lookups no one else may make.
        self.service_context = RequestContext(sender_tenant=PLATFORM_TENANT, sender_identity=service["identity_id"],
                                              sender_session=service["key_id"])

    # --- small helpers -------------------------------------------------------------------------------------

    def _submit(self, command, aggregate_id: str, tenant: str = PLATFORM_TENANT, context: RequestContext | None = None) -> Outcome:
        return self.kernable.submit(command, aggregate_id=aggregate_id, tenant=tenant, context=context or RequestContext())

    def _ask(self, query, tenant: str = PLATFORM_TENANT, **options):
        return self.kernable.query(query, tenant=tenant, context=self.service_context, **options)

    def _find(self, query) -> str | None:
        try:
            return self._ask(query)["id"]
        except Rejected:
            return None

    @staticmethod
    def _own(claims: Claims) -> RequestContext:
        """The context of the token's account acting on itself."""
        return RequestContext(sender_account=claims.sub, sender_session=claims.sid)

    def _signed_in_account(self, claims: Claims) -> dict:
        """The token's account, read as itself: the middleware checks the session as for every signed-in
        request (active, the account active, a second factor proved if it has one). For what the API
        then does with its own context (accepting an invitation, joining through a link)."""
        try:
            account = self.kernable.query(GetAccount(account_id=claims.sub), tenant=PLATFORM_TENANT, context=self._own(claims))
        except Rejected as err:
            return {"ok": False, "reason": err.code}
        return {"ok": True, "account": account}

    def _may_join(self, claims: Claims, tenant: str) -> dict:
        """The token's account checked against a tenant it is about to join, where it holds no identity
        yet: the middleware applies that tenant's checks to the account acting on itself (the tenant
        active, its second-factor requirement, its enforced SSO). A cheap query of one's own sessions
        in that tenant carries the check; its answer is not used."""
        try:
            self.kernable.query(ListSessions(account_id=claims.sub), tenant=tenant, context=self._own(claims))
        except Rejected as err:
            return {"ok": False, "reason": err.code}
        return {"ok": True}

    def _signed_in(self, account_id: str, session_id: str, refresh_token: str, data: dict, second_factor: bool | None = None) -> dict:
        return {"ok": True, "account_id": account_id, "session_id": session_id, "refresh_token": refresh_token,
                "second_factor": bool(data.get("mfa")) if second_factor is None else second_factor,
                "access_token": issue_access_token(self.keys, account_id, session_id, data.get("factors") or [])}

    # --- registration and sign-in --------------------------------------------------------------------------

    def register(self, email: str, name: str, password: str | None = None) -> dict:
        """Registers a person. Answers the same whether or not the email was taken: the caller
        shows "check your email" either way."""
        account_id = str(uuid.uuid4())
        command = RegisterAccount(email=email, name=name, password=password)
        outcome = self._submit(command, account_id)
        if outcome.committed:
            return {"ok": True, "account_id": account_id}
        if outcome.code != "unique_taken":
            return {"ok": False, "reason": outcome.code}
        # The address is held: a registration that expired unverified is taken over (same aggregate);
        # otherwise the same answer as success, so nobody learns who has an account.
        held = self._ask(FindAccountByEmail(email=email))
        if held.get("status") == "pending":
            if self._submit(command, held["id"]).committed:
                return {"ok": True, "account_id": held["id"]}
        else:
            # The account's owner learns that someone tried; the caller learns nothing.
            self._submit(NoteRegistrationAttempt(), held["id"], context=self.service_context)
        return {"ok": True, "account_id": str(uuid.uuid4())}

    def verify_email(self, email_token: str) -> bool:
        """Confirms an email address with the token from the mail."""
        parts = parse_token(email_token)
        return bool(parts) and self._submit(VerifyEmail(token=email_token), parts[0]).committed

    def sign_in(self, email: str, password: str, ip: str | None = None, user_agent: str | None = None) -> dict:
        """Signs in with email and password: an access token and a refresh token, or why not.
        `second_factor`: the account has a second factor on; prove it before anything else. An
        unknown address and a locked account answer `wrong_password`, and an unknown address takes
        as long (a password check against a dummy hash), so nobody learns whether an account exists."""
        account_id = self._find(FindAccountByEmail(email=email))
        if not account_id:
            check_password(None, password)  # as slow as a wrong password
            return {"ok": False, "reason": "wrong_password"}  # never say whether the account exists
        session_id = str(uuid.uuid4())
        refresh_token = token(session_id, 1, random_secret())
        outcome = self._submit(SignIn(session_id=session_id, password=password, refresh_token=refresh_token, ip=ip, user_agent=user_agent), account_id)
        if not outcome.committed:
            raise Rejected(outcome.message, outcome.code)
        signed_in = _first(outcome, "SignedIn")
        if signed_in is None:
            return {"ok": False, "reason": _failed(outcome)}
        return self._signed_in(account_id, session_id, refresh_token, signed_in)

    def check(self, access_token: str) -> Claims | None:
        """The claims of a valid access token, else None."""
        return check_access_token(self.keys, access_token)

    def context_for(self, claims: Claims, tenant: str | None = None) -> RequestContext:
        """The request context for the token's holder, in `tenant` (its identity there) or, without a
        tenant, only as a signed-in account (for `authenticated` requests)."""
        if not tenant:
            return self._own(claims)
        found = self._ask(FindIdentityByAccount(account_id=claims.sub), tenant)
        return RequestContext(sender_account=claims.sub, sender_session=claims.sid, sender_tenant=tenant, sender_identity=found["identity_id"])

    def refresh(self, refresh_token: str, ip: str | None = None) -> dict:
        """A new access token and a rotated refresh token. An older refresh token is a reuse:
        the session ends, and so does every token of it."""
        parts = parse_token(refresh_token)
        if not parts:
            return {"ok": False, "reason": "invalid_token"}
        session_id, generation, _ = parts
        if not re.fullmatch(r"[0-9]+", generation):
            return {"ok": False, "reason": "invalid_token"}
        following = token(session_id, int(generation) + 1, random_secret())
        outcome = self._submit(RefreshSession(refresh_token=refresh_token, new_refresh_token=following, ip=ip), session_id)
        if not outcome.committed:
            return {"ok": False, "reason": outcome.code}
        events = _events(outcome)
        if events[0]["type"] != "SessionRefreshed":
            return {"ok": False, "reason": events[0]["data"].get("reason")}
        d = events[0]["data"]
        return {"ok": True, "refresh_token": following, "access_token": issue_access_token(self.keys, d["account_id"], session_id, d["factors"])}

    def prove_second_factor(self, claims: Claims, code: str) -> dict:
        """Proves the second factor (a TOTP or recovery code) for the session: a new access token with it."""
        outcome = self._submit(ProveSecondFactor(session_id=claims.sid, code=code), claims.sub)
        if not outcome.committed:
            return {"ok": False, "reason": outcome.code}
        proved = _first(outcome, "SecondFactorProved")
        if proved is None:
            return {"ok": False, "reason": "wrong_code"}
        factor = "recovery" if proved.get("method") == "recovery" else "totp"
        return {"ok": True, "access_token": issue_access_token(self.keys, claims.sub, claims.sid, [*claims.fac, factor])}

    def start_totp(self, claims: Claims, issuer: str, label: str) -> dict:
        """Starts TOTP: a new secret, and the `otpauth:` URI an authenticator app reads (as a QR code)."""
        secret = totp_secret()
        outcome = self._submit(StartTotp(secret=secret), claims.sub, context=self._own(claims))
        if not outcome.committed:
            return {"ok": False, "reason": outcome.code}
        uri = f"otpauth://totp/{quote(f'{issuer}:{label}', safe='')}?secret={secret}&issuer={quote(issuer, safe='')}"
        return {"ok": True, "secret": secret, "uri": uri}

    def confirm_totp(self, claims: Claims, code: str) -> dict:
        """Turns TOTP on with its first code: the recovery codes, shown this once."""
        recovery_codes = [random_secret(8) for _ in range(10)]
        outcome = self._submit(ConfirmTotp(code=code, recovery_codes=recovery_codes), claims.sub, context=self._own(claims))
        return {"ok": True, "recovery_codes": recovery_codes} if outcome.committed else {"ok": False, "reason": outcome.code}

    # --- API keys ------------------------------------------------------------------------------------------

    def create_api_key(self, context: RequestContext, tenant: str, identity_id: str, label: str = "", expires_at: int | None = None) -> dict:
        """A new API key for a service identity, shown this once."""
        key_id = "".join(c if c.isalnum() else "x" for c in random_secret(9))
        secret = random_secret()
        outcome = self._submit(CreateApiKey(key_id=key_id, secret=secret, label=label, expires_at=expires_at), identity_id, tenant, context)
        return {"ok": True, "key": api_key(key_id, secret), "key_id": key_id} if outcome.committed else {"ok": False, "reason": outcome.code}

    def check_api_key(self, key: str) -> RequestContext | None:
        """The request context of an API key, or None: checked against its hash, cached for a minute. A
        revoked key is refused by the middleware at once, cache or not."""
        parsed = parse_api_key(key)
        if not parsed:
            return None
        cached = self._api_keys.get(parsed["hash"])
        if cached and cached[1] > time.time():
            return cached[0]
        try:
            found = self._ask(FindApiKey(key_id=parsed["key_id"]), all_partitions=True)
        except Rejected:
            return None
        expired = found.get("expires_at") is not None and time.time() * 1000 > found["expires_at"]
        if found.get("revoked") or expired or not equal_secrets(found["hash"], parsed["hash"]):
            return None
        context = RequestContext(sender_tenant=found["tenant"], sender_identity=found["identity_id"], sender_session=parsed["key_id"])
        self._api_keys[parsed["hash"]] = (context, time.time() + 60)
        return context

    # --- Passkeys: challenges live a few minutes in the kernable cache and count once. ---------------

    def _keep(self, key: str, value: str) -> None:
        self.kernable.cache.set("account", key, value, ttl_ms=CHALLENGE_SECONDS * 1000, tenant=PLATFORM_TENANT, context=self.service_context)

    def _take(self, key: str) -> bytes | None:
        """The cached value, deleted: it counts once."""
        value = self.kernable.cache.get("account", key, tenant=PLATFORM_TENANT, context=self.service_context)
        if value is None or not self.kernable.cache.delete("account", key, tenant=PLATFORM_TENANT, context=self.service_context):
            return None
        return value

    def _take_challenge(self, response: dict) -> str | None:
        """The challenge a browser response answers, if it is one the API issued and nobody used yet."""
        try:
            challenge = json.loads(_unb64(response["response"]["clientDataJSON"]))["challenge"]
        except (KeyError, TypeError, ValueError):
            return None
        return challenge if self._take(f"challenge:{challenge}") is not None else None

    def passkey_registration_options(self, claims: Claims, user_name: str) -> dict:
        """What the browser needs to create a passkey for the signed-in account (JSON for
        `navigator.credentials.create`)."""
        options = generate_registration_options(
            rp_id=PASSKEYS.rp_id, rp_name=PASSKEYS.rp_name, user_name=user_name, user_id=claims.sub.encode(),
            authenticator_selection=AuthenticatorSelectionCriteria(resident_key=ResidentKeyRequirement.REQUIRED,
                                                                   user_verification=UserVerificationRequirement.REQUIRED))
        self._keep(f"challenge:{_b64(options.challenge)}", "1")
        return json.loads(options_to_json(options))

    def add_passkey(self, claims: Claims, response: dict, label: str | None = None) -> dict:
        """Adds the passkey the browser created."""
        challenge = self._take_challenge(response)
        if not challenge:
            return {"ok": False, "reason": "invalid_challenge"}
        return _done(self._submit(AddPasskey(challenge=challenge, response=response, label=label), claims.sub, context=self._own(claims)))

    def passkey_sign_in_options(self) -> dict:
        """What the browser needs to sign in with a passkey (or prove one): any of the user's passkeys."""
        options = generate_authentication_options(rp_id=PASSKEYS.rp_id, user_verification=UserVerificationRequirement.REQUIRED)
        self._keep(f"challenge:{_b64(options.challenge)}", "1")
        return json.loads(options_to_json(options))

    def sign_in_with_passkey(self, response: dict, ip: str | None = None) -> dict:
        """Signs in with a passkey, without a password: the passkey names its account (its user handle)."""
        challenge = self._take_challenge(response)
        handle = (response.get("response") or {}).get("userHandle")
        account_id = _unb64(handle).decode() if handle else ""
        if not challenge or not account_id:
            return {"ok": False, "reason": "wrong_password"}
        session_id = str(uuid.uuid4())
        refresh_token = token(session_id, 1, random_secret())
        outcome = self._submit(SignInWithPasskey(session_id=session_id, challenge=challenge, response=response, refresh_token=refresh_token, ip=ip),
                               account_id, context=self.service_context)
        if not outcome.committed:
            return {"ok": False, "reason": "wrong_password"}
        signed_in = _first(outcome, "SignedIn")
        if signed_in is None:
            return {"ok": False, "reason": _failed(outcome)}
        return self._signed_in(account_id, session_id, refresh_token, signed_in, second_factor=False)

    def prove_passkey(self, claims: Claims, response: dict) -> dict:
        """Proves a passkey as the second factor of a password session."""
        challenge = self._take_challenge(response)
        if not challenge:
            return {"ok": False, "reason": "invalid_challenge"}
        outcome = self._submit(ProvePasskey(session_id=claims.sid, challenge=challenge, response=response), claims.sub, context=self.service_context)
        if not outcome.committed or _first(outcome, "SecondFactorProved") is None:
            return {"ok": False, "reason": "wrong_passkey"}
        return {"ok": True, "access_token": issue_access_token(self.keys, claims.sub, claims.sid, [*claims.fac, "passkey"])}

    # --- OAuth/OIDC and SSO: the API does the exchange; the account decides. --------------------------

    def _config(self, issuer: str) -> dict:
        """The provider's discovery document, kept once fetched."""
        if not self.insecure_issuers and not issuer.startswith("https://"):
            raise ValueError(f"{issuer} is not https")
        if issuer not in self._configs:
            res = requests.get(f"{issuer.rstrip('/')}/.well-known/openid-configuration", timeout=10)
            res.raise_for_status()
            config = res.json()
            # The endpoints it names carry the client secret, codes and tokens: https too.
            for key in ("jwks_uri", "token_endpoint", "authorization_endpoint"):
                if key in config and not self._secure(str(config[key])):
                    raise ValueError(f"{key} {config[key]} is not https")
            config["jwks"] = requests.get(config["jwks_uri"], timeout=10).json()
            self._configs[issuer] = config
        return self._configs[issuer]

    def _sso_settings(self, tenant: str, sso_id: str) -> dict | None:
        """A tenant's SSO: what the API signs in with, and the domains it verified."""
        try:
            s = self._ask(GetSsoSecret(sso_id=sso_id), tenant)
        except Rejected:
            return None
        if not s.get("client_secret"):
            return None
        return {"provider": f"sso:{sso_id}", "issuer": s["issuer"], "client_id": s["client_id"], "client_secret": s["client_secret"],
                "tenant": tenant, "sso_id": sso_id, "domains": s.get("domains") or []}

    def _provider(self, provider: str | None, email: str | None) -> dict | None:
        """The provider's settings: a social one by name, or the SSO of the tenant that verified the email's domain."""
        if provider:
            p = self.providers.get(provider)
            return {"provider": provider, **p} if p else None
        try:
            found = self._ask(FindSsoByDomain(domain=email_domain(email)), all_partitions=True)
        except Rejected:
            return None
        return self._sso_settings(found["tenant"], found["sso_id"])

    def oidc_start(self, *, provider: str | None = None, email: str | None = None, link: Claims | None = None) -> dict:
        """Where to send the browser: a social provider by name, or a person's tenant SSO by their email.
        With `link`, the signed-in account links the login instead of signing in."""
        p = self._provider(provider, email)
        if not p or not self.redirect_uri:
            return {"ok": False, "reason": "no_provider"}
        oauth2 = p.get("oauth2")
        if oauth2 and not self._secure(oauth2["authorize"]):
            return {"ok": False, "reason": "no_provider"}
        config = None if oauth2 else self._config(p["issuer"])
        verifier = secrets.token_urlsafe(48)
        state = secrets.token_urlsafe(24)
        nonce = secrets.token_urlsafe(24)
        kept = {"provider": p["provider"], "tenant": p.get("tenant"), "sso_id": p.get("sso_id"), "verifier": verifier, "nonce": nonce,
                "link": {"account_id": link.sub} if link else None}
        self._keep(f"oidc:{state}", json.dumps(kept))
        challenge = _b64(hashlib.sha256(verifier.encode()).digest())
        if oauth2:
            # Plain OAuth2: no nonce (nothing brings it back).
            query = urlencode({"response_type": "code", "client_id": p["client_id"], "redirect_uri": self.redirect_uri, "scope": oauth2["scope"],
                               "state": state, "code_challenge_method": "S256", "code_challenge": challenge})
            return {"ok": True, "url": f"{oauth2['authorize']}?{query}"}
        query = urlencode({"response_type": "code", "client_id": p["client_id"], "redirect_uri": self.redirect_uri, "scope": "openid email",
                           "state": state, "nonce": nonce, "code_challenge_method": "S256", "code_challenge": challenge})
        return {"ok": True, "url": f"{config['authorization_endpoint']}?{query}"}

    def _secure(self, url: str) -> bool:
        """Whether `url` may be used: https, or anything with `insecure_issuers` (tests)."""
        return self.insecure_issuers or url.startswith("https://")

    def _exchange_oauth2(self, p: dict, o: dict, code: str, kept: dict) -> dict:
        """For an OAuth2 provider: the person behind `code`, as the claims an ID token would hold
        (`sub`: their `id`; `email`, `email_verified`: their primary verified address)."""
        for url in (o["token"], o["user"], o["emails"]):
            if not self._secure(url):
                raise ValueError(f"{url} is not https")
        res = requests.post(o["token"], timeout=10, headers={"accept": "application/json"},
                            data={"grant_type": "authorization_code", "code": code, "client_id": p["client_id"],
                                  "client_secret": p["client_secret"], "redirect_uri": self.redirect_uri, "code_verifier": kept["verifier"]})
        access = res.json().get("access_token")
        if not access:
            raise ValueError("no access_token")
        headers = {"authorization": f"Bearer {access}", "accept": "application/json", "user-agent": "kernable-kit"}
        user = requests.get(o["user"], timeout=10, headers=headers).json()
        if user.get("id") in (None, ""):
            raise ValueError("no id")
        email = oauth2_email(requests.get(o["emails"], timeout=10, headers=headers).json())
        return {"sub": str(user["id"]), "email": email or "", "email_verified": email is not None}

    def _exchange(self, p: dict, callback: str, kept: dict, state: str) -> dict:
        """The ID token's claims for the code in `callback`, checked (issuer, audience, nonce, expiry);
        for an OAuth2 provider, the same claims read from it."""
        params = {k: v[0] for k, v in parse_qs(urlparse(callback).query).items()}
        if params.get("state") != state or "code" not in params:
            raise ValueError("no code for this state")
        if p.get("oauth2"):
            return self._exchange_oauth2(p, p["oauth2"], params["code"], kept)
        config = self._config(p["issuer"])
        res = requests.post(config["token_endpoint"], timeout=10,
                            auth=(quote(p["client_id"], safe=""), quote(p["client_secret"], safe="")),
                            data={"grant_type": "authorization_code", "code": params["code"], "redirect_uri": self.redirect_uri,
                                  "code_verifier": kept["verifier"]})
        res.raise_for_status()
        decoded = jwt.decode(res.json()["id_token"], KeySet.import_key_set(config["jwks"]), algorithms=["RS256", "ES256", "EdDSA"])
        jwt.JWTClaimsRegistry(iss={"essential": True, "value": config["issuer"]}, aud={"essential": True, "value": p["client_id"]},
                              nonce={"essential": True, "value": kept["nonce"]}, exp={"essential": True}).validate(decoded.claims)
        return dict(decoded.claims)

    def oidc_finish(self, callback: str, ip: str | None = None) -> dict:
        """The browser came back (`callback` is the full URL). Signs in a linked login; links a tenant SSO
        login to the account with the same verified email; registers a new person whose email nobody
        has; refuses a social login whose email has an account (`not_linked`: sign in and link it). A
        tenant's SSO answers only for its verified domains: an unlinked login with another email is
        refused (`email_not_in_domain`), so a tenant's provider cannot take over anyone else's account."""
        state = (parse_qs(urlparse(callback).query).get("state") or [""])[0]
        raw = self._take(f"oidc:{state}") if state else None
        if raw is None:
            return {"ok": False, "reason": "invalid_state"}
        kept = json.loads(raw)
        if kept.get("sso_id"):
            p = self._sso_settings(kept["tenant"], kept["sso_id"])
        else:
            p = {"provider": kept["provider"], **self.providers[kept["provider"]]} if kept["provider"] in self.providers else None
        if not p:
            return {"ok": False, "reason": "no_provider"}
        try:
            claims = self._exchange(p, callback, kept, state)
        except Exception:  # noqa: BLE001: whatever the provider or the token got wrong
            return {"ok": False, "reason": "provider_refused"}
        email = claims.get("email") or ""
        email_verified = claims.get("email_verified") is True
        provider, subject = kept["provider"], claims["sub"]
        if kept.get("link"):
            linked = self._submit(LinkProvider(provider=provider, subject=subject, email=email), kept["link"]["account_id"], context=self.service_context)
            return {"ok": True, "linked": True} if linked.committed else {"ok": False, "reason": linked.code}
        sso = provider.startswith("sso:")
        account_id = self._find(FindAccountByLink(provider=provider, subject=subject))
        # Not linked yet: a tenant's SSO speaks only for the domains its tenant verified.
        if not account_id and sso and email_domain(email) not in (p.get("domains") or []):
            return {"ok": False, "reason": "email_not_in_domain"}
        if not account_id and email:
            account_id = self._find(FindAccountByEmail(email=email))
        if not account_id:
            # A new person: registered (verified if the provider says so), the login linked.
            if not email:
                return {"ok": False, "reason": "no_email"}
            # A social login registers only where anyone may: with sign-up by invitation, the person
            # accepts an invitation first and links the login after. A tenant's SSO speaks for its own
            # verified domains, so it registers either way.
            if not sso:
                try:
                    accounts = self._ask(GetSignUpPolicy())["accounts"]
                except Rejected:
                    accounts = None
                if accounts != "open":
                    return {"ok": False, "reason": "sign_up_closed"}
            account_id = str(uuid.uuid4())
            registered = self._submit(RegisterAccount(email=email, name=email.partition("@")[0], verified=email_verified), account_id,
                                      context=self.service_context)
            if not registered.committed:
                return {"ok": False, "reason": registered.code}
            if not sso:
                self._submit(LinkProvider(provider=provider, subject=subject, email=email), account_id, context=self.service_context)
        session_id = str(uuid.uuid4())
        refresh_token = token(session_id, 1, random_secret())
        outcome = self._submit(SignInWithProvider(session_id=session_id, provider=provider, subject=subject, email=email, email_verified=email_verified,
                                                  auto_link=sso, domains=p.get("domains"), refresh_token=refresh_token, ip=ip), account_id, context=self.service_context)
        if not outcome.committed:
            return {"ok": False, "reason": outcome.code}
        signed_in = _first(outcome, "SignedIn")
        if signed_in is None:
            return {"ok": False, "reason": _failed(outcome)}
        return self._signed_in(account_id, session_id, refresh_token, signed_in)

    def verify_sso_domain(self, tenant: str, sso_id: str, domain: str) -> dict:
        """Looks for the SSO domain's TXT record (`_kernable-sso.<domain>`) and verifies the domain if it is there."""
        try:
            found = list(self.resolve_txt(f"_kernable-sso.{domain}"))
        except Exception:  # noqa: BLE001: no record
            found = []
        return _done(self._submit(VerifySsoDomain(domain=domain, found=found), sso_id, tenant, self.service_context))

    # --- passwords and email -------------------------------------------------------------------------------

    def change_password(self, claims: Claims, current: str | None, new: str) -> dict:
        """Changes the password; the other sessions end, this one stays."""
        return _done(self._submit(ChangePassword(current=current, new=new, keep_session_id=claims.sid), claims.sub, context=self._own(claims)))

    def request_password_reset(self, email: str) -> None:
        """Sends a reset mail if the address has an active account; answers the same either way. At
        most one mail per account in `MAIL_COOLDOWN_MINUTES` (the command sends nothing within it)."""
        account_id = self._find(FindAccountByEmail(email=email))
        if account_id:
            self._submit(RequestPasswordReset(), account_id, context=self.service_context)

    def reset_password(self, reset_token: str, new: str) -> dict:
        """Sets a new password with the mailed token; every session ends."""
        parts = parse_token(reset_token)
        if not parts:
            return {"ok": False, "reason": "invalid_token"}
        return _done(self._submit(ResetPassword(token=reset_token, new=new), parts[0]))

    def request_email_change(self, claims: Claims, email: str) -> dict:
        """Asks to change the account's address: a token goes to the new one."""
        return _done(self._submit(RequestEmailChange(email=email), claims.sub, context=self._own(claims)))

    def confirm_email_change(self, change_token: str) -> dict:
        """Confirms the new address with its token; a taken address is refused (`unique_taken`)."""
        parts = parse_token(change_token)
        if not parts:
            return {"ok": False, "reason": "invalid_token"}
        return _done(self._submit(ConfirmEmailChange(token=change_token), parts[0]))

    # --- invitations ---------------------------------------------------------------------------------------

    def peek_invitation(self, invitation_token: str) -> dict | None:
        """What the acceptance page shows for an invitation token."""
        t = parse_invitation_token(invitation_token)
        if not t:
            return None
        try:
            return self._ask(PeekInvitation(token=invitation_token), t["tenant"])
        except Rejected:
            return None

    def accept_invitation(self, invitation_token: str, *, claims: Claims | None = None, name: str | None = None,
                          password: str | None = None) -> dict:
        """Accepts an invitation: as the signed-in account (`claims`, its session checked as for every
        signed-in call, and against the invitation's tenant: active, its second factor, its enforced
        SSO, except for an owner invitation: owners are exempt from it, so they can mend it), or as a
        new person with a name and password, registered in the same step (active: the token proved
        the email). The accepting account's email must be the invited one. A new person's name and
        password are checked before the invitation is used, so a refused registration does not spend
        it. Either way a tenant that is not active takes nobody in: `tenant_suspended` or
        `tenant_not_active` (pending) from the middleware's check of a signed-in account, else
        `tenant_not_active` (the command's requirement cannot tell suspended from pending)."""
        peek = self.peek_invitation(invitation_token)
        if not peek or not peek.get("email"):
            return {"ok": False, "reason": "invalid_token"}
        if claims:
            signed_in = self._signed_in_account(claims)
            if not signed_in["ok"]:
                return signed_in
            may = self._may_join(claims, peek["tenant"])
            # The middleware checks the tenant (exists, active), then its second factor, then its
            # enforced SSO: `sso_required` means every other check passed, and an owner is exempt from it.
            if not may["ok"] and not (peek.get("owner") and may["reason"] == "sso_required"):
                return may
            account_id = claims.sub
            email = signed_in["account"].get("email") or ""
        else:
            if not isinstance(name, str) or not trim_space(name) or len(name) > 200:
                return {"ok": False, "reason": "invalid_name"}
            if not valid_password(password):
                return {"ok": False, "reason": "weak_password"}
            # Someone with an account signs in first, so nobody gets an account they could not use.
            if self._find(FindAccountByEmail(email=peek["email"])):
                return {"ok": False, "reason": "sign_in_first"}
            account_id, email = str(uuid.uuid4()), peek["email"]
        accepted = self._submit(AcceptInvitation(token=invitation_token, account_id=account_id, email=email, new_account=claims is None),
                                peek["invitation_id"], peek["tenant"], self.service_context)
        # The command requires the tenant active (its fact): missing, the tenant is pending, suspended or gone.
        if not accepted.committed:
            return {"ok": False, "reason": "tenant_not_active" if accepted.code == "requirement_missing" else accepted.code}
        if claims is None:
            registered = self._submit(RegisterAccount(email=email, name=name, password=password, verified=True), account_id,
                                      context=self.service_context)
            if not registered.committed:
                return {"ok": False, "reason": registered.code}
        return {"ok": True, "account_id": account_id, "tenant": peek["tenant"]}

    def create_invitation_link(self, context: RequestContext, tenant: str, groups: list[InvitedGroup], label: str | None = None,
                               expires_at: int | None = None, max_uses: int | None = None) -> dict:
        """A reusable invitation link into `groups` of the tenant, as `context` (whose groups the
        middleware checks): the token, shown this once, for a URL such as `/join?token=…`."""
        link_id, secret = str(uuid.uuid4()), random_secret()
        outcome = self._submit(CreateInvitationLink(secret=secret, groups=groups, label=label, expires_at=expires_at, max_uses=max_uses),
                               link_id, tenant, context)
        if not outcome.committed:
            return {"ok": False, "reason": outcome.code}
        return {"ok": True, "token": link_token(tenant, link_id, secret), "link_id": link_id}

    def peek_invitation_link(self, link: str) -> dict | None:
        """What the join page shows for a link token: the tenant, the link's label, whether it can be used."""
        t = parse_link_token(link)
        if not t:
            return None
        try:
            return self._ask(PeekInvitationLink(token=link), t["tenant"])
        except Rejected:
            return None

    def join_with_invitation_link(self, claims: Claims, link: str) -> dict:
        """The signed-in account joins the link's tenant with its groups (its session checked as for
        every signed-in call, and against the link's tenant: active, its second factor, its enforced
        SSO). Someone without an account registers and verifies first: a link proves
        no email."""
        t = parse_link_token(link)
        if not t:
            return {"ok": False, "reason": "invalid_token"}
        signed_in = self._signed_in_account(claims)
        if not signed_in["ok"]:
            return signed_in
        may = self._may_join(claims, t["tenant"])
        if not may["ok"]:
            return may
        outcome = self._submit(JoinWithInvitationLink(token=link, account_id=claims.sub), t["link_id"], t["tenant"], self.service_context)
        if outcome.committed:
            return {"ok": True, "tenant": t["tenant"]}
        return {"ok": False, "reason": "tenant_not_active" if outcome.code == "requirement_missing" else outcome.code}

    def sign_out(self, claims: Claims) -> None:
        """Signs out: ends the session."""
        outcome = self._submit(EndSession(reason="signed_out"), claims.sid, context=self._own(claims))
        if not outcome.committed:
            raise Rejected(outcome.message, outcome.code)
