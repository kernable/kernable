package kit

// The API part: framework-neutral functions for what only the API can do. Wire them into your
// HTTP framework.
//
// A refusal the caller shows (a wrong password, an expired link) is a *Refused with its code;
// anything else (the kernel unreachable) is the error as it came.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	kernable "github.com/kernable/kernable/sdks/go"
)

// Refused is a refusal with a code the caller can show or act on.
type Refused struct{ Code string }

func (r *Refused) Error() string { return "refused: " + r.Code }

// RefusedCode is the code of a refusal, or false if err is another error.
func RefusedCode(err error) (string, bool) {
	var r *Refused
	if errors.As(err, &r) {
		return r.Code, true
	}
	return "", false
}

func refused(code string) error { return &Refused{Code: code} }

// NewInvitationLink is an invitation link to create: KitAPI.CreateInvitationLink.
type NewInvitationLink struct {
	// The groups the people who join get.
	Groups []InvitedGroup
	Label  *string
	// Until when it works (ms since epoch); nil: until revoked.
	ExpiresAt *int64
	// How many people may join through it; nil: no limit.
	MaxUses *int64
}

// SignedInSession is a signed-in session: its tokens and who it is.
type SignedInSession struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	AccountID    string `json:"account_id"`
	SessionID    string `json:"session_id"`
	// The account has a second factor on: prove it before anything else.
	SecondFactor bool `json:"second_factor"`
}

// Provider is a social OAuth/OIDC provider; a tenant's SSO is configured in the account domain.
// An OpenID Connect provider is found by its Issuer; a plain OAuth2 one (GitHub) names its
// endpoints in OAuth2 (GitHubProvider).
type Provider struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	OAuth2       *OAuth2
}

// OAuth2 is the endpoints of an OAuth2 provider without OpenID Connect: where the browser signs
// in, where the code becomes an access token, and where the person (User: its id is the subject)
// and their email addresses (Emails: the primary verified one counts) are read.
type OAuth2 struct {
	Authorize string
	Token     string
	User      string
	Emails    string
	Scope     string
}

// OIDCProvider is an OpenID Connect provider at issuer (discovery at
// <issuer>/.well-known/openid-configuration).
func OIDCProvider(issuer, clientID, clientSecret string) Provider {
	return Provider{Issuer: issuer, ClientID: clientID, ClientSecret: clientSecret}
}

// GitHubProvider is GitHub (an OAuth app's client): web https://github.com and api
// https://api.github.com, or a GitHub Enterprise Server's.
func GitHubProvider(clientID, clientSecret, web, api string) Provider {
	web, api = strings.TrimRight(web, "/"), strings.TrimRight(api, "/")
	return Provider{Issuer: web, ClientID: clientID, ClientSecret: clientSecret, OAuth2: &OAuth2{
		Authorize: web + "/login/oauth/authorize",
		Token:     web + "/login/oauth/access_token",
		User:      api + "/user",
		Emails:    api + "/user/emails",
		Scope:     "read:user user:email",
	}}
}

// oidcState is what OIDCStart keeps in the cache until the browser comes back.
type oidcState struct {
	Provider string  `json:"provider"`
	Tenant   *string `json:"tenant"`
	SsoID    *string `json:"sso_id"`
	Verifier string  `json:"verifier"`
	Nonce    string  `json:"nonce"`
	Link     *string `json:"link"`
}

type cachedKey struct {
	context kernable.RequestContext
	until   time.Time
}

// KitAPI is what only your API does. Make it with NewKitAPI, then set providers and the redirect
// URI.
type KitAPI struct {
	client *kernable.Client
	keys   SigningKeys
	// ServiceContext is the request context of the API's own requests: lookups no one else may make.
	ServiceContext kernable.RequestContext
	providers      map[string]Provider
	redirectURI    string
	insecure       bool
	resolveTXT     func(ctx context.Context, name string) []string
	// HTTP is the client for the providers; default http.DefaultClient.
	HTTP *http.Client

	mu      sync.Mutex
	configs map[string]map[string]json.RawMessage
	apiKeys map[string]cachedKey
}

// NewKitAPI makes the API part. client: a client whose key may submit and query (it signs every
// request). keys: what access tokens are signed with. serviceIdentity and serviceKeyID: the API's
// own identity and key ID, from the bootstrap.
func NewKitAPI(client *kernable.Client, keys SigningKeys, serviceIdentity, serviceKeyID string) *KitAPI {
	return &KitAPI{
		client: client,
		keys:   keys,
		ServiceContext: kernable.RequestContext{
			SenderTenant: PlatformTenant, SenderIdentity: serviceIdentity, SenderSession: serviceKeyID,
		},
		providers: map[string]Provider{},
		HTTP:      http.DefaultClient,
		configs:   map[string]map[string]json.RawMessage{},
		apiKeys:   map[string]cachedKey{},
	}
}

// Provider adds a social OAuth/OIDC provider by name (google).
func (a *KitAPI) Provider(name string, p Provider) *KitAPI {
	a.providers[name] = p
	return a
}

// RedirectURI is where providers send the browser back to; your route calls OIDCFinish with the
// full URL.
func (a *KitAPI) RedirectURI(uri string) *KitAPI {
	a.redirectURI = uri
	return a
}

// InsecureIssuers allows http:// issuers: for development and tests only.
func (a *KitAPI) InsecureIssuers() *KitAPI {
	a.insecure = true
	return a
}

// ResolveTXT sets how DNS TXT records are read (tests replace it); default: the system's resolver.
func (a *KitAPI) ResolveTXT(f func(ctx context.Context, name string) []string) *KitAPI {
	a.resolveTXT = f
	return a
}

// --- small helpers -------------------------------------------------------------------------------

// commandAggregates is the domain and aggregate type of each command the API submits.
var commandAggregates = map[string][2]string{
	"RegisterAccount": {"account", "account"}, "VerifyEmail": {"account", "account"}, "SignIn": {"account", "account"},
	"NoteRegistrationAttempt": {"account", "account"}, "ProveSecondFactor": {"account", "account"},
	"StartTotp": {"account", "account"}, "ConfirmTotp": {"account", "account"}, "AddPasskey": {"account", "account"},
	"SignInWithPasskey": {"account", "account"}, "ProvePasskey": {"account", "account"},
	"LinkProvider": {"account", "account"}, "SignInWithProvider": {"account", "account"},
	"ChangePassword": {"account", "account"}, "RequestPasswordReset": {"account", "account"},
	"ResetPassword": {"account", "account"}, "RequestEmailChange": {"account", "account"},
	"ConfirmEmailChange": {"account", "account"},
	"RefreshSession":     {"account", "session"}, "EndSession": {"account", "session"},
	"VerifySsoDomain":  {"account", "sso"},
	"CreateApiKey":     {"identity", "identity"},
	"AcceptInvitation": {"invitation", "invitation"}, "CreateInvitationLink": {"invitation", "link"},
	"JoinWithInvitationLink": {"invitation", "link"},
}

func commandType(command any) string {
	name := fmt.Sprintf("%T", command)
	return name[strings.LastIndexByte(name, '.')+1:]
}

func (a *KitAPI) submit(ctx context.Context, command any, aggregateID, tenant string, rc kernable.RequestContext) (kernable.Outcome, error) {
	t := commandType(command)
	at, ok := commandAggregates[t]
	if !ok {
		panic("kit: the API submits no " + t)
	}
	return a.client.Submit(ctx, kernable.Command{Domain: at[0], AggregateType: at[1], AggregateID: aggregateID,
		Type: t, Data: command, Tenant: tenant, Context: &rc})
}

func (a *KitAPI) platform(ctx context.Context, command any, aggregateID string, rc kernable.RequestContext) (kernable.Outcome, error) {
	return a.submit(ctx, command, aggregateID, PlatformTenant, rc)
}

// ask is the API's own lookup; nil when the read model refuses (not found).
func ask[T any](ctx context.Context, a *KitAPI, domain string, query any, tenant string, allPartitions bool) (*T, error) {
	rc := a.ServiceContext
	answer, err := kernable.QueryAs[T](ctx, a.client, kernable.Query{Domain: domain, Type: commandType(query), Data: query,
		Tenant: tenant, Context: &rc, AllPartitions: allPartitions})
	var r *kernable.Rejected
	if errors.As(err, &r) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &answer, nil
}

// ownContext is the context of the token's account acting on itself.
func ownContext(c *Claims) kernable.RequestContext {
	return kernable.RequestContext{SenderAccount: c.Sub, SenderSession: c.Sid}
}

// asRefused turns a rejection into a refusal with its code.
func asRefused(err error) error {
	var r *kernable.Rejected
	if errors.As(err, &r) {
		return refused(r.Code)
	}
	return err
}

// signedInAccount is the token's account, read as itself: the middleware checks the session as
// for every signed-in request (active, the account active, a second factor proved if it has
// one). For what the API then does with its own context (accepting an invitation, joining
// through a link).
func (a *KitAPI) signedInAccount(ctx context.Context, c *Claims) (AccountRow, error) {
	rc := ownContext(c)
	row, err := kernable.QueryAs[AccountRow](ctx, a.client, kernable.Query{Domain: "account", Type: "GetAccount",
		Data: GetAccount{AccountID: c.Sub}, Tenant: PlatformTenant, Context: &rc})
	return row, asRefused(err)
}

// mayJoin checks the token's account against a tenant it is about to join, where it holds no
// identity yet: the middleware applies that tenant's checks to the account acting on itself (the
// tenant active, its second-factor requirement, its enforced SSO). A cheap query of one's own
// sessions in that tenant carries the check; its answer is not used.
func (a *KitAPI) mayJoin(ctx context.Context, c *Claims, tenant string) error {
	rc := ownContext(c)
	_, err := a.client.Query(ctx, kernable.Query{Domain: "account", Type: "ListSessions",
		Data: ListSessions{AccountID: c.Sub}, Tenant: tenant, Context: &rc})
	return asRefused(err)
}

// first is the data of the outcome's first event of type t.
func first(o kernable.Outcome, t string) map[string]any {
	for _, e := range o.Events {
		if e.Type == t {
			var data map[string]any
			_ = json.Unmarshal(e.Data, &data)
			if data == nil {
				data = map[string]any{}
			}
			return data
		}
	}
	return nil
}

// reasonOf is the first event's reason, or fallback.
func reasonOf(o kernable.Outcome, fallback string) string {
	if len(o.Events) > 0 {
		var data struct {
			Reason *string `json:"reason"`
		}
		if json.Unmarshal(o.Events[0].Data, &data) == nil && data.Reason != nil {
			return *data.Reason
		}
	}
	return fallback
}

// failedReason is why a sign-in that committed did not sign in, as the caller hears it: its
// SignInFailed reason, except that a locked account answers wrong_password like an unknown
// address, so nobody learns from it that an account exists (the event keeps locked).
func failedReason(o kernable.Outcome) string {
	if r := reasonOf(o, "wrong_password"); r != "locked" {
		return r
	}
	return "wrong_password"
}

func done(o kernable.Outcome, err error) error {
	if err != nil {
		return err
	}
	if !o.Committed {
		return refused(o.Code)
	}
	return nil
}

// joined is done for a command that requires the tenant active (its fact): missing, the tenant
// is pending, suspended or gone, which the requirement does not tell apart.
func joined(o kernable.Outcome, err error) error {
	if err == nil && !o.Committed && o.Code == "requirement_missing" {
		return refused("tenant_not_active")
	}
	return done(o, err)
}

func factors(data map[string]any) []string {
	list, _ := data["factors"].([]any)
	out := []string{}
	for _, f := range list {
		if s, ok := f.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (a *KitAPI) signedIn(accountID, sessionID, refreshToken string, data map[string]any, secondFactor *bool) SignedInSession {
	mfa, _ := data["mfa"].(bool)
	if secondFactor != nil {
		mfa = *secondFactor
	}
	return SignedInSession{
		AccessToken: IssueAccessToken(a.keys, accountID, sessionID, factors(data), 0), RefreshToken: refreshToken,
		AccountID: accountID, SessionID: sessionID, SecondFactor: mfa,
	}
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// --- registration and sign-in --------------------------------------------------------------------

// Register registers a person. Answers the same whether or not the email was taken: the caller
// shows "check your email" either way. The account ID it returns is the new one, or a decoy.
func (a *KitAPI) Register(ctx context.Context, email, name string, password *string) (string, error) {
	command := RegisterAccount{Email: email, Name: name, Password: password}
	accountID := NewID()
	o, err := a.platform(ctx, command, accountID, kernable.RequestContext{})
	if err != nil {
		return "", err
	}
	if o.Committed {
		return accountID, nil
	}
	if o.Code != "unique_taken" {
		return "", refused(o.Code)
	}
	// The address is held: a registration that expired unverified is taken over (same aggregate);
	// otherwise the same answer as success, so nobody learns who has an account.
	held, err := ask[Found](ctx, a, "account", FindAccountByEmail{Email: email}, PlatformTenant, false)
	if err != nil {
		return "", err
	}
	if held != nil {
		if held.Status == AccountStatusPending {
			o, err := a.platform(ctx, command, held.ID, kernable.RequestContext{})
			if err != nil {
				return "", err
			}
			if o.Committed {
				return held.ID, nil
			}
		} else if _, err := a.platform(ctx, NoteRegistrationAttempt{}, held.ID, a.ServiceContext); err != nil {
			// The account's owner learns that someone tried; the caller learns nothing.
			return "", err
		}
	}
	return NewID(), nil
}

// VerifyEmail confirms an email address with the token from the mail.
func (a *KitAPI) VerifyEmail(ctx context.Context, emailToken string) (bool, error) {
	accountID, _, _, ok := ParseToken(emailToken)
	if !ok {
		return false, nil
	}
	o, err := a.platform(ctx, VerifyEmail{Token: emailToken}, accountID, kernable.RequestContext{})
	return o.Committed, err
}

// SignIn signs in with email and password: an access token and a refresh token, or why not. An
// unknown address and a locked account answer wrong_password, and an unknown address takes as
// long (a password check against a dummy hash), so nobody learns whether an account exists.
func (a *KitAPI) SignIn(ctx context.Context, email, password string, ip, userAgent *string) (SignedInSession, error) {
	account, err := ask[Found](ctx, a, "account", FindAccountByEmail{Email: email}, PlatformTenant, false)
	if err != nil {
		return SignedInSession{}, err
	}
	if account == nil {
		CheckPassword("", password)
		return SignedInSession{}, refused("wrong_password")
	}
	sessionID := NewID()
	refreshToken := Token(sessionID, 1, RandomSecret(32))
	o, err := a.platform(ctx, SignIn{SessionID: sessionID, Password: password, RefreshToken: refreshToken, IP: ip, UserAgent: userAgent},
		account.ID, kernable.RequestContext{})
	if err != nil {
		return SignedInSession{}, err
	}
	if !o.Committed {
		return SignedInSession{}, refused(o.Code)
	}
	if data := first(o, "SignedIn"); data != nil {
		return a.signedIn(account.ID, sessionID, refreshToken, data, nil), nil
	}
	return SignedInSession{}, refused(failedReason(o))
}

// Check is the claims of a valid access token, else nil.
func (a *KitAPI) Check(accessToken string) *Claims { return CheckAccessToken(a.keys, accessToken, 0) }

// ContextFor is the request context for the token's holder in tenant (its identity there) or,
// with tenant "", only as a signed-in account (for authenticated requests). not_found without an
// identity in the tenant.
func (a *KitAPI) ContextFor(ctx context.Context, c *Claims, tenant string) (kernable.RequestContext, error) {
	if tenant == "" {
		return ownContext(c), nil
	}
	found, err := ask[FoundIdentity](ctx, a, "identity", FindIdentityByAccount{AccountID: c.Sub}, tenant, false)
	if err != nil {
		return kernable.RequestContext{}, err
	}
	if found == nil {
		return kernable.RequestContext{}, refused("not_found")
	}
	rc := ownContext(c)
	rc.SenderTenant, rc.SenderIdentity = tenant, found.IdentityID
	return rc, nil
}

// Refresh is a new access token and a rotated refresh token, in that order. An older refresh
// token is a reuse: the session ends, and so does every token of it.
func (a *KitAPI) Refresh(ctx context.Context, refreshToken string, ip *string) (string, string, error) {
	sessionID, generation, _, ok := ParseToken(refreshToken)
	if !ok {
		return "", "", refused("invalid_token")
	}
	digits := generation != "" && strings.Trim(generation, "0123456789") == ""
	g, err := strconv.ParseUint(generation, 10, 64)
	if !digits || err != nil || g == ^uint64(0) {
		return "", "", refused("invalid_token")
	}
	next := Token(sessionID, g+1, RandomSecret(32))
	o, err := a.platform(ctx, RefreshSession{RefreshToken: refreshToken, NewRefreshToken: next, IP: ip}, sessionID, kernable.RequestContext{})
	if err != nil {
		return "", "", err
	}
	if !o.Committed {
		return "", "", refused(o.Code)
	}
	d := first(o, "SessionRefreshed")
	if d == nil {
		return "", "", refused(reasonOf(o, "invalid_token"))
	}
	accountID, _ := d["accountId"].(string)
	return IssueAccessToken(a.keys, accountID, sessionID, factors(d), 0), next, nil
}

// ProveSecondFactor proves the second factor (a TOTP or recovery code) for the session: a new
// access token with it.
func (a *KitAPI) ProveSecondFactor(ctx context.Context, c *Claims, code string) (string, error) {
	o, err := a.platform(ctx, ProveSecondFactor{SessionID: c.Sid, Code: code}, c.Sub, kernable.RequestContext{})
	if err != nil {
		return "", err
	}
	if !o.Committed {
		return "", refused(o.Code)
	}
	proved := first(o, "SecondFactorProved")
	if proved == nil {
		return "", refused("wrong_code")
	}
	factor := "totp"
	if proved["method"] == "recovery" {
		factor = "recovery"
	}
	return IssueAccessToken(a.keys, c.Sub, c.Sid, append(append([]string{}, c.Fac...), factor), 0), nil
}

// StartTotp starts TOTP: a new secret, and the otpauth: URI an authenticator app reads (as a QR
// code).
func (a *KitAPI) StartTotp(ctx context.Context, c *Claims, issuer, label string) (secret, uri string, err error) {
	secret = TOTPSecret()
	if err := done(a.platform(ctx, StartTotp{Secret: secret}, c.Sub, ownContext(c))); err != nil {
		return "", "", err
	}
	return secret, "otpauth://totp/" + percent(issuer+":"+label) + "?secret=" + secret + "&issuer=" + percent(issuer), nil
}

// ConfirmTotp turns TOTP on with its first code: the recovery codes, shown this once.
func (a *KitAPI) ConfirmTotp(ctx context.Context, c *Claims, code string) ([]string, error) {
	codes := make([]string, 10)
	for i := range codes {
		codes[i] = RandomSecret(8)
	}
	if err := done(a.platform(ctx, ConfirmTotp{Code: code, RecoveryCodes: codes}, c.Sub, ownContext(c))); err != nil {
		return nil, err
	}
	return codes, nil
}

// --- API keys ------------------------------------------------------------------------------------

// CreateApiKey is a new API key for a service identity, shown this once: the key and its ID.
func (a *KitAPI) CreateApiKey(ctx context.Context, rc kernable.RequestContext, tenant, identityID, label string, expiresAt *int64) (key, keyID string, err error) {
	keyID = strings.Map(func(r rune) rune {
		if r < utf8.RuneSelf && (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return r
		}
		return 'x'
	}, RandomSecret(9))
	secret := RandomSecret(32)
	command := CreateApiKey{KeyID: keyID, Secret: secret, Label: &label, ExpiresAt: expiresAt}
	if err := done(a.submit(ctx, command, identityID, tenant, rc)); err != nil {
		return "", "", err
	}
	return APIKey(keyID, secret), keyID, nil
}

// CheckApiKey is the request context of an API key, or nil: checked against its hash, cached for
// a minute. A revoked key is refused by the middleware at once, cache or not.
func (a *KitAPI) CheckApiKey(ctx context.Context, key string) (*kernable.RequestContext, error) {
	keyID, hash, ok := ParseAPIKey(key)
	if !ok {
		return nil, nil
	}
	a.mu.Lock()
	cached, ok := a.apiKeys[hash]
	a.mu.Unlock()
	if ok && cached.until.After(time.Now()) {
		rc := cached.context
		return &rc, nil
	}
	found, err := ask[ApiKeyRow](ctx, a, "identity", FindApiKey{KeyID: keyID}, PlatformTenant, true)
	if err != nil || found == nil {
		return nil, err
	}
	expired := found.ExpiresAt != nil && time.Now().UnixMilli() > *found.ExpiresAt
	if found.Revoked || expired || !EqualSecrets(found.Hash, hash) {
		return nil, nil
	}
	rc := kernable.RequestContext{SenderTenant: found.Tenant, SenderIdentity: found.IdentityID, SenderSession: keyID}
	a.mu.Lock()
	a.apiKeys[hash] = cachedKey{context: rc, until: time.Now().Add(time.Minute)}
	a.mu.Unlock()
	return &rc, nil
}

// --- Passkeys: challenges live a few minutes in the kernable cache and count once. ---------------

func (a *KitAPI) cacheOptions() kernable.DataOptions {
	rc := a.ServiceContext
	return kernable.DataOptions{Tenant: PlatformTenant, Context: &rc, TTL: ChallengeSeconds * time.Second}
}

func (a *KitAPI) keep(ctx context.Context, key, value string) error {
	return a.client.Cache.Set(ctx, "account", key, []byte(value), a.cacheOptions())
}

// take is the cached value, deleted: it counts once.
func (a *KitAPI) take(ctx context.Context, key string) ([]byte, error) {
	cache := a.client.Cache
	value, err := cache.Get(ctx, "account", key, a.cacheOptions())
	if err != nil || value == nil {
		return nil, err
	}
	deleted, err := cache.Delete(ctx, "account", key, a.cacheOptions())
	if err != nil || !deleted {
		return nil, err
	}
	return value, nil
}

func clientDataChallenge(response map[string]any) string {
	r, _ := response["response"].(map[string]any)
	c, _ := r["clientDataJSON"].(string)
	raw, err := b64url(c)
	if err != nil {
		return ""
	}
	var d map[string]any
	if json.Unmarshal(raw, &d) != nil {
		return ""
	}
	challenge, _ := d["challenge"].(string)
	return challenge
}

// takeChallenge is the challenge a browser response answers, if it is one the API issued and
// nobody used yet.
func (a *KitAPI) takeChallenge(ctx context.Context, response map[string]any) (string, error) {
	challenge := clientDataChallenge(response)
	if challenge == "" {
		return "", nil
	}
	value, err := a.take(ctx, "challenge:"+challenge)
	if err != nil || value == nil {
		return "", err
	}
	return challenge, nil
}

func (a *KitAPI) newChallenge(ctx context.Context) (string, error) {
	challenge := base64.RawURLEncoding.EncodeToString(RandomBytes(32))
	return challenge, a.keep(ctx, "challenge:"+challenge, "1")
}

// PasskeyRegistrationOptions is what the browser needs to create a passkey for the signed-in
// account (JSON for navigator.credentials.create, as PublicKeyCredentialCreationOptionsJSON).
func (a *KitAPI) PasskeyRegistrationOptions(ctx context.Context, c *Claims, userName string) (map[string]any, error) {
	challenge, err := a.newChallenge(ctx)
	if err != nil {
		return nil, err
	}
	rp := Passkeys()
	params := []any{}
	for _, alg := range PasskeyAlgorithms {
		params = append(params, map[string]any{"alg": alg, "type": "public-key"})
	}
	return map[string]any{
		"challenge":          challenge,
		"rp":                 map[string]any{"name": rp.RPName, "id": rp.RPID},
		"user":               map[string]any{"id": base64.RawURLEncoding.EncodeToString([]byte(c.Sub)), "name": userName, "displayName": userName},
		"pubKeyCredParams":   params,
		"excludeCredentials": []any{},
		"timeout":            ChallengeSeconds * 1000,
		"attestation":        "none",
		"authenticatorSelection": map[string]any{
			"residentKey": "required", "requireResidentKey": true, "userVerification": "required",
		},
	}, nil
}

// AddPasskey adds the passkey the browser created.
func (a *KitAPI) AddPasskey(ctx context.Context, c *Claims, response map[string]any, label *string) error {
	challenge, err := a.takeChallenge(ctx, response)
	if err != nil {
		return err
	}
	if challenge == "" {
		return refused("invalid_challenge")
	}
	return done(a.platform(ctx, AddPasskey{Challenge: challenge, Response: response, Label: label}, c.Sub, ownContext(c)))
}

// PasskeySignInOptions is what the browser needs to sign in with a passkey (or prove one): any of
// the user's passkeys.
func (a *KitAPI) PasskeySignInOptions(ctx context.Context) (map[string]any, error) {
	challenge, err := a.newChallenge(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"challenge": challenge, "rpId": Passkeys().RPID, "timeout": ChallengeSeconds * 1000,
		"userVerification": "required"}, nil
}

// SignInWithPasskey signs in with a passkey, without a password: the passkey names its account
// (its user handle).
func (a *KitAPI) SignInWithPasskey(ctx context.Context, response map[string]any, ip *string) (SignedInSession, error) {
	challenge, err := a.takeChallenge(ctx, response)
	if err != nil {
		return SignedInSession{}, err
	}
	r, _ := response["response"].(map[string]any)
	handle, _ := r["userHandle"].(string)
	raw, err := b64url(handle)
	accountID := ""
	if err == nil && utf8.Valid(raw) {
		accountID = string(raw)
	}
	if challenge == "" || accountID == "" {
		return SignedInSession{}, refused("wrong_password")
	}
	sessionID := NewID()
	refreshToken := Token(sessionID, 1, RandomSecret(32))
	command := SignInWithPasskey{SessionID: sessionID, Challenge: challenge, Response: response, RefreshToken: refreshToken, IP: ip}
	o, err := a.platform(ctx, command, accountID, a.ServiceContext)
	if err != nil {
		return SignedInSession{}, err
	}
	if !o.Committed {
		return SignedInSession{}, refused("wrong_password")
	}
	if data := first(o, "SignedIn"); data != nil {
		no := false
		return a.signedIn(accountID, sessionID, refreshToken, data, &no), nil
	}
	return SignedInSession{}, refused(failedReason(o))
}

// ProvePasskey proves a passkey as the second factor of a password session: a new access token
// with it.
func (a *KitAPI) ProvePasskey(ctx context.Context, c *Claims, response map[string]any) (string, error) {
	challenge, err := a.takeChallenge(ctx, response)
	if err != nil {
		return "", err
	}
	if challenge == "" {
		return "", refused("invalid_challenge")
	}
	o, err := a.platform(ctx, ProvePasskey{SessionID: c.Sid, Challenge: challenge, Response: response}, c.Sub, a.ServiceContext)
	if err != nil {
		return "", err
	}
	if first(o, "SecondFactorProved") == nil {
		return "", refused("wrong_passkey")
	}
	return IssueAccessToken(a.keys, c.Sub, c.Sid, append(append([]string{}, c.Fac...), "passkey"), 0), nil
}

// --- OAuth/OIDC and SSO: the API does the exchange; the account decides. -------------------------

func (a *KitAPI) getJSON(ctx context.Context, u string, header http.Header, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	return a.do(req, into)
}

func (a *KitAPI) do(req *http.Request, into any) error {
	res, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, into)
}

// config is the provider's discovery document with its keys (under jwks), kept once fetched.
func (a *KitAPI) config(ctx context.Context, issuer string) (map[string]json.RawMessage, error) {
	if !a.insecure && !strings.HasPrefix(issuer, "https://") {
		return nil, fmt.Errorf("%s is not https", issuer)
	}
	a.mu.Lock()
	c, ok := a.configs[issuer]
	a.mu.Unlock()
	if ok {
		return c, nil
	}
	var config map[string]json.RawMessage
	if err := a.getJSON(ctx, strings.TrimRight(issuer, "/")+"/.well-known/openid-configuration", nil, &config); err != nil {
		return nil, err
	}
	// The endpoints it names carry the client secret, codes and tokens: https too.
	for _, key := range []string{"jwks_uri", "token_endpoint", "authorization_endpoint"} {
		if u := jsonString(config[key]); u != "" && !a.secure(u) {
			return nil, fmt.Errorf("%s %s is not https", key, u)
		}
	}
	jwksURI := jsonString(config["jwks_uri"])
	if jwksURI == "" {
		return nil, errors.New("no jwks_uri")
	}
	var jwks json.RawMessage
	if err := a.getJSON(ctx, jwksURI, nil, &jwks); err != nil {
		return nil, err
	}
	config["jwks"] = jwks
	a.mu.Lock()
	a.configs[issuer] = config
	a.mu.Unlock()
	return config, nil
}

// jsonString is a JSON string's value, else "".
func jsonString(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

type ssoSettings struct {
	name     string
	provider Provider
	domains  []string
}

// ssoSettings is a tenant's SSO: its provider name, what the API signs in with, and the domains
// it verified.
func (a *KitAPI) ssoSettings(ctx context.Context, tenant, ssoID string) (*ssoSettings, error) {
	found, err := ask[SsoSecret](ctx, a, "account", GetSsoSecret{SsoID: ssoID}, tenant, false)
	if err != nil || found == nil || found.ClientSecret == nil || *found.ClientSecret == "" {
		return nil, err
	}
	domains := found.Domains
	if domains == nil {
		domains = []string{}
	}
	return &ssoSettings{name: "sso:" + ssoID, domains: domains,
		provider: Provider{Issuer: found.Issuer, ClientID: found.ClientID, ClientSecret: *found.ClientSecret}}, nil
}

// OIDCStart is where to send the browser: a social provider by name, or with provider "" a
// person's tenant SSO by their email. With link, the signed-in account links the login instead
// of signing in.
func (a *KitAPI) OIDCStart(ctx context.Context, provider, email string, link *Claims) (string, error) {
	var name string
	var p Provider
	var tenant, ssoID *string
	if provider != "" {
		found, ok := a.providers[provider]
		if !ok {
			return "", refused("no_provider")
		}
		name, p = provider, found
	} else {
		found, err := ask[SsoFound](ctx, a, "account", FindSsoByDomain{Domain: EmailDomain(email)}, PlatformTenant, true)
		if err != nil {
			return "", err
		}
		if found == nil {
			return "", refused("no_provider")
		}
		s, err := a.ssoSettings(ctx, found.Tenant, found.SsoID)
		if err != nil {
			return "", err
		}
		if s == nil {
			return "", refused("no_provider")
		}
		name, p, tenant, ssoID = s.name, s.provider, &found.Tenant, &found.SsoID
	}
	if a.redirectURI == "" {
		return "", refused("no_provider")
	}
	verifier, state, nonce := RandomSecret(48), RandomSecret(24), RandomSecret(24)
	var authorize, scope string
	if p.OAuth2 != nil {
		if !a.secure(p.OAuth2.Authorize) {
			return "", refused("no_provider")
		}
		authorize, scope = p.OAuth2.Authorize, p.OAuth2.Scope
	} else {
		config, err := a.config(ctx, p.Issuer)
		if err != nil {
			return "", refused("no_provider")
		}
		authorize, scope = jsonString(config["authorization_endpoint"]), "openid email"
	}
	kept := oidcState{Provider: name, Tenant: tenant, SsoID: ssoID, Verifier: verifier, Nonce: nonce}
	if link != nil {
		kept.Link = &link.Sub
	}
	if err := a.keep(ctx, "oidc:"+state, string(compactJSON(kept))); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(digest[:])
	u, err := url.Parse(authorize)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", refused("no_provider")
	}
	pairs := [][2]string{{"response_type", "code"}, {"client_id", p.ClientID}, {"redirect_uri", a.redirectURI},
		{"scope", scope}, {"state", state}}
	// Only OpenID Connect knows a nonce (it comes back in the ID token).
	if p.OAuth2 == nil {
		pairs = append(pairs, [2]string{"nonce", nonce})
	}
	pairs = append(pairs, [2]string{"code_challenge_method", "S256"}, [2]string{"code_challenge", challenge})
	var q strings.Builder
	q.WriteString(u.RawQuery)
	for _, kv := range pairs {
		if q.Len() > 0 {
			q.WriteByte('&')
		}
		q.WriteString(formEncode(kv[0]) + "=" + formEncode(kv[1]))
	}
	u.RawQuery = q.String()
	return u.String(), nil
}

// secure is whether u may be used: https, or anything with InsecureIssuers (tests).
func (a *KitAPI) secure(u string) bool { return a.insecure || strings.HasPrefix(u, "https://") }

func postForm(ctx context.Context, u string, form [][2]string) (*http.Request, error) {
	var b strings.Builder
	for i, kv := range form {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(formEncode(kv[0]) + "=" + formEncode(kv[1]))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(b.String()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

// exchangeOAuth2 is, for an OAuth2 provider, the person behind the code, as claims an ID token
// would hold (sub: their id; email and email_verified: their primary verified address, else none).
func (a *KitAPI) exchangeOAuth2(ctx context.Context, p Provider, code string, kept oidcState) (map[string]any, error) {
	o := p.OAuth2
	for _, u := range []string{o.Token, o.User, o.Emails} {
		if !a.secure(u) {
			return nil, fmt.Errorf("%s is not https", u)
		}
	}
	req, err := postForm(ctx, o.Token, [][2]string{{"grant_type", "authorization_code"}, {"code", code},
		{"client_id", p.ClientID}, {"client_secret", p.ClientSecret}, {"redirect_uri", a.redirectURI},
		{"code_verifier", kept.Verifier}})
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	var tokens map[string]any
	if err := a.do(req, &tokens); err != nil {
		return nil, err
	}
	access, _ := tokens["access_token"].(string)
	if access == "" {
		return nil, fmt.Errorf("no access_token: %v", tokens["error"])
	}
	header := http.Header{"Authorization": {"Bearer " + access}, "Accept": {"application/json"}, "User-Agent": {"kernable-kit"}}
	var user map[string]any
	d := func(u string, into any) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		req.Header = header.Clone()
		res, err := a.HTTP.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		dec := json.NewDecoder(io.LimitReader(res.Body, 1<<20))
		dec.UseNumber()
		return dec.Decode(into)
	}
	if err := d(o.User, &user); err != nil {
		return nil, err
	}
	var sub string
	switch id := user["id"].(type) {
	case json.Number:
		sub = id.String()
	case string:
		sub = id
	}
	if sub == "" {
		return nil, errors.New("no id")
	}
	var emails any
	if err := d(o.Emails, &emails); err != nil {
		return nil, err
	}
	email, ok := OAuth2Email(emails)
	return map[string]any{"sub": sub, "email": email, "email_verified": ok}, nil
}

// exchange is the ID token's claims for the code in callback, checked (issuer, audience, nonce,
// expiry); for an OAuth2 provider, the same claims read from it (exchangeOAuth2).
func (a *KitAPI) exchange(ctx context.Context, p Provider, callback *url.URL, kept oidcState) (map[string]any, error) {
	code := queryValue(callback, "code")
	if code == nil {
		return nil, errors.New("no code")
	}
	if p.OAuth2 != nil {
		return a.exchangeOAuth2(ctx, p, *code, kept)
	}
	config, err := a.config(ctx, p.Issuer)
	if err != nil {
		return nil, err
	}
	endpoint := jsonString(config["token_endpoint"])
	if endpoint == "" {
		return nil, errors.New("no token_endpoint")
	}
	req, err := postForm(ctx, endpoint, [][2]string{{"grant_type", "authorization_code"}, {"code", *code},
		{"redirect_uri", a.redirectURI}, {"code_verifier", kept.Verifier}})
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(percent(p.ClientID), percent(p.ClientSecret))
	res, err := a.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, fmt.Errorf("token endpoint: %s", res.Status)
	}
	var tokens map[string]any
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&tokens); err != nil {
		return nil, err
	}
	idToken, _ := tokens["id_token"].(string)
	if idToken == "" {
		return nil, errors.New("no id_token")
	}
	claims, err := verifyIDToken(idToken, config["jwks"], p.ClientID, jsonString(config["issuer"]))
	if err != nil {
		return nil, err
	}
	if nonce, _ := claims["nonce"].(string); nonce != kept.Nonce {
		return nil, errors.New("another nonce")
	}
	return claims, nil
}

// queryValue is the first value of key in u's query, decoded as a form, or nil.
func queryValue(u *url.URL, key string) *string {
	for _, part := range strings.Split(u.RawQuery, "&") {
		k, v, _ := strings.Cut(part, "=")
		if formDecode(k) == key {
			s := formDecode(v)
			return &s
		}
	}
	return nil
}

// OIDCFinish is for when the browser came back (callback is the full URL). Signs in a linked
// login; links a tenant SSO login to the account with the same verified email; registers a new
// person whose email nobody has; refuses a social login whose email has an account (not_linked:
// sign in and link it). A tenant's SSO answers only for its verified domains: an unlinked login
// with another email is refused (email_not_in_domain), so a tenant's provider cannot take over
// anyone else's account. nil without an error: the signed-in account linked the login (OIDCStart
// with link).
func (a *KitAPI) OIDCFinish(ctx context.Context, callback string, ip *string) (*SignedInSession, error) {
	u, err := url.Parse(callback)
	if err != nil || u.Scheme == "" {
		return nil, refused("invalid_state")
	}
	state := ""
	if s := queryValue(u, "state"); s != nil {
		state = *s
	}
	var kept oidcState
	if state == "" {
		return nil, refused("invalid_state")
	}
	raw, err := a.take(ctx, "oidc:"+state)
	if err != nil {
		return nil, err
	}
	if raw == nil || json.Unmarshal(raw, &kept) != nil {
		return nil, refused("invalid_state")
	}
	var p Provider
	var domains []string
	if kept.Tenant != nil && kept.SsoID != nil {
		s, err := a.ssoSettings(ctx, *kept.Tenant, *kept.SsoID)
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, refused("no_provider")
		}
		p, domains = s.provider, s.domains
	} else {
		found, ok := a.providers[kept.Provider]
		if !ok {
			return nil, refused("no_provider")
		}
		p = found
	}
	claims, err := a.exchange(ctx, p, u, kept)
	if err != nil {
		return nil, refused("provider_refused")
	}
	email, _ := claims["email"].(string)
	emailVerified, _ := claims["email_verified"].(bool)
	subject, _ := claims["sub"].(string)
	provider := kept.Provider
	link := func(account string) (kernable.Outcome, error) {
		return a.platform(ctx, LinkProvider{Provider: provider, Subject: subject, Email: &email}, account, a.ServiceContext)
	}
	if kept.Link != nil {
		return nil, done(link(*kept.Link))
	}
	byLink, err := ask[Found](ctx, a, "account", FindAccountByLink{Provider: provider, Subject: subject}, PlatformTenant, false)
	if err != nil {
		return nil, err
	}
	sso := strings.HasPrefix(provider, "sso:")
	// Not linked yet: a tenant's SSO speaks only for the domains its tenant verified.
	if byLink == nil && sso && !contains(domains, EmailDomain(email)) {
		return nil, refused("email_not_in_domain")
	}
	found := byLink
	if found == nil && email != "" {
		if found, err = ask[Found](ctx, a, "account", FindAccountByEmail{Email: email}, PlatformTenant, false); err != nil {
			return nil, err
		}
	}
	var accountID string
	if found != nil {
		accountID = found.ID
	} else {
		// A new person: registered (verified if the provider says so), the login linked.
		if email == "" {
			return nil, refused("no_email")
		}
		// A social login registers only where anyone may: with sign-up by invitation, the person
		// accepts an invitation first and links the login after. A tenant's SSO speaks for its
		// own verified domains, so it registers either way.
		if !sso {
			policy, err := ask[SignUpPolicy](ctx, a, "tenancy", GetSignUpPolicy{}, PlatformTenant, false)
			if err != nil {
				return nil, err
			}
			if policy == nil || policy.Accounts != AccountSignUpOpen {
				return nil, refused("sign_up_closed")
			}
		}
		accountID = NewID()
		name, _, _ := strings.Cut(email, "@")
		register := RegisterAccount{Email: email, Name: name, Verified: &emailVerified}
		if err := done(a.platform(ctx, register, accountID, a.ServiceContext)); err != nil {
			return nil, err
		}
		if !sso {
			if _, err := link(accountID); err != nil {
				return nil, err
			}
		}
	}
	sessionID := NewID()
	refreshToken := Token(sessionID, 1, RandomSecret(32))
	command := SignInWithProvider{SessionID: sessionID, Provider: provider, Subject: subject, Email: &email,
		EmailVerified: &emailVerified, AutoLink: &sso, Domains: domains, RefreshToken: refreshToken, IP: ip}
	o, err := a.platform(ctx, command, accountID, a.ServiceContext)
	if err != nil {
		return nil, err
	}
	if !o.Committed {
		return nil, refused(o.Code)
	}
	if data := first(o, "SignedIn"); data != nil {
		s := a.signedIn(accountID, sessionID, refreshToken, data, nil)
		return &s, nil
	}
	return nil, refused(failedReason(o))
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// VerifySsoDomain looks for the SSO domain's TXT record (_kernable-sso.<domain>) and verifies the
// domain if it is there.
func (a *KitAPI) VerifySsoDomain(ctx context.Context, tenant, ssoID, domain string) error {
	name := "_kernable-sso." + domain
	var found []string
	if a.resolveTXT != nil {
		found = a.resolveTXT(ctx, name)
	} else if records, err := net.DefaultResolver.LookupTXT(ctx, name); err == nil {
		found = records
	}
	if found == nil {
		found = []string{}
	}
	return done(a.submit(ctx, VerifySsoDomain{Domain: domain, Found: found}, ssoID, tenant, a.ServiceContext))
}

// --- passwords and email -------------------------------------------------------------------------

// ChangePassword changes the password; the other sessions end, this one stays.
func (a *KitAPI) ChangePassword(ctx context.Context, c *Claims, current *string, newPassword string) error {
	sid := c.Sid
	return done(a.platform(ctx, ChangePassword{Current: current, New: newPassword, KeepSessionID: &sid}, c.Sub, ownContext(c)))
}

// RequestPasswordReset sends a reset mail if the address has an active account; answers the same
// either way. At most one mail per account in MailCooldownMinutes (the command sends nothing
// within it).
func (a *KitAPI) RequestPasswordReset(ctx context.Context, email string) error {
	held, err := ask[Found](ctx, a, "account", FindAccountByEmail{Email: email}, PlatformTenant, false)
	if err != nil || held == nil {
		return err
	}
	_, err = a.platform(ctx, RequestPasswordReset{}, held.ID, a.ServiceContext)
	return err
}

// ResetPassword sets a new password with the mailed token; every session ends.
func (a *KitAPI) ResetPassword(ctx context.Context, resetToken, newPassword string) error {
	accountID, _, _, ok := ParseToken(resetToken)
	if !ok {
		return refused("invalid_token")
	}
	return done(a.platform(ctx, ResetPassword{Token: resetToken, New: newPassword}, accountID, kernable.RequestContext{}))
}

// RequestEmailChange asks to change the account's address: a token goes to the new one.
func (a *KitAPI) RequestEmailChange(ctx context.Context, c *Claims, email string) error {
	return done(a.platform(ctx, RequestEmailChange{Email: email}, c.Sub, ownContext(c)))
}

// ConfirmEmailChange confirms the new address with its token; a taken address is refused
// (unique_taken).
func (a *KitAPI) ConfirmEmailChange(ctx context.Context, changeToken string) error {
	accountID, _, _, ok := ParseToken(changeToken)
	if !ok {
		return refused("invalid_token")
	}
	return done(a.platform(ctx, ConfirmEmailChange{Token: changeToken}, accountID, kernable.RequestContext{}))
}

// --- invitations ---------------------------------------------------------------------------------

// PeekInvitation is what the acceptance page shows for an invitation token, or nil.
func (a *KitAPI) PeekInvitation(ctx context.Context, invitationToken string) (*Peeked, error) {
	t, ok := ParseInvitationToken(invitationToken)
	if !ok {
		return nil, nil
	}
	return ask[Peeked](ctx, a, "invitation", PeekInvitation{Token: invitationToken}, t.Tenant, false)
}

// NewPerson is who accepts an invitation without an account: KitAPI.AcceptInvitation.
type NewPerson struct{ Name, Password string }

// AcceptInvitation accepts an invitation: as the signed-in account (c, its session checked as for
// every signed-in call, and against the invitation's tenant: active, its second factor, its
// enforced SSO, except for an owner invitation: owners are exempt from it, so they can mend it),
// or as a new person with a name and password (person), registered in the same step (active: the
// token proved the email). The accepting account's email must be the invited one. A new person's
// name and password are checked before the invitation is used, so a refused registration does not
// spend it. Either way a tenant that is not active takes nobody in: tenant_suspended or
// tenant_not_active (pending) from the middleware's check of a signed-in account, else
// tenant_not_active (the command's requirement cannot tell suspended from pending). The account
// and tenant.
func (a *KitAPI) AcceptInvitation(ctx context.Context, invitationToken string, c *Claims, person *NewPerson) (accountID, tenant string, err error) {
	peek, err := a.PeekInvitation(ctx, invitationToken)
	if err != nil {
		return "", "", err
	}
	if peek == nil || peek.Email == nil || *peek.Email == "" {
		return "", "", refused("invalid_token")
	}
	invited := *peek.Email
	var email string
	if c != nil {
		account, err := a.signedInAccount(ctx, c)
		if err != nil {
			return "", "", err
		}
		// The middleware checks the tenant (exists, active), then its second factor, then its
		// enforced SSO: sso_required means every other check passed, and an owner is exempt from it.
		if err := a.mayJoin(ctx, c, peek.Tenant); err != nil {
			if code, ok := RefusedCode(err); !ok || !peek.Owner || code != "sso_required" {
				return "", "", err
			}
		}
		accountID, email = c.Sub, deref(account.Email)
	} else {
		if person == nil {
			person = &NewPerson{}
		}
		if TrimSpace(person.Name) == "" || utf8.RuneCountInString(person.Name) > 200 {
			return "", "", refused("invalid_name")
		}
		if !ValidPassword(person.Password) {
			return "", "", refused("weak_password")
		}
		// Someone with an account signs in first, so nobody gets an account they could not use.
		held, err := ask[Found](ctx, a, "account", FindAccountByEmail{Email: invited}, PlatformTenant, false)
		if err != nil {
			return "", "", err
		}
		if held != nil {
			return "", "", refused("sign_in_first")
		}
		accountID, email = NewID(), invited
	}
	accept := AcceptInvitation{Token: invitationToken, AccountID: accountID, Email: email, NewAccount: c == nil}
	if err := joined(a.submit(ctx, accept, peek.InvitationID, peek.Tenant, a.ServiceContext)); err != nil {
		return "", "", err
	}
	if c == nil {
		yes := true
		password := person.Password
		register := RegisterAccount{Email: email, Name: person.Name, Password: &password, Verified: &yes}
		if err := done(a.platform(ctx, register, accountID, a.ServiceContext)); err != nil {
			return "", "", err
		}
	}
	return accountID, peek.Tenant, nil
}

// CreateInvitationLink is a reusable invitation link into the groups of the tenant, as rc (whose
// groups the middleware checks): the token, shown this once, for a URL such as /join?token=…,
// and the link's ID.
func (a *KitAPI) CreateInvitationLink(ctx context.Context, rc kernable.RequestContext, tenant string, link NewInvitationLink) (token, linkID string, err error) {
	linkID = NewID()
	secret := RandomSecret(32)
	groups := link.Groups
	if groups == nil {
		groups = []InvitedGroup{}
	}
	command := CreateInvitationLink{Secret: secret, Groups: groups, Label: link.Label, ExpiresAt: link.ExpiresAt, MaxUses: link.MaxUses}
	if err := done(a.submit(ctx, command, linkID, tenant, rc)); err != nil {
		return "", "", err
	}
	return LinkTokenOf(tenant, linkID, secret), linkID, nil
}

// PeekInvitationLink is what the join page shows for a link token: the tenant, the link's label,
// whether it can be used; or nil.
func (a *KitAPI) PeekInvitationLink(ctx context.Context, link string) (*PeekedLink, error) {
	t, ok := ParseLinkToken(link)
	if !ok {
		return nil, nil
	}
	return ask[PeekedLink](ctx, a, "invitation", PeekInvitationLink{Token: link}, t.Tenant, false)
}

// JoinWithInvitationLink has the signed-in account join the link's tenant with its groups (its
// session checked as for every signed-in call, and against the link's tenant: active, its second
// factor, its enforced SSO): the tenant. Someone without an account registers and verifies first:
// a link proves no email.
func (a *KitAPI) JoinWithInvitationLink(ctx context.Context, c *Claims, link string) (string, error) {
	t, ok := ParseLinkToken(link)
	if !ok {
		return "", refused("invalid_token")
	}
	if _, err := a.signedInAccount(ctx, c); err != nil {
		return "", err
	}
	if err := a.mayJoin(ctx, c, t.Tenant); err != nil {
		return "", err
	}
	join := JoinWithInvitationLink{Token: link, AccountID: c.Sub}
	if err := joined(a.submit(ctx, join, t.LinkID, t.Tenant, a.ServiceContext)); err != nil {
		return "", err
	}
	return t.Tenant, nil
}

// SignOut ends the session.
func (a *KitAPI) SignOut(ctx context.Context, c *Claims) error {
	reason := EndSignedOut
	return done(a.platform(ctx, EndSession{Reason: &reason}, c.Sid, ownContext(c)))
}

// --- encodings -----------------------------------------------------------------------------------

// percent is s percent-encoded, every byte but letters, digits and -._~ (RFC 3986's unreserved).
func percent(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("-._~", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// formEncode is s as application/x-www-form-urlencoded writes it (the WHATWG URL standard):
// letters, digits and *-._ as they are, a space as +, every other byte percent-encoded.
func formEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("*-._", c) >= 0:
			b.WriteByte(c)
		case c == ' ':
			b.WriteByte('+')
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// formDecode is the inverse of formEncode, lenient like a browser: an invalid escape stays as it is.
func formDecode(s string) string {
	var b bytes.Buffer
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '+':
			b.WriteByte(' ')
		case c == '%' && i+2 < len(s) && isHex(s[i+1]) && isHex(s[i+2]):
			v, _ := strconv.ParseUint(s[i+1:i+3], 16, 8)
			b.WriteByte(byte(v))
			i += 2
		default:
			b.WriteByte(c)
		}
	}
	return strings.ToValidUTF8(b.String(), "�")
}

func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

// OAuth2Email is the address an OAuth2 provider's email list ([{email, primary, verified}]) says
// is the person's: the primary one if verified, else the first verified; false if none is
// verified.
func OAuth2Email(emails any) (string, bool) {
	list, _ := emails.([]any)
	verified := func(e map[string]any) bool { v, _ := e["verified"].(bool); return v }
	var pick map[string]any
	for _, x := range list {
		if e, ok := x.(map[string]any); ok && verified(e) && e["primary"] == true {
			pick = e
			break
		}
	}
	if pick == nil {
		for _, x := range list {
			if e, ok := x.(map[string]any); ok && verified(e) {
				pick = e
				break
			}
		}
	}
	if email, _ := pick["email"].(string); email != "" {
		return email, true
	}
	return "", false
}
