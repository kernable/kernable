// The kit parity runner for Go (see kits/parity/README.md). Run by kits/parity/run.ts:
//
//	go build -o target/parity-go ./internal/kitparity
//	parity-go host     # the kit's domains and the parity shop, until SIGTERM
//	parity-go driver   # runs the scenario through KitAPI, writes the transcript
//
// Both read PARITY_URL, PARITY_KEY, PARITY_DIR; the driver also PARITY_SCENARIO, PARITY_OUT,
// PARITY_KERNABLE (the kernel binary) and PARITY_STORE. Results are written in the shapes the
// TypeScript kit answers in ({ ok, reason }, camelCase keys), so they compare.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
	"github.com/kernable/kernable/sdks/go/kit"
)

func env(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(name + " is not set")
	}
	return v
}

func client() *kernable.Client {
	c, err := kernable.New(kernable.Options{URL: env("PARITY_URL"), Key: env("PARITY_KEY"), Service: "kit"})
	if err != nil {
		panic(err)
	}
	return c
}

// --- Mails go to a file both processes see: the host's reactors send, the driver reads. ----------

type fileMailer struct {
	path string
	mu   sync.Mutex
}

func (m *fileMailer) Send(mail kit.Mail) {
	m.mu.Lock()
	defer m.mu.Unlock()
	line, _ := json.Marshal(map[string]any{"to": mail.To, "subject": mail.Subject, "text": mail.Text, "purpose": mail.Purpose, "token": mail.Token})
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

func readMails(path string) []map[string]any {
	raw, _ := os.ReadFile(path)
	var out []map[string]any
	for _, l := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		out = append(out, parse([]byte(l)).(map[string]any))
	}
	return out
}

// --- Host ------------------------------------------------------------------------------------------

func host() {
	dir := env("PARITY_DIR")
	kit.UseMailer(&fileMailer{path: dir + "/mails.jsonl"})
	kit.UseAuthorizationStore("sqlite:" + dir + "/authorization.db")
	// The app's relations first: the kit's middleware reads them when its domains are built.
	s := shop()
	app, err := client().App(kernable.AppOptions{Service: "kit", Domains: append(kit.KitDomains(), s)})
	if err != nil {
		panic(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	go func() {
		if err := app.Run(ctx); err != nil && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "app:", err)
			os.Exit(1)
		}
	}()
	<-app.Ready()
	fmt.Println("ready")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	<-signals
	stop()
	time.Sleep(200 * time.Millisecond)
	os.Exit(0)
}

// --- Driver ----------------------------------------------------------------------------------------

type obj = map[string]any

// parse is JSON with numbers kept as they are written.
func parse(raw []byte) any {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		panic(fmt.Sprintf("%v: %s", err, raw))
	}
	return v
}

// toJSON is v as the transcript holds it: through JSON, numbers kept.
func toJSON(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return parse(raw)
}

func contextJSON(c kernable.RequestContext) obj {
	out := obj{}
	for k, v := range map[string]string{"senderTenant": c.SenderTenant, "senderIdentity": c.SenderIdentity,
		"senderAccount": c.SenderAccount, "senderSession": c.SenderSession, "onBehalfOf": c.OnBehalfOf} {
		if v != "" {
			out[k] = v
		}
	}
	if c.BypassAuthorization {
		out["bypassAuthorization"] = true
	}
	return out
}

func toContext(j any) kernable.RequestContext {
	m, _ := j.(obj)
	s := func(k string) string { v, _ := m[k].(string); return v }
	bypass, _ := m["bypassAuthorization"].(bool)
	return kernable.RequestContext{SenderTenant: s("senderTenant"), SenderIdentity: s("senderIdentity"),
		SenderAccount: s("senderAccount"), SenderSession: s("senderSession"), OnBehalfOf: s("onBehalfOf"), BypassAuthorization: bypass}
}

func camel(s string) string {
	var b strings.Builder
	up := false
	for _, c := range s {
		switch {
		case c == '_':
			up = true
		case up:
			b.WriteString(strings.ToUpper(string(c)))
			up = false
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// camelKeys is v with keys in camelCase, as the log and the other kits answer.
func camelKeys(v any) any {
	switch x := v.(type) {
	case obj:
		out := obj{}
		for k, v := range x {
			out[camel(k)] = camelKeys(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = camelKeys(v)
		}
		return out
	}
	return v
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func matches(expected, actual any) bool {
	e, ok := expected.(obj)
	if !ok {
		return equalJSON(expected, actual)
	}
	a, ok := actual.(obj)
	if !ok {
		return false
	}
	for k, v := range e {
		if !matches(v, a[k]) {
			return false
		}
	}
	return true
}

func errorJSON(err error) any {
	var r *kernable.Rejected
	if errors.As(err, &r) {
		return obj{"error": r.Code}
	}
	return obj{"error": err.Error()}
}

// answer is a KitAPI result as the TypeScript kit answers: ok with fields, or { ok: false, reason }.
func answer(err error, fields func() obj) any {
	if err == nil {
		out := obj{"ok": true}
		for k, v := range fields() {
			out[k] = v
		}
		return out
	}
	if code, ok := kit.RefusedCode(err); ok {
		return obj{"ok": false, "reason": code}
	}
	return errorJSON(err)
}

// plain is a result the TypeScript kit returns as it is, throwing on a refusal.
func plain(err error, value func() any) any {
	if err == nil {
		return value()
	}
	if code, ok := kit.RefusedCode(err); ok {
		return obj{"error": code}
	}
	return errorJSON(err)
}

func signedIn(s kit.SignedInSession) obj { return camelKeys(toJSON(s)).(obj) }

type driver struct {
	ctx         context.Context
	kit         *kernable.Client
	vars        obj
	apis        map[string]*kit.KitAPI
	devices     map[string]*authenticator
	providers   map[string]*oidcProvider
	txtMu       sync.Mutex
	txt         map[string]string
	consumed    int
	service     [2]string
	mails       string
	kernableBin string
	store       string
	dir         string
}

func (d *driver) lookup(path string) any {
	parts := strings.Split(path, ".")
	v, ok := d.vars[parts[0]]
	if !ok {
		panic("unknown variable " + parts[0])
	}
	for _, p := range parts[1:] {
		switch x := v.(type) {
		case []any:
			i, err := strconv.Atoi(p)
			if err != nil || i < 0 || i >= len(x) {
				v = nil
			} else {
				v = x[i]
			}
		case obj:
			v = x[p]
		default:
			v = nil
		}
	}
	return v
}

func asInt(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			panic(err)
		}
		return i
	case int64:
		return n
	}
	panic(fmt.Sprintf("not a number: %v", v))
}

func (d *driver) resolve(v any) any {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "$") {
			return d.lookup(x[1:])
		}
		return x
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = d.resolve(e)
		}
		return out
	case obj:
		if parts, ok := x["concat"].([]any); ok {
			var b strings.Builder
			for _, p := range parts {
				switch r := d.resolve(p).(type) {
				case string:
					b.WriteString(r)
				default:
					raw, _ := json.Marshal(r)
					b.Write(raw)
				}
			}
			return b.String()
		}
		if parts, ok := x["merge"].([]any); ok {
			out := obj{}
			for _, p := range parts {
				if o, ok := d.resolve(p).(obj); ok {
					for k, v := range o {
						out[k] = v
					}
				}
			}
			return out
		}
		if t, ok := x["tamper"]; ok {
			s := d.resolve(t).(string)
			switch x["how"] {
			case "lastChar":
				last := "A"
				if strings.HasSuffix(s, "A") {
					last = "B"
				}
				return s[:len(s)-1] + last
			case "secret":
				return s[:strings.LastIndexByte(s, '.')] + ".not-the-secret"
			}
			panic(fmt.Sprintf("unknown tamper %v", x["how"]))
		}
		if secret, ok := x["totp"]; ok {
			at := asInt(d.resolve(x["at"]))
			if plus, ok := x["plus"]; ok {
				at += asInt(plus)
			}
			return kit.TOTPCode(d.resolve(secret).(string), at)
		}
		out := obj{}
		for k, e := range x {
			out[k] = d.resolve(e)
		}
		return out
	}
	return v
}

func (d *driver) api(s obj) *kit.KitAPI {
	name, _ := s["api"].(string)
	if name == "" {
		name = "api"
	}
	a, ok := d.apis[name]
	if !ok {
		panic("no KitAPI " + name)
	}
	return a
}

func (d *driver) claims(s obj, key string) *kit.Claims {
	switch v := d.resolve(s[key]).(type) {
	case string:
		return d.api(s).Check(v)
	case nil:
		return nil
	case obj:
		var c kit.Claims
		raw, _ := json.Marshal(v)
		if json.Unmarshal(raw, &c) != nil {
			return nil
		}
		return &c
	}
	return nil
}

var errNoClaims = obj{"error": "no claims"}

func (d *driver) run(s obj) any {
	ctx := d.ctx
	p := func(k string) any { return d.resolve(s[k]) }
	ps := func(k string) string { v, _ := p(k).(string); return v }
	has := func(k string) bool { _, ok := s[k]; return ok }
	optional := func(k string) *string {
		if !has(k) {
			return nil
		}
		v := ps(k)
		return &v
	}
	api := func() *kit.KitAPI { return d.api(s) }
	switch op := s["op"].(string); op {
	case "uuid":
		return kit.NewID()
	case "value":
		return p("value")
	case "bootstrap":
		var signUp kit.SignUpPolicy
		raw, _ := json.Marshal(p("signUp"))
		if err := json.Unmarshal(raw, &signUp); err != nil {
			panic(err)
		}
		admin := p("admin").(obj)
		booted, err := kit.Bootstrap(ctx, d.kit, signUp, "Platform",
			&kit.Admin{Email: admin["email"].(string), Name: admin["name"].(string), Password: admin["password"].(string)})
		if err != nil {
			panic(err)
		}
		d.service = [2]string{booted.APIIdentity, booted.APIKeyID}
		d.apis["api"] = kit.NewKitAPI(d.kit, kit.NewSigningKeys(), booted.APIIdentity, booted.APIKeyID)
		var first any
		if booted.AdminAccount != "" {
			first = obj{"accountId": booted.AdminAccount, "identityId": booted.AdminIdentity}
		}
		return obj{"api": obj{"identityId": booted.APIIdentity, "keyId": booted.APIKeyID, "key": booted.APIKey}, "admin": first}
	case "kitApi":
		a := kit.NewKitAPI(d.kit, kit.NewSigningKeys(), d.service[0], d.service[1]).RedirectURI(ps("redirectUri")).InsecureIssuers()
		if providers, ok := s["providers"].(obj); ok {
			for name, handle := range providers {
				pr := d.providers[handle.(string)]
				a.Provider(name, kit.OIDCProvider(pr.issuer, pr.clientID, pr.clientSecret))
			}
		}
		a.ResolveTXT(func(_ context.Context, name string) []string {
			d.txtMu.Lock()
			defer d.txtMu.Unlock()
			if v, ok := d.txt[name]; ok {
				return []string{v}
			}
			return nil
		})
		d.apis[s["as"].(string)] = a
		return nil
	case "register":
		password := ps("password")
		id, err := api().Register(ctx, ps("email"), ps("name"), &password)
		return answer(err, func() obj { return obj{"accountId": id} })
	case "mail":
		to := ps("to")
		for range 400 {
			mails := readMails(d.mails)
			for i := len(mails) - 1; i >= 0; i-- {
				if mails[i]["to"] == to {
					if has("old") || i >= d.consumed {
						d.consumed = max(d.consumed, i+1)
						return mails[i]
					}
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		return obj{"error": "no new mail to " + to}
	case "mailCount":
		settle := int64(300)
		if v, ok := s["settleMs"]; ok {
			settle = asInt(v)
		}
		time.Sleep(time.Duration(settle) * time.Millisecond)
		n := len(readMails(d.mails))
		d.consumed = max(d.consumed, n)
		return n
	case "verifyEmail":
		ok, err := api().VerifyEmail(ctx, ps("token"))
		return plain(err, func() any { return ok })
	case "signIn":
		r, err := api().SignIn(ctx, ps("email"), ps("password"), optional("ip"), nil)
		return answer(err, func() obj { return signedIn(r) })
	case "check":
		if c := api().Check(ps("token")); c != nil {
			return toJSON(c)
		}
		return nil
	case "contextFor":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		rc, err := api().ContextFor(ctx, c, ps("tenant"))
		return plain(err, func() any { return contextJSON(rc) })
	case "refresh":
		access, refresh, err := api().Refresh(ctx, ps("token"), nil)
		return answer(err, func() obj { return obj{"accessToken": access, "refreshToken": refresh} })
	case "signOut":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		return plain(api().SignOut(ctx, c), func() any { return nil })
	case "startTotp":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		secret, uri, err := api().StartTotp(ctx, c, ps("issuer"), ps("label"))
		return answer(err, func() obj { return obj{"secret": secret, "uri": uri} })
	case "totpStep":
		return time.Now().Unix() / 30
	case "confirmTotp":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		codes, err := api().ConfirmTotp(ctx, c, ps("code"))
		return answer(err, func() obj { return obj{"recoveryCodes": codes} })
	case "proveSecondFactor":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		t, err := api().ProveSecondFactor(ctx, c, ps("code"))
		return answer(err, func() obj { return obj{"accessToken": t} })
	case "createApiKey":
		key, keyID, err := api().CreateApiKey(ctx, toContext(p("context")), ps("tenant"), ps("identityId"), ps("label"), nil)
		return answer(err, func() obj { return obj{"key": key, "keyId": keyID} })
	case "checkApiKey":
		rc, err := api().CheckApiKey(ctx, ps("key"))
		return plain(err, func() any {
			if rc == nil {
				return nil
			}
			return contextJSON(*rc)
		})
	case "changePassword":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		current := ps("current")
		return answer(api().ChangePassword(ctx, c, &current, ps("next")), func() obj { return obj{} })
	case "requestPasswordReset":
		return plain(api().RequestPasswordReset(ctx, ps("email")), func() any { return nil })
	case "resetPassword":
		return answer(api().ResetPassword(ctx, ps("token"), ps("next")), func() obj { return obj{} })
	case "requestEmailChange":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		return answer(api().RequestEmailChange(ctx, c, ps("email")), func() obj { return obj{} })
	case "confirmEmailChange":
		return answer(api().ConfirmEmailChange(ctx, ps("token")), func() obj { return obj{} })
	case "peekInvitation":
		peeked, err := api().PeekInvitation(ctx, ps("token"))
		return plain(err, func() any {
			if peeked == nil {
				return nil
			}
			return toJSON(peeked)
		})
	case "acceptInvitation":
		var account, tenant string
		var err error
		if has("claims") {
			c := d.claims(s, "claims")
			if c == nil {
				return errNoClaims
			}
			account, tenant, err = api().AcceptInvitation(ctx, ps("token"), c, nil)
		} else {
			account, tenant, err = api().AcceptInvitation(ctx, ps("token"), nil, &kit.NewPerson{Name: ps("name"), Password: ps("password")})
		}
		return answer(err, func() obj { return obj{"accountId": account, "tenant": tenant} })
	case "createInvitationLink":
		var groups []kit.InvitedGroup
		raw, _ := json.Marshal(p("groups"))
		_ = json.Unmarshal(raw, &groups)
		link := kit.NewInvitationLink{Groups: groups, Label: optional("label")}
		if n, ok := p("maxUses").(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				link.MaxUses = &i
			}
		}
		token, linkID, err := api().CreateInvitationLink(ctx, toContext(p("context")), ps("tenant"), link)
		return answer(err, func() obj { return obj{"token": token, "linkId": linkID} })
	case "peekInvitationLink":
		peeked, err := api().PeekInvitationLink(ctx, ps("token"))
		return plain(err, func() any {
			if peeked == nil {
				return nil
			}
			return toJSON(peeked)
		})
	case "joinWithInvitationLink":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		tenant, err := api().JoinWithInvitationLink(ctx, c, ps("token"))
		return answer(err, func() obj { return obj{"tenant": tenant} })
	case "verifySsoDomain":
		return answer(api().VerifySsoDomain(ctx, ps("tenant"), ps("ssoId"), ps("domain")), func() obj { return obj{} })
	case "serviceContext":
		return contextJSON(api().ServiceContext)
	case "authenticator":
		d.devices[s["as"].(string)] = newAuthenticator("localhost", "http://localhost:3000", ps("userId"))
		return nil
	case "passkeyRegistrationOptions":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		options, err := api().PasskeyRegistrationOptions(ctx, c, ps("userName"))
		return plain(err, func() any { return toJSON(options) })
	case "passkeyRegister":
		return toJSON(d.devices[s["device"].(string)].register(ps("challenge")))
	case "passkeyAssert":
		return toJSON(d.devices[s["device"].(string)].assert(ps("challenge"), ps("origin")))
	case "addPasskey":
		c := d.claims(s, "claims")
		if c == nil {
			return errNoClaims
		}
		response, _ := p("response").(obj)
		return answer(api().AddPasskey(ctx, c, response, optional("label")), func() obj { return obj{} })
	case "passkeySignInOptions":
		options, err := api().PasskeySignInOptions(ctx)
		return plain(err, func() any { return toJSON(options) })
	case "signInWithPasskey":
		response, _ := p("response").(obj)
		r, err := api().SignInWithPasskey(ctx, response, nil)
		return answer(err, func() obj { return signedIn(r) })
	case "oidcProvider":
		d.providers[s["as"].(string)] = startOIDCProvider()
		return nil
	case "oidcIssuer":
		pr := d.providers[s["provider"].(string)]
		return obj{"issuer": pr.issuer, "clientId": pr.clientID, "clientSecret": pr.clientSecret}
	case "setTxt":
		d.txtMu.Lock()
		d.txt[ps("name")] = ps("value")
		d.txtMu.Unlock()
		return nil
	case "oidcStart":
		by, _ := p("by").(obj)
		provider, _ := by["provider"].(string)
		email, _ := by["email"].(string)
		var link *kit.Claims
		if has("link") {
			if link = d.claims(s, "link"); link == nil {
				return errNoClaims
			}
		}
		u, err := api().OIDCStart(ctx, provider, email, link)
		return answer(err, func() obj { return obj{"url": u} })
	case "oidcLogin":
		user, _ := p("user").(obj)
		return d.providers[s["provider"].(string)].login(ps("url"), user)
	case "oidcFinish":
		r, err := api().OIDCFinish(ctx, ps("callback"), nil)
		return answer(err, func() obj {
			if r == nil {
				return obj{"linked": true}
			}
			return signedIn(*r)
		})
	case "submit":
		parts := strings.Split(s["command"].(string), ".")
		data := p("data")
		if data == nil {
			data = obj{}
		}
		rc := toContext(p("context"))
		o, err := d.kit.Submit(ctx, kernable.Command{Domain: parts[0], AggregateType: parts[1], AggregateID: ps("aggregateId"),
			Type: parts[2], Data: data, Tenant: ps("tenant"), Context: &rc})
		if err != nil {
			return errorJSON(err)
		}
		if !o.Committed {
			return obj{"committed": false, "code": o.Code, "message": o.Message}
		}
		events := []any{}
		for _, e := range o.Events {
			events = append(events, obj{"type": e.Type, "data": parse(e.Data)})
		}
		return obj{"committed": true, "events": events}
	case "query":
		domain, typ, _ := strings.Cut(s["query"].(string), ".")
		data := p("data")
		if data == nil {
			data = obj{}
		}
		rc := toContext(p("context"))
		raw, err := d.kit.Query(ctx, kernable.Query{Domain: domain, Type: typ, Data: data, Tenant: ps("tenant"), Context: &rc})
		var r *kernable.Rejected
		if errors.As(err, &r) {
			return obj{"ok": false, "code": r.Code, "message": r.Message}
		}
		if err != nil {
			return errorJSON(err)
		}
		return obj{"ok": true, "value": parse(raw)}
	case "processIdentityId":
		return "process-" + ps("process")
	case "processAct":
		changed, err := d.kit.ProcessAct(ctx, ps("process"), toContext(p("context")))
		if err != nil {
			return errorJSON(err)
		}
		return obj{"changed": changed}
	case "waitIdentityKey":
		for range 400 {
			raw, err := d.kit.Query(ctx, kernable.Query{Domain: "identity", Type: "GetIdentity", Data: obj{"identityId": ps("identityId")},
				Tenant: "platform", Context: &kernable.RequestContext{BypassAuthorization: true}})
			if err == nil {
				var identity struct {
					APIKeys []struct {
						KeyID string `json:"keyId"`
					} `json:"apiKeys"`
				}
				if json.Unmarshal(raw, &identity) == nil && len(identity.APIKeys) > 0 && identity.APIKeys[0].KeyID != "" {
					return identity.APIKeys[0].KeyID
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		return obj{"error": "no key for the identity"}
	case "waitCase":
		process, kase, state := ps("process"), ps("case"), ps("state")
		for range 400 {
			out, _ := exec.Command(d.kernableBin, "processes", "cases", d.store, process, "--state", state).Output()
			for _, l := range strings.Split(string(out), "\n") {
				if strings.Contains(l, " "+kase+" ") {
					return obj{"state": state}
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		return obj{"state": "not reached"}
	case "retryCase":
		var stderr bytes.Buffer
		cmd := exec.Command(d.kernableBin, "processes", "retry", env("PARITY_URL"), env("PARITY_KEY"), ps("process"), ps("case"), "--tenant", ps("tenant"))
		cmd.Stderr = &stderr
		if cmd.Run() != nil {
			return obj{"error": stderr.String()}
		}
		return nil
	case "restartHost":
		flag := d.dir + "/restart"
		if err := os.WriteFile(flag, nil, 0o644); err != nil {
			panic(err)
		}
		for range 1200 {
			if _, err := os.Stat(flag); err != nil {
				return nil
			}
			time.Sleep(25 * time.Millisecond)
		}
		return obj{"error": "the host did not restart"}
	default:
		panic("unknown op " + op)
	}
}

func runDriver() {
	raw, err := os.ReadFile(env("PARITY_SCENARIO"))
	if err != nil {
		panic(err)
	}
	scenario := parse(raw).(obj)
	c := client()
	c.Use(append(kit.KitDomains(), shop())...)
	dir := env("PARITY_DIR")
	d := &driver{ctx: context.Background(), kit: c, vars: obj{}, apis: map[string]*kit.KitAPI{}, devices: map[string]*authenticator{},
		providers: map[string]*oidcProvider{}, txt: map[string]string{}, mails: dir + "/mails.jsonl",
		kernableBin: env("PARITY_KERNABLE"), store: env("PARITY_STORE"), dir: dir}
	transcript := []any{}
	for i, step := range scenario["steps"].([]any) {
		s := step.(obj)
		result := toJSON(d.run(s))
		if name, ok := s["as"].(string); ok {
			d.vars[name] = result
		}
		entry := obj{"step": i, "op": s["op"], "result": result}
		if note, ok := s["note"]; ok {
			entry["note"] = note
		}
		if expect, ok := s["expect"]; ok {
			expected := d.resolve(expect)
			if !matches(expected, result) {
				entry["unexpected"] = expected
			}
		}
		transcript = append(transcript, entry)
	}
	out, err := json.MarshalIndent(transcript, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(env("PARITY_OUT"), out, 0o644); err != nil {
		panic(err)
	}
	os.Exit(0)
}

func main() {
	if len(os.Args) < 2 {
		panic("usage: kitparity host|driver")
	}
	switch os.Args[1] {
	case "host":
		host()
	case "driver":
		runDriver()
	default:
		panic("usage: kitparity host|driver")
	}
}
