// Package example is an example API on net/http over the kit's API part: how a framework wires
// KitAPI in. Carry the routes over to whatever you use; the kit's functions stay.
//
// Requests carry Authorization: Bearer <access token> (or ApiKey <key>) and, inside a tenant,
// X-Tenant: <tenant ID>; the server turns them into the request context the middleware decides
// on. Run starts it all at once, from a main of yours:
//
//	func main() { log.Fatal(example.Run(context.Background())) }   // KERNABLE_URL, KERNABLE_KEY; 127.0.0.1, port 3000 or PORT
package example

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	kernable "github.com/kernable/kernable/sdks/go"
	"github.com/kernable/kernable/sdks/go/kit"
)

type object = map[string]any

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// refusal answers a refusal with its code at status; anything else is a 500.
func refusal(w http.ResponseWriter, err error, status int) {
	if code, ok := kit.RefusedCode(err); ok {
		reply(w, status, object{"error": code})
		return
	}
	reply(w, http.StatusInternalServerError, object{"error": err.Error()})
}

// notOK answers a refusal in the shape the other kits' examples answer: { "ok": false, "reason" }.
func notOK(w http.ResponseWriter, err error, status int) {
	if code, ok := kit.RefusedCode(err); ok {
		reply(w, status, object{"ok": false, "reason": code})
		return
	}
	reply(w, http.StatusInternalServerError, object{"error": err.Error()})
}

func unauthenticated(w http.ResponseWriter) {
	reply(w, http.StatusUnauthorized, object{"error": "unauthenticated"})
}

// body is the request's JSON object, empty if it has none.
func body(r *http.Request) object {
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	var b object
	if d.Decode(&b) != nil || b == nil {
		return object{}
	}
	return b
}

func text(b object, field string) string { s, _ := b[field].(string); return s }

func number(b object, field string) *int64 {
	if n, ok := b[field].(json.Number); ok {
		if i, err := n.Int64(); err == nil {
			return &i
		}
	}
	return nil
}

// data is the request's data, {} if it has none.
func data(b object) any {
	if b["data"] == nil {
		return object{}
	}
	return b["data"]
}

func ip(r *http.Request) *string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return nil
	}
	return &host
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// contextJSON is the request context as JSON: its fields that are set.
func contextJSON(c kernable.RequestContext) object {
	out := object{}
	for _, f := range []struct{ name, value string }{{"senderAccount", c.SenderAccount}, {"senderSession", c.SenderSession},
		{"senderTenant", c.SenderTenant}, {"senderIdentity", c.SenderIdentity}, {"onBehalfOf", c.OnBehalfOf}} {
		if f.value != "" {
			out[f.name] = f.value
		}
	}
	if c.BypassAuthorization {
		out["bypassAuthorization"] = true
	}
	if len(c.Attributes) > 0 {
		out["attributes"] = c.Attributes
	}
	return out
}

// tenantFor is where a request of domain goes: accounts and sessions live in the platform tenant,
// the rest in the caller's.
func tenantFor(domain string, c kernable.RequestContext) string {
	if domain == "account" {
		return kit.PlatformTenant
	}
	return c.SenderTenant
}

type server struct {
	api    *kit.KitAPI
	client *kernable.Client
}

func (s *server) bearer(r *http.Request) *kit.Claims {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return nil
	}
	return s.api.Check(token)
}

// caller is the caller's request context, from the bearer token or API key and the tenant
// header; nil if none.
func (s *server) caller(r *http.Request) *kernable.RequestContext {
	if key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "ApiKey "); ok {
		c, _ := s.api.CheckApiKey(r.Context(), key)
		return c
	}
	claims := s.bearer(r)
	if claims == nil {
		return nil
	}
	// No identity in that tenant: none.
	c, err := s.api.ContextFor(r.Context(), claims, r.Header.Get("X-Tenant"))
	if err != nil {
		return nil
	}
	return &c
}

// Server is the routes; client submits the app's own commands with the caller's context. A query
// given by name carries the aggregate it is about if client knows its domain (Client.Use).
func Server(api *kit.KitAPI, client *kernable.Client) http.Handler {
	s := &server{api: api, client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		password := text(b, "password")
		// The same whether or not the address was taken.
		if _, err := api.Register(r.Context(), text(b, "email"), text(b, "name"), &password); err != nil {
			if _, refused := kit.RefusedCode(err); !refused {
				reply(w, http.StatusInternalServerError, object{"error": err.Error()})
				return
			}
		}
		reply(w, http.StatusAccepted, object{"message": "Check your email"})
	})
	mux.HandleFunc("POST /verify", func(w http.ResponseWriter, r *http.Request) {
		if ok, err := api.VerifyEmail(r.Context(), text(body(r), "token")); ok && err == nil {
			reply(w, http.StatusOK, object{})
			return
		}
		reply(w, http.StatusBadRequest, object{})
	})
	mux.HandleFunc("POST /sign-in", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		signed, err := api.SignIn(r.Context(), text(b, "email"), text(b, "password"), ip(r), optional(r.UserAgent()))
		if err != nil {
			refusal(w, err, http.StatusUnauthorized)
			return
		}
		reply(w, http.StatusOK, object{"accessToken": signed.AccessToken, "refreshToken": signed.RefreshToken, "secondFactor": signed.SecondFactor})
	})
	mux.HandleFunc("POST /refresh", func(w http.ResponseWriter, r *http.Request) {
		access, refresh, err := api.Refresh(r.Context(), text(body(r), "refreshToken"), ip(r))
		if err != nil {
			notOK(w, err, http.StatusUnauthorized)
			return
		}
		reply(w, http.StatusOK, object{"ok": true, "accessToken": access, "refreshToken": refresh})
	})
	mux.HandleFunc("POST /password-reset", func(w http.ResponseWriter, r *http.Request) {
		_ = api.RequestPasswordReset(r.Context(), text(body(r), "email"))
		reply(w, http.StatusAccepted, object{"message": "Check your email"})
	})
	mux.HandleFunc("POST /password-reset/confirm", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		if err := api.ResetPassword(r.Context(), text(b, "token"), text(b, "password")); err != nil {
			refusal(w, err, http.StatusBadRequest)
			return
		}
		reply(w, http.StatusOK, object{})
	})
	mux.HandleFunc("POST /invitations/accept", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		claims := s.bearer(r)
		var person *kit.NewPerson
		if claims == nil {
			person = &kit.NewPerson{Name: text(b, "name"), Password: text(b, "password")}
		}
		account, tenant, err := api.AcceptInvitation(r.Context(), text(b, "token"), claims, person)
		if err != nil {
			notOK(w, err, http.StatusBadRequest)
			return
		}
		reply(w, http.StatusOK, object{"ok": true, "accountId": account, "tenant": tenant})
	})
	mux.HandleFunc("POST /second-factor", func(w http.ResponseWriter, r *http.Request) {
		claims := s.bearer(r)
		if claims == nil {
			unauthenticated(w)
			return
		}
		access, err := api.ProveSecondFactor(r.Context(), claims, text(body(r), "code"))
		if err != nil {
			notOK(w, err, http.StatusUnauthorized)
			return
		}
		reply(w, http.StatusOK, object{"ok": true, "accessToken": access})
	})
	mux.HandleFunc("POST /sign-out", func(w http.ResponseWriter, r *http.Request) {
		if claims := s.bearer(r); claims != nil {
			_ = api.SignOut(r.Context(), claims)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	// Opens a tenant the caller owns (self-service, as the sign-up policy allows).
	mux.HandleFunc("POST /tenants", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		claims := s.bearer(r)
		if claims == nil {
			unauthenticated(w)
			return
		}
		rc, err := api.ContextFor(r.Context(), claims, "")
		if err != nil {
			refusal(w, err, http.StatusForbidden)
			return
		}
		tenant := kit.NewID()
		o, err := client.Submit(r.Context(), kernable.Command{Domain: "tenancy", AggregateType: "tenant", AggregateID: tenant, Type: "CreateTenant",
			Data: object{"slug": b["slug"], "name": b["name"], "owner": object{"accountId": claims.Sub}}, Tenant: tenant, Context: &rc})
		switch {
		case err != nil:
			reply(w, http.StatusInternalServerError, object{"error": err.Error()})
		case o.Committed:
			reply(w, http.StatusCreated, object{"tenant": tenant})
		default:
			reply(w, http.StatusForbidden, object{"error": o.Code})
		}
	})
	// The signed-in caller joins a tenant through an invitation link.
	mux.HandleFunc("POST /invitation-links/join", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		claims := s.bearer(r)
		if claims == nil {
			unauthenticated(w)
			return
		}
		tenant, err := api.JoinWithInvitationLink(r.Context(), claims, text(b, "token"))
		if err != nil {
			notOK(w, err, http.StatusBadRequest)
			return
		}
		reply(w, http.StatusOK, object{"ok": true, "tenant": tenant})
	})
	// The caller's request context in the tenant of X-Tenant.
	mux.HandleFunc("GET /me", func(w http.ResponseWriter, r *http.Request) {
		c := s.caller(r)
		if c == nil {
			unauthenticated(w)
			return
		}
		reply(w, http.StatusOK, contextJSON(*c))
	})
	// A reusable invitation link into groups of the caller's tenant.
	mux.HandleFunc("POST /invitation-links", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		c := s.caller(r)
		if c == nil {
			unauthenticated(w)
			return
		}
		var groups []kit.InvitedGroup
		raw, _ := json.Marshal(b["groups"])
		_ = json.Unmarshal(raw, &groups)
		link := kit.NewInvitationLink{Groups: groups, Label: optional(text(b, "label")), ExpiresAt: number(b, "expiresAt"), MaxUses: number(b, "maxUses")}
		token, linkID, err := api.CreateInvitationLink(r.Context(), *c, c.SenderTenant, link)
		if err != nil {
			refusal(w, err, http.StatusForbidden)
			return
		}
		reply(w, http.StatusCreated, object{"token": token, "linkId": linkID})
	})
	// The app's commands, as the caller; account ones go to the platform tenant, where every
	// account lives.
	mux.HandleFunc("POST /commands/{domain}/{aggregate}/{type}", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		c := s.caller(r)
		if c == nil {
			unauthenticated(w)
			return
		}
		id := text(b, "aggregateId")
		if id == "" {
			id = kit.NewID()
		}
		domain := r.PathValue("domain")
		o, err := client.Submit(r.Context(), kernable.Command{Domain: domain, AggregateType: r.PathValue("aggregate"), AggregateID: id,
			Type: r.PathValue("type"), Data: data(b), Tenant: tenantFor(domain, *c), Context: c})
		switch {
		case err != nil:
			reply(w, http.StatusInternalServerError, object{"error": err.Error()})
		case o.Committed:
			events := []string{}
			for _, e := range o.Events {
				events = append(events, e.Type)
			}
			reply(w, http.StatusOK, object{"events": events})
		default:
			reply(w, http.StatusForbidden, object{"error": o.Code})
		}
	})
	// The app's queries, as the caller.
	mux.HandleFunc("POST /queries/{domain}/{type}", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		c := s.caller(r)
		if c == nil {
			unauthenticated(w)
			return
		}
		domain := r.PathValue("domain")
		answer, err := client.Query(r.Context(), kernable.Query{Domain: domain, Type: r.PathValue("type"), Data: data(b),
			Tenant: tenantFor(domain, *c), Context: c})
		var rejected *kernable.Rejected
		switch {
		case errors.As(err, &rejected) && rejected.Code == "not_found":
			reply(w, http.StatusNotFound, object{"error": rejected.Code})
		case errors.As(err, &rejected):
			reply(w, http.StatusForbidden, object{"error": rejected.Code})
		case err != nil:
			reply(w, http.StatusInternalServerError, object{"error": err.Error()})
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(answer)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, http.StatusNotFound, object{"error": "not_found"})
	})
	return mux
}

// Run runs the kit as one service, the bootstrap (it may run again), and the server on
// 127.0.0.1 at PORT (default 3000), until ctx ends. Loopback only: put it behind your reverse
// proxy (TLS, rate limits) to serve it.
func Run(ctx context.Context) error {
	app, err := newClient()
	if err != nil {
		return err
	}
	running, err := app.App(kernable.AppOptions{Service: "kit", Domains: kit.KitDomains()})
	if err != nil {
		return err
	}
	failed := make(chan error, 1)
	go func() { failed <- running.Run(ctx) }()
	select {
	case <-running.Ready():
	case err := <-failed:
		return err
	}
	// The API's client knows the kit's schemas: vault fields, and what queries asked by name are about.
	client, err := newClient()
	if err != nil {
		return err
	}
	client.Use(kit.KitDomains()...)
	booted, err := kit.Bootstrap(ctx, client, kit.SignUpPolicy{Tenants: kit.TenantSignUpActive, Accounts: kit.AccountSignUpOpen}, "Platform", nil)
	if err != nil {
		return err
	}
	// The access token key: a PEM file in production (KIT_SIGNING_KEY), new on every start otherwise.
	keys := kit.NewSigningKeys()
	if path := os.Getenv("KIT_SIGNING_KEY"); path != "" {
		if keys, err = kit.SigningKeysFrom(path); err != nil {
			return err
		}
	}
	api := kit.NewKitAPI(client, keys, booted.APIIdentity, booted.APIKeyID)
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	srv := &http.Server{Addr: "127.0.0.1:" + port, Handler: Server(api, client)}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	fmt.Fprintf(os.Stderr, "example server on 127.0.0.1:%s\n", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func newClient() (*kernable.Client, error) { return kernable.New(kernable.Options{}) }
