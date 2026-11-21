package kit

// The shared fixtures (kits/fixtures): given these events, when this command, then these events
// (or this rejection), these unique keys and facts, and these requirements; and the
// authorization cases. The TypeScript, Python and Rust kits run the same cases.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
)

// fixturesDir is next to the kit in a copy (kernable-sdk add kit), or in the repository.
func fixturesDir(t *testing.T) string {
	for _, dir := range []string{"fixtures", filepath.Join("..", "..", "..", "kits", "fixtures")} {
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return dir
		}
	}
	t.Skip("no fixtures next to the kit")
	return ""
}

func parseJSON(raw []byte) any {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		panic(err)
	}
	return v
}

func through(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return parseJSON(raw)
}

// fixtureMatches is "" if actual matches expected, else where not; placeholders <string>,
// <number>, <argon2>, <any>.
func fixtureMatches(expected, actual any, path string) string {
	if p, ok := expected.(string); ok && len(p) > 2 && strings.HasPrefix(p, "<") && strings.HasSuffix(p, ">") {
		s, isString := actual.(string)
		_, isNumber := actual.(json.Number)
		ok := map[string]bool{"<any>": true, "<string>": isString && s != "", "<number>": isNumber,
			"<argon2>": isString && strings.HasPrefix(s, "$argon2id$")}
		if known, named := ok[p]; named {
			if !known {
				return fmt.Sprintf("%s: %v is not %s", path, actual, p)
			}
			return ""
		}
	}
	switch e := expected.(type) {
	case []any:
		a, ok := actual.([]any)
		if ok && len(a) == len(e) {
			for i := range e {
				if m := fixtureMatches(e[i], a[i], fmt.Sprintf("%s[%d]", path, i)); m != "" {
					return m
				}
			}
			return ""
		}
	case map[string]any:
		a, ok := actual.(map[string]any)
		if ok {
			var ek, ak []string
			for k := range e {
				ek = append(ek, k)
			}
			for k := range a {
				ak = append(ak, k)
			}
			slices.Sort(ek)
			slices.Sort(ak)
			if !slices.Equal(ek, ak) {
				return fmt.Sprintf("%s: fields %v, expected %v", path, ak, ek)
			}
			for _, k := range ek {
				if m := fixtureMatches(e[k], a[k], path+"."+k); m != "" {
					return m
				}
			}
			return ""
		}
	case json.Number:
		if a, ok := actual.(json.Number); ok {
			x, _ := e.Float64()
			y, _ := a.Float64()
			if x == y {
				return ""
			}
		}
	}
	x, _ := json.Marshal(expected)
	y, _ := json.Marshal(actual)
	if bytes.Equal(x, y) {
		return ""
	}
	return fmt.Sprintf("%s: %s, expected %s", path, y, x)
}

type fixtureRunner func(domain string, c map[string]any) error

func text(v any) string { s, _ := v.(string); return s }

// runner runs a case on aggregate: folds given, handles when, compares then/rejected/index/requires.
func runner[S any](a *kernable.Aggregate[S]) fixtureRunner {
	return func(domain string, c map[string]any) error {
		id, tenant := text(c["aggregateId"]), text(c["tenant"])
		fold := func(events []any, from int) []kernable.FoldEvent {
			var out []kernable.FoldEvent
			for i, x := range events {
				e := x.(map[string]any)
				data := e["data"]
				if data == nil {
					data = map[string]any{}
				}
				out = append(out, kernable.FoldEvent{EventMeta: kernable.EventMeta{Type: text(e["type"]), Version: uint64(from + i + 1),
					Tenant: tenant, Domain: domain, AggregateType: a.Name, AggregateID: id}, Schema: 1, Data: data})
			}
			return out
		}
		given, _ := c["given"].([]any)
		state, err := a.Fold(fold(given, 0))
		if err != nil {
			return fmt.Errorf("given: %w", err)
		}
		now, _ := c["now"].(json.Number).Int64()
		ctx := kernable.NewCommandContext(nil, "fixture-"+text(c["name"]), kernable.RequestContext{}, time.UnixMilli(now))
		ctx.AggregateID, ctx.Tenant = id, tenant
		when := c["when"].(map[string]any)
		data := when["data"]
		if data == nil {
			data = map[string]any{}
		}
		produced, err := a.Handle(ctx, state, text(when["type"]), data)
		if r, ok := err.(*kernable.Rejected); ok {
			if code := text(c["rejected"]); code == r.Code {
				return nil
			}
			return fmt.Errorf("rejected with %s: %s", r.Code, r.Message)
		}
		if err != nil {
			return fmt.Errorf("failed: %w", err)
		}
		if code := text(c["rejected"]); code != "" {
			return fmt.Errorf("expected rejected %s, got %d events", code, len(produced))
		}
		plain := []any{}
		for _, e := range produced {
			plain = append(plain, map[string]any{"type": e.Type, "data": through(e.Data)})
		}
		if m := fixtureMatches(c["then"], plain, "then"); m != "" {
			return fmt.Errorf("%s", m)
		}
		if c["index"] != nil {
			after, err := a.Fold(append(fold(given, 0), fold(plain, len(given))...))
			if err != nil {
				return fmt.Errorf("then: %w", err)
			}
			held := a.Index(after)
			keys := []any{}
			for _, k := range held.Keys {
				scope := "tenant"
				if k.Global {
					scope = "global"
				}
				key := map[string]any{"name": k.Name, "value": k.Value, "scope": scope}
				if k.Subject != "" {
					key["subject"] = k.Subject
				}
				keys = append(keys, key)
			}
			facts := []any{}
			for _, f := range held.Facts {
				facts = append(facts, f)
			}
			if m := fixtureMatches(c["index"], map[string]any{"keys": keys, "facts": facts}, "index"); m != "" {
				return fmt.Errorf("%s", m)
			}
		}
		if c["requires"] != nil {
			declared, err := a.Requirements(state, text(when["type"]), data)
			if err != nil {
				return fmt.Errorf("requires: %w", err)
			}
			plain := []any{}
			for _, r := range declared {
				d := r.Domain
				if d == "" {
					d = domain
				}
				req := map[string]any{"domain": d, "aggregateType": r.AggregateType, "aggregateId": r.AggregateID, "fact": r.Fact}
				if r.Key != "" {
					req["key"], req["value"] = r.Key, r.Value
				}
				if r.Absent {
					req["absent"] = true
				}
				plain = append(plain, req)
			}
			if m := fixtureMatches(c["requires"], through(plain), "requires"); m != "" {
				return fmt.Errorf("%s", m)
			}
		}
		return nil
	}
}

func TestEverySharedCasePasses(t *testing.T) {
	dir := fixturesDir(t)
	tenancy, account, invitation := NewTenancy(), NewAccountDomain(), NewInvitationDomain()
	aggregates := map[string]fixtureRunner{
		"tenancy.tenant":        runner(tenancy.Tenant),
		"account.account":       runner(account.Account),
		"account.session":       runner(account.Session),
		"account.sso":           runner(account.Sso),
		"group.group":           runner(NewGroupDomain().Group),
		"identity.identity":     runner(NewIdentityDomain().Identity),
		"invitation.invitation": runner(invitation.Invitation),
		"invitation.link":       runner(invitation.Link),
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	slices.Sort(files)
	passed := 0
	var failed []string
	for _, file := range files {
		if filepath.Base(file) == "authorization.json" {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		spec := parseJSON(raw).(map[string]any)
		domain := text(spec["domain"])
		for _, x := range spec["cases"].([]any) {
			c := x.(map[string]any)
			name := domain + "." + text(c["aggregate"])
			run, ok := aggregates[name]
			if !ok {
				failed = append(failed, fmt.Sprintf("%s: %s: no aggregate %s", domain, text(c["name"]), name))
				continue
			}
			if err := run(domain, c); err != nil {
				failed = append(failed, fmt.Sprintf("%s: %s: %v", domain, text(c["name"]), err))
			} else {
				passed++
			}
		}
	}
	if len(failed) > 0 {
		t.Fatalf("%d cases failed:\n%s", len(failed), strings.Join(failed, "\n"))
	}
	if passed == 0 {
		t.Fatal("no cases")
	}
	t.Logf("%d cases", passed)
}

func TestEverySharedAuthorizationCaseDecidesTheSame(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(fixturesDir(t), "authorization.json"))
	if err != nil {
		t.Fatal(err)
	}
	spec := parseJSON(raw).(map[string]any)
	for _, x := range spec["relations"].([]any) {
		r := x.(map[string]any)
		field := text(r["field"])
		Relation(text(r["type"]), text(r["name"])).Set(text(r["event"]), func(e RelationEvent) []string { return IDs(e.Data[field]) })
	}
	learnAll := func(k Knowledge, events []any) {
		for _, x := range events {
			e := x.(map[string]any)
			typ := text(e["domain"]) + "." + text(e["type"])
			payload, _ := json.Marshal(e["data"])
			if err := Learn(k, Seen{Type: typ, Tenant: text(e["tenant"]), AggregateID: text(e["aggregateId"]),
				Domain: text(e["domain"]), AggregateType: text(e["aggregateType"])}, payload); err != nil {
				t.Fatalf("%s: %v", typ, err)
			}
		}
	}
	world, _ := spec["world"].([]any)
	var failed []string
	cases := spec["cases"].([]any)
	for _, x := range cases {
		c := x.(map[string]any)
		k := Knowledge{KV: &MemoryKeyValues{}}
		learnAll(k, world)
		extra, _ := c["extra"].([]any)
		learnAll(k, extra)
		ch := c["check"].(map[string]any)
		r, _ := ch["request"].(map[string]any)
		bypass, _ := r["bypassAuthorization"].(bool)
		data, _ := json.Marshal(ch["data"])
		check := kernable.Check{Kind: text(ch["kind"]), Tenant: text(ch["tenant"]), Domain: text(ch["domain"]), Type: text(ch["type"]),
			AggregateType: text(ch["aggregateType"]), AggregateID: text(ch["aggregateId"]), Data: data,
			Request: kernable.RequestContext{SenderTenant: text(r["senderTenant"]), SenderIdentity: text(r["senderIdentity"]),
				SenderAccount: text(r["senderAccount"]), SenderSession: text(r["senderSession"]), OnBehalfOf: text(r["onBehalfOf"]),
				BypassAuthorization: bypass}}
		if target, ok := ch["target"].(map[string]any); ok {
			check.TargetType, check.TargetID = text(target["type"]), text(target["id"])
		}
		outcome := "allow"
		if refusal := Decide(k, check, time.Now().UnixMilli(), nil, false); refusal != nil {
			outcome = refusal.Code
		}
		if outcome != text(c["expect"]) {
			failed = append(failed, fmt.Sprintf("%s: %s, expected %s", text(c["name"]), outcome, text(c["expect"])))
		}
	}
	if len(failed) > 0 {
		t.Fatalf("%d cases failed:\n%s", len(failed), strings.Join(failed, "\n"))
	}
	t.Logf("%d authorization cases", len(cases))
}
