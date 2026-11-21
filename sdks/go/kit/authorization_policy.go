package kit

// What the authorization middleware knows (folded from events) and how it decides
// (design/kits.md "The authorization middleware"). Pure: no kernel needed, so it is tested alone.

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	kernable "github.com/kernable/kernable/sdks/go"
)

// inactive is the refusal code for a tenant that is not active: tenant_not_active while it is
// pending (not yet activated), tenant_suspended otherwise.
func inactive(status string) string {
	if status == "pending" {
		return "tenant_not_active"
	}
	return "tenant_suspended"
}

// Refusal is why a request is refused.
type Refusal struct {
	// unauthenticated, forbidden, tenant_suspended, tenant_not_active, mfa_required,
	// sso_required, sign_up_closed.
	Code    string
	Message string
}

// KeyValues is where the knowledge lives: the listener's store, or memory (tests). A value that
// cannot be read counts as unknown, so the middleware refuses rather than allows.
type KeyValues interface {
	Get(ns, key string) (json.RawMessage, bool)
	Set(ns, key string, value json.RawMessage) error
	Remove(ns, key string) error
	Keys(ns string) []string
}

// storeKV is a listener's store as KeyValues, under a prefix of its own, so the kit's namespaces
// never meet the app's ("" for the middleware's own store).
type storeKV struct {
	s      *kernable.Store
	prefix string
}

func (s storeKV) Get(ns, key string) (json.RawMessage, bool) {
	var v json.RawMessage
	found, err := s.s.Get(s.prefix+ns, key, &v)
	return v, found && err == nil
}

func (s storeKV) Set(ns, key string, value json.RawMessage) error {
	return s.s.Set(s.prefix+ns, key, value)
}

func (s storeKV) Remove(ns, key string) error { return s.s.Remove(s.prefix+ns, key) }

func (s storeKV) Keys(ns string) []string {
	all, err := s.s.All(s.prefix + ns)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(all))
	for _, e := range all {
		out = append(out, e.Key)
	}
	return out
}

// MemoryKeyValues are key-values in memory, the same as a store holds them (as JSON).
type MemoryKeyValues struct {
	mu sync.Mutex
	m  map[string]map[string]json.RawMessage
}

func (m *MemoryKeyValues) Get(ns, key string) (json.RawMessage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.m[ns][key]
	return v, ok
}

func (m *MemoryKeyValues) Set(ns, key string, value json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = map[string]map[string]json.RawMessage{}
	}
	if m.m[ns] == nil {
		m.m[ns] = map[string]json.RawMessage{}
	}
	m.m[ns][key] = slices.Clone(value)
	return nil
}

func (m *MemoryKeyValues) Remove(ns, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m[ns], key)
	return nil
}

func (m *MemoryKeyValues) Keys(ns string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sortedRowKeys(m.m[ns])
}

// --- What it knows (design/kits.md, "What it knows") ------------------------------------------

type TenantK struct {
	Status      string   `json:"status"`
	Owners      []string `json:"owners"`
	MfaRequired bool     `json:"mfa_required"`
	Suspension  *string  `json:"suspension"`
	SsoEnforced *string  `json:"sso_enforced"`
}

type AccountK struct {
	Status string `json:"status"`
	// The account turned a second factor on: every session needs it.
	Mfa bool `json:"mfa"`
}

type SessionK struct {
	AccountID string   `json:"account_id"`
	Factors   []string `json:"factors"`
	Active    bool     `json:"active"`
	Method    string   `json:"method"`
}

type MembershipK struct {
	MembershipID string `json:"membership_id"`
	Group        string `json:"group"`
	Scope        *Scope `json:"scope"`
	ExpiresAt    *int64 `json:"expires_at"`
	Active       bool   `json:"active"`
}

type IdentityK struct {
	AccountID   *string       `json:"account_id"`
	Active      bool          `json:"active"`
	Memberships []MembershipK `json:"memberships"`
}

type ApiKeyK struct {
	Tenant     string `json:"tenant"`
	IdentityID string `json:"identity_id"`
	ExpiresAt  *int64 `json:"expires_at"`
	Revoked    bool   `json:"revoked"`
}

const (
	nsTenants  = "tenants"
	nsAccounts = "accounts"
	nsSessions = "sessions"
	// tenant/identityId
	nsIdentities = "identities"
	// keyId
	nsAPIKeys = "apiKeys"
	// tenant/groupId → permissions
	nsGroups = "groups"
	// tenant/type/id → relation → identity IDs
	nsRelations = "relations"
	// tenant/type/id → the parent
	nsParents  = "parents"
	nsSettings = "settings"
)

// Knowledge is the knowledge, read and written through KV. Values are copies: write back with
// set or update.
type Knowledge struct{ KV KeyValues }

func kget[T any](k Knowledge, ns, key string) (T, bool) {
	var v T
	raw, ok := k.KV.Get(ns, key)
	if !ok || json.Unmarshal(raw, &v) != nil {
		var zero T
		return zero, false
	}
	return v, true
}

func (k Knowledge) set(ns, key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return k.KV.Set(ns, key, b)
}

// kupdate changes the value under key, if there is one.
func kupdate[T any](k Knowledge, ns, key string, change func(*T)) error {
	v, ok := kget[T](k, ns, key)
	if !ok {
		return nil
	}
	change(&v)
	return k.set(ns, key, v)
}

func (k Knowledge) delete(ns, key string) error { return k.KV.Remove(ns, key) }

// deletePrefix removes every key starting with prefix (a deleted tenant's).
func (k Knowledge) deletePrefix(ns, prefix string) error {
	for _, key := range k.KV.Keys(ns) {
		if strings.HasPrefix(key, prefix) {
			if err := k.KV.Remove(ns, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (k Knowledge) signUp() SignUpPolicy {
	p, ok := kget[SignUpPolicy](k, nsSettings, "signUp")
	if !ok {
		return defaultSignUp()
	}
	return p
}

func (k Knowledge) setSignUp(p SignUpPolicy) error { return k.set(nsSettings, "signUp", p) }

func (k Knowledge) identity(tenant, identityID string) (IdentityK, bool) {
	if identityID == "" {
		return IdentityK{}, false
	}
	return kget[IdentityK](k, nsIdentities, tenant+"/"+identityID)
}

func targetKey(tenant string, t Target) string { return tenant + "/" + t.Type + "/" + t.ID }

func (k Knowledge) holds(tenant string, target Target, relation, identityID string) bool {
	rels, _ := kget[map[string][]string](k, nsRelations, targetKey(tenant, target))
	return slices.Contains(rels[relation], identityID)
}

func (k Knowledge) relate(tenant string, target Target, relation string, ids []string, on bool) error {
	key := targetKey(tenant, target)
	rels, ok := kget[map[string][]string](k, nsRelations, key)
	if !ok || rels == nil {
		rels = map[string][]string{}
	}
	current := rels[relation]
	if current == nil {
		current = []string{}
	}
	for _, id := range ids {
		if on && !slices.Contains(current, id) {
			current = append(current, id)
		} else if !on {
			current = slices.DeleteFunc(current, func(c string) bool { return c == id })
		}
	}
	slices.Sort(current)
	rels[relation] = current
	return k.set(nsRelations, key, rels)
}

func (k Knowledge) parent(tenant string, target Target) (Target, bool) {
	return kget[Target](k, nsParents, targetKey(tenant, target))
}

// Covers is whether pattern covers name: *, domain.* or the qualified type; name may be domain.*
// itself.
func Covers(pattern, name string) bool {
	if pattern == "*" || pattern == name {
		return true
	}
	domain, ok := strings.CutSuffix(pattern, ".*")
	if !ok || name == "*" {
		return false
	}
	first, _, _ := strings.Cut(name, ".")
	return first == domain
}

// TargetOf is the aggregate the request is about: a command's, or the one a query declares.
func TargetOf(check kernable.Check) (Target, bool) {
	if check.TargetType != "" && check.TargetID != "" {
		return Target{Type: check.TargetType, ID: check.TargetID}, true
	}
	if check.Kind != "command" || check.AggregateType == "" || check.AggregateID == "" {
		return Target{}, false
	}
	return Target{Type: check.Domain + "." + check.AggregateType, ID: check.AggregateID}, true
}

// isSelf is whether the target is the sender itself: its identity, account or session (built-in self).
func isSelf(k Knowledge, check kernable.Check, r kernable.RequestContext, target *Target) bool {
	if target == nil {
		return false
	}
	switch target.Type {
	case "identity.identity":
		return target.ID == r.SenderIdentity && check.Tenant == r.SenderTenant
	case "account.account":
		return target.ID == r.SenderAccount
	case "account.session":
		if target.ID == r.SenderSession {
			return true
		}
		s, ok := kget[SessionK](k, nsSessions, target.ID)
		return ok && s.AccountID == r.SenderAccount
	}
	return false
}

// sender is who sends a request.
type sender struct {
	accountID *string
	factors   []string
	method    string
	apiKey    bool
}

func refuse(code, message string) *Refusal { return &Refusal{Code: code, Message: message} }

// authenticate: an active session of an active account, or a valid API key (step 3).
func authenticate(k Knowledge, r kernable.RequestContext, now int64) (sender, *Refusal) {
	sid := r.SenderSession
	if sid == "" {
		return sender{}, refuse("unauthenticated", "Sign in first")
	}
	if session, ok := kget[SessionK](k, nsSessions, sid); ok {
		if !session.Active {
			return sender{}, refuse("unauthenticated", "The session has ended")
		}
		if session.AccountID != r.SenderAccount {
			return sender{}, refuse("unauthenticated", "The session is another account's")
		}
		if a, ok := kget[AccountK](k, nsAccounts, session.AccountID); !ok || a.Status != "active" {
			return sender{}, refuse("unauthenticated", "The account is not active")
		}
		return sender{accountID: ptr(session.AccountID), factors: session.Factors, method: session.Method}, nil
	}
	if key, ok := kget[ApiKeyK](k, nsAPIKeys, sid); ok {
		if key.Revoked || (key.ExpiresAt != nil && now > *key.ExpiresAt) {
			return sender{}, refuse("unauthenticated", "The API key is revoked or expired")
		}
		if key.IdentityID != r.SenderIdentity || key.Tenant != r.SenderTenant {
			return sender{}, refuse("unauthenticated", "The API key is another identity's")
		}
		return sender{factors: []string{"api_key"}, apiKey: true}, nil
	}
	return sender{}, refuse("unauthenticated", "Unknown session")
}

type held struct {
	p Permission
	m MembershipK
}

// permissionsOf is the permissions an identity holds through its memberships, at now.
func permissionsOf(k Knowledge, tenant, identityID string, now int64) []held {
	identity, ok := k.identity(tenant, identityID)
	if !ok || !identity.Active {
		return nil
	}
	var out []held
	for _, m := range identity.Memberships {
		if !m.Active || (m.ExpiresAt != nil && now >= *m.ExpiresAt) {
			continue
		}
		ps, _ := kget[[]Permission](k, nsGroups, tenant+"/"+m.Group)
		for _, p := range ps {
			out = append(out, held{p, m})
		}
	}
	return out
}

func matches(k Knowledge, tenant, identityID string, p Permission, m MembershipK, check kernable.Check, typ string, target *Target) bool {
	if !Covers(p.Pattern, typ) {
		return false
	}
	if m.Scope != nil {
		at := func(t Target) bool { return m.Scope.Type == t.Type && m.Scope.ID == t.ID }
		ok := target != nil && at(*target)
		if !ok && target != nil {
			if parent, found := k.parent(tenant, *target); found {
				ok = at(parent)
			}
		}
		if !ok {
			return false
		}
	}
	if p.When == nil {
		return true
	}
	// A relation needs a target.
	if target == nil {
		return false
	}
	when := *p.When
	if when == "self" {
		return isSelf(k, check, check.Request, target)
	}
	if k.holds(check.Tenant, *target, when, identityID) {
		return true
	}
	if p.ViaParent != nil && *p.ViaParent {
		if parent, ok := k.parent(check.Tenant, *target); ok {
			return k.holds(check.Tenant, parent, when, identityID)
		}
	}
	return false
}

// concerned is the patterns concerned by a request that hands out permissions (no escalation).
func concerned(k Knowledge, typ, tenant string, data map[string]any) []string {
	ofGroup := func(g any) []string {
		id, ok := g.(string)
		if !ok {
			return nil
		}
		ps, _ := kget[[]Permission](k, nsGroups, tenant+"/"+id)
		var out []string
		for _, p := range ps {
			out = append(out, p.Pattern)
		}
		return out
	}
	switch typ {
	case "identity.GrantMembership":
		return ofGroup(data["group"])
	case "invitation.CreateInvitation", "invitation.CreateInvitationLink":
		var out []string
		groups, _ := data["groups"].([]any)
		for _, g := range groups {
			gm, _ := g.(map[string]any)
			out = append(out, ofGroup(gm["group"])...)
		}
		return out
	}
	var out []string
	permissions, _ := data["permissions"].([]any)
	for _, p := range permissions {
		pm, _ := p.(map[string]any)
		pattern, ok := pm["pattern"].(string)
		if !ok {
			pattern = "null"
		}
		out = append(out, pattern)
	}
	return out
}

func checkData(check kernable.Check) map[string]any {
	var data map[string]any
	if json.Unmarshal(check.Data, &data) != nil || data == nil {
		return map[string]any{}
	}
	return data
}

// Decide decides on a request (design/kits.md, steps 1–9): nil allows it. now is the
// middleware's clock (ms). With trace, each deciding step is noted (group.Explain);
// assumeSession explains what another identity may do, its sessions aside.
func Decide(k Knowledge, check kernable.Check, now int64, trace *[]string, assumeSession bool) *Refusal {
	typ := check.Domain + "." + check.Type
	r := check.Request
	rule := RuleOf(typ)
	data := checkData(check)
	say := func(line string) {
		if trace != nil {
			*trace = append(*trace, line)
		}
	}
	allow := func(why string) *Refusal {
		say("allowed: " + why)
		return nil
	}
	no := func(code, message string) *Refusal {
		say(fmt.Sprintf("refused (%s): %s", code, message))
		return refuse(code, message)
	}

	// 1. Privileged keys (the kernel checked the grant): the kit's reactors and the bootstrap.
	if r.BypassAuthorization {
		return allow("a privileged key (bypass_authorization)")
	}
	if rule.Level == LevelInternal {
		return no("forbidden", typ+" is sent by the kit only")
	}

	// 2. Public: the command carries its proof, the aggregate checks it.
	registering := typ == "account.RegisterAccount"
	if rule.Level == LevelPublic && !(registering && data["verified"] == true) {
		if registering && k.signUp().Accounts != AccountSignUpOpen {
			return no("sign_up_closed", "Registration is by invitation only")
		}
		return allow(typ + " is public; the command carries its proof")
	}

	// 3. Who is asking.
	var s sender
	if assumeSession {
		who, _ := k.identity(r.SenderTenant, r.SenderIdentity)
		say("sessions not checked (explaining for another identity)")
		s = sender{apiKey: who.AccountID == nil, accountID: who.AccountID, factors: []string{"password", "assumed"}}
	} else {
		var refusal *Refusal
		if s, refusal = authenticate(k, r, now); refusal != nil {
			return no(refusal.Code, refusal.Message)
		}
	}
	if !assumeSession {
		if s.apiKey {
			say("signed in with the API key " + r.SenderSession)
		} else {
			say(fmt.Sprintf("signed in as account %s (session %s, %s)", deref(s.accountID), r.SenderSession, strings.Join(s.factors, " + ")))
		}
	}

	// An account with a second factor on: nothing but signing out until the session proved it.
	var account *AccountK
	if s.accountID != nil {
		if a, ok := kget[AccountK](k, nsAccounts, *s.accountID); ok {
			account = &a
		}
	}
	two := len(s.factors) >= 2 || slices.Contains(s.factors, "passkey")
	if account != nil && account.Mfa && !two && typ != "account.EndSession" {
		return no("mfa_required", "Prove the second factor first")
	}
	var target *Target
	if t, ok := TargetOf(check); ok {
		target = &t
	}
	// Oneself: the account and its sessions at once; an identity only after the tenant's checks.
	itself := rule.Itself && isSelf(k, check, r, target)
	if itself && target != nil && target.Type != "identity.identity" {
		// Oneself in another tenant than the platform's (the API checks a person against a tenant
		// they join, before they hold an identity there): that tenant's checks too.
		if check.Domain == "account" && check.Tenant != PlatformTenant {
			t, ok := kget[TenantK](k, nsTenants, check.Tenant)
			switch {
			case !ok, t.Status == "deleted":
				return no("forbidden", "No such tenant")
			case t.Status != "active":
				return no(inactive(t.Status), "The tenant is "+t.Status)
			case t.MfaRequired && !two:
				return no("mfa_required", "This tenant requires a second factor")
			case t.SsoEnforced != nil && !assumeSession && s.method != "sso:"+*t.SsoEnforced:
				return no("sso_required", "Sign in through the tenant's single sign-on")
			}
		}
		return allow(fmt.Sprintf("oneself (%s)", target.Type))
	}
	if typ == "account.SuspendAccount" && data["reason"] == "owner_requested" && isSelf(k, check, r, target) {
		return allow("one's own account, for oneself")
	}

	// 4. Authenticated: a session is enough.
	if rule.Level == LevelAuthenticated {
		if typ == "tenancy.CreateTenant" {
			owner, _ := data["owner"].(map[string]any)
			accountID, isString := owner["accountId"].(string)
			own := isString && s.accountID != nil && accountID == *s.accountID
			status, ok := data["status"].(string)
			if !ok {
				status = "active"
			}
			wanted := TenantSignUpOff
			switch status {
			case "pending":
				wanted = TenantSignUpPending
			case "active":
				wanted = TenantSignUpActive
			}
			policy := k.signUp().Tenants
			if own && policy != TenantSignUpOff && wanted == policy {
				return allow("self-service sign-up of a tenant")
			}
		} else {
			return allow(typ + " needs a session only")
		}
	}

	// 5. The tenant, and the sender's tenant.
	platform := r.SenderTenant == PlatformTenant
	if r.SenderTenant == "" || (!platform && r.SenderTenant != check.Tenant) {
		return no("forbidden", "Not a member of this tenant")
	}
	tenant, tenantFound := kget[TenantK](k, nsTenants, check.Tenant)
	if !platform {
		switch {
		case !tenantFound, tenant.Status == "deleted":
			return no("forbidden", "No such tenant")
		case tenant.Status != "active" && typ != "tenancy.ReactivateTenant":
			return no(inactive(tenant.Status), "The tenant is "+tenant.Status)
		}
	}

	// 6. The identity, and the tenant's MFA requirement.
	if r.OnBehalfOf != "" {
		return no("forbidden", "Acting on behalf of others is not available yet")
	}
	identityID := r.SenderIdentity
	identity, ok := k.identity(r.SenderTenant, identityID)
	if !ok || !identity.Active {
		return no("forbidden", "No active identity in this tenant")
	}
	if !s.apiKey && !eqPtr(identity.AccountID, s.accountID) {
		return no("forbidden", "The identity is another account's")
	}
	home, homeFound := kget[TenantK](k, nsTenants, r.SenderTenant)
	if ((tenantFound && tenant.MfaRequired) || (homeFound && home.MfaRequired)) && !s.apiKey && !two {
		return no("mfa_required", "This tenant requires a second factor")
	}

	// 7. Owners hold every permission in their tenant, except the platform's; platform owners hold all.
	owner := homeFound && slices.Contains(home.Owners, identityID)
	if tenantFound && tenant.SsoEnforced != nil && !platform && !owner && !s.apiKey && !assumeSession &&
		s.method != "sso:"+*tenant.SsoEnforced {
		return no("sso_required", "Sign in through the tenant's single sign-on")
	}
	if itself {
		return allow("oneself (identity.identity)")
	}
	if rule.Level == LevelPlatform && !platform {
		var suspension string
		if tenantFound && tenant.Suspension != nil {
			suspension = *tenant.Suspension
		}
		own := owner && ((typ == "tenancy.SuspendTenant" && data["reason"] == "owner_requested") ||
			(typ == "tenancy.ReactivateTenant" && suspension == "owner_requested"))
		if own {
			return allow("an owner, for their own suspension")
		}
		return no("forbidden", typ+" is for the platform")
	}
	if owner {
		if platform {
			return allow("an owner of the platform")
		}
		return allow("an owner of the tenant (every permission)")
	}
	if typ == "invitation.CreateInvitation" && data["owner"] == true && !platform {
		return no("forbidden", "Owner invitations are the owners'")
	}
	status := ""
	if tenantFound {
		status = tenant.Status
	}
	if !platform && status != "active" {
		return no(inactive(status), "The tenant is "+status)
	}
	if rule.Level == LevelOwners {
		return no("forbidden", typ+" is for the tenant's owners")
	}

	// 8. Memberships: pattern, scope, relation, expiry.
	holding := permissionsOf(k, r.SenderTenant, identityID, now)
	i := slices.IndexFunc(holding, func(h held) bool { return matches(k, r.SenderTenant, identityID, h.p, h.m, check, typ, target) })
	if i < 0 {
		return no("forbidden", "No permission for "+typ)
	}
	p, m := holding[i].p, holding[i].m

	// 9. No escalation: only permissions held without scope and relation count.
	if slices.Contains(Escalation, typ) {
		var plain []string
		for _, h := range holding {
			if h.p.When == nil && h.m.Scope == nil {
				plain = append(plain, h.p.Pattern)
			}
		}
		for _, x := range concerned(k, typ, check.Tenant, data) {
			if !slices.ContainsFunc(plain, func(h string) bool { return Covers(h, x) }) {
				return no("forbidden", "Granting "+x+" needs holding it")
			}
		}
	}
	when := ""
	if p.When != nil {
		when = " when " + *p.When
		if p.ViaParent != nil && *p.ViaParent {
			when += " (via parent)"
		}
	}
	scope := ""
	if m.Scope != nil {
		scope = fmt.Sprintf(", scoped to %s %s", m.Scope.Type, m.Scope.ID)
	}
	until := ""
	if m.ExpiresAt != nil {
		until = fmt.Sprintf(", until %d", *m.ExpiresAt)
	}
	of := ""
	if platform {
		of = " (platform)"
	}
	return allow(fmt.Sprintf("membership %s of group %s%s: %s%s%s%s", m.MembershipID, m.Group, of, p.Pattern, when, scope, until))
}
