package kit

// The authorization middleware: one listener at order -30 that learns from events and checks
// every request before the kernel accepts or routes it. Its knowledge lives in its store.

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
)

var (
	storeMu  sync.RWMutex
	storeURL = AuthorizationStore
	// Without a store (a unit test): memory.
	memoryKV = &MemoryKeyValues{}
)

// UseAuthorizationStore says where the middleware keeps its knowledge: a SQLite file, so a
// restart continues where it stopped instead of replaying the log while every request is refused.
// Call it before KitDomains.
func UseAuthorizationStore(url string) {
	storeMu.Lock()
	defer storeMu.Unlock()
	storeURL = url
}

func kvOf(store *kernable.Store) KeyValues {
	if store == nil {
		return memoryKV
	}
	return storeKV{s: store}
}

// Seen is the event the knowledge learns from: its qualified type and where it happened.
type Seen struct {
	Type, Tenant, AggregateID, Domain, AggregateType string
}

// Learned are the event types the knowledge is folded from (besides the relations' events).
var Learned = []string{
	"tenancy.TenantCreated", "tenancy.TenantActivated", "tenancy.TenantSuspended", "tenancy.TenantReactivated",
	"tenancy.OwnerAdded", "tenancy.OwnerRemoved", "tenancy.MfaRequirementChanged", "tenancy.TenantDeleted",
	"tenancy.SignUpPolicyChanged",
	"account.AccountRegistered", "account.EmailVerified", "account.AccountSuspended", "account.AccountReactivated",
	"account.AccountDeleted", "account.TotpConfirmed", "account.TotpDisabled", "account.SessionOpened",
	"account.SessionFactorAdded", "account.SessionEnded", "account.SsoEnforcementChanged", "account.SsoDisabled",
	"account.SsoDeleted",
	"identity.IdentityCreated", "identity.IdentitySuspended", "identity.IdentityReactivated", "identity.MembershipGranted",
	"identity.MembershipSuspended", "identity.MembershipResumed", "identity.MembershipRevoked", "identity.IdentityDeleted",
	"identity.ApiKeyCreated", "identity.ApiKeyRevoked",
	"group.GroupCreated", "group.GroupPermissionsSet", "group.GroupDeleted",
}

func payloadOf[T any](payload json.RawMessage) (T, error) {
	var v T
	err := json.Unmarshal(payload, &v)
	return v, err
}

// Learn folds one event into the knowledge.
func Learn(k Knowledge, e Seen, payload json.RawMessage) error {
	id := e.AggregateID
	ikey := e.Tenant + "/" + id
	tenant := func(f func(*TenantK)) error { return kupdate(k, nsTenants, id, f) }
	accountStatus := func(status string) error {
		a, _ := kget[AccountK](k, nsAccounts, id)
		a.Status = status
		return k.set(nsAccounts, id, a)
	}
	membership := func(membershipID string, active bool) error {
		return kupdate(k, nsIdentities, ikey, func(i *IdentityK) {
			for j := range i.Memberships {
				if i.Memberships[j].MembershipID == membershipID {
					i.Memberships[j].Active = active
				}
			}
		})
	}
	ssoOff := func() error {
		return kupdate(k, nsTenants, e.Tenant, func(t *TenantK) {
			if t.SsoEnforced != nil && *t.SsoEnforced == id {
				t.SsoEnforced = nil
			}
		})
	}
	var err error
	switch e.Type {
	case "tenancy.TenantCreated":
		var d TenantCreated
		if d, err = payloadOf[TenantCreated](payload); err == nil {
			err = k.set(nsTenants, id, TenantK{Status: string(d.Status), Owners: []string{}})
		}
	case "tenancy.TenantActivated":
		err = tenant(func(t *TenantK) { t.Status = "active" })
	case "tenancy.TenantSuspended":
		var d TenantSuspended
		if d, err = payloadOf[TenantSuspended](payload); err == nil {
			err = tenant(func(t *TenantK) { t.Status, t.Suspension = "suspended", ptr(string(d.Reason)) })
		}
	case "tenancy.TenantReactivated":
		err = tenant(func(t *TenantK) { t.Status, t.Suspension = "active", nil })
	case "tenancy.OwnerAdded":
		var d OwnerAdded
		if d, err = payloadOf[OwnerAdded](payload); err == nil {
			err = tenant(func(t *TenantK) {
				if !slices.Contains(t.Owners, d.IdentityID) {
					t.Owners = append(t.Owners, d.IdentityID)
				}
			})
		}
	case "tenancy.OwnerRemoved":
		var d OwnerRemoved
		if d, err = payloadOf[OwnerRemoved](payload); err == nil {
			err = tenant(func(t *TenantK) {
				t.Owners = slices.DeleteFunc(t.Owners, func(o string) bool { return o == d.IdentityID })
			})
		}
	case "tenancy.MfaRequirementChanged":
		var d MfaRequirementChanged
		if d, err = payloadOf[MfaRequirementChanged](payload); err == nil {
			err = tenant(func(t *TenantK) { t.MfaRequired = d.Required })
		}
	case "tenancy.TenantDeleted":
		if err = tenant(func(t *TenantK) { t.Status, t.Owners = "deleted", []string{} }); err == nil {
			for _, ns := range []string{nsIdentities, nsGroups, nsRelations, nsParents} {
				if err = k.deletePrefix(ns, id+"/"); err != nil {
					break
				}
			}
		}
	case "tenancy.SignUpPolicyChanged":
		var d SignUpPolicyChanged
		if d, err = payloadOf[SignUpPolicyChanged](payload); err == nil {
			err = k.setSignUp(SignUpPolicy{Tenants: d.Tenants, Accounts: d.Accounts})
		}
	case "account.AccountRegistered":
		var d AccountRegistered
		if d, err = payloadOf[AccountRegistered](payload); err == nil {
			status := "pending"
			if d.Verified {
				status = "active"
			}
			err = accountStatus(status)
		}
	case "account.EmailVerified":
		err = kupdate(k, nsAccounts, id, func(a *AccountK) {
			if a.Status == "pending" {
				a.Status = "active"
			}
		})
	case "account.AccountSuspended":
		err = accountStatus("suspended")
	case "account.AccountReactivated":
		err = accountStatus("active")
	case "account.AccountDeleted":
		err = accountStatus("deleted")
	case "account.TotpConfirmed":
		err = kupdate(k, nsAccounts, id, func(a *AccountK) { a.Mfa = true })
	case "account.TotpDisabled":
		err = kupdate(k, nsAccounts, id, func(a *AccountK) { a.Mfa = false })
	case "account.SessionOpened":
		var d SessionOpened
		if d, err = payloadOf[SessionOpened](payload); err == nil {
			err = k.set(nsSessions, id, SessionK{AccountID: d.AccountID, Factors: d.Factors, Active: true, Method: d.Method})
		}
	case "account.SessionFactorAdded":
		var d SessionFactorAdded
		if d, err = payloadOf[SessionFactorAdded](payload); err == nil {
			err = kupdate(k, nsSessions, id, func(s *SessionK) { s.Factors = append(s.Factors, d.Factor) })
		}
	case "account.SessionEnded":
		err = kupdate(k, nsSessions, id, func(s *SessionK) { s.Active = false })
	case "account.SsoEnforcementChanged":
		var d SsoEnforcementChanged
		if d, err = payloadOf[SsoEnforcementChanged](payload); err == nil {
			err = kupdate(k, nsTenants, e.Tenant, func(t *TenantK) {
				if d.Enforced {
					t.SsoEnforced = ptr(id)
				} else if t.SsoEnforced != nil && *t.SsoEnforced == id {
					t.SsoEnforced = nil
				}
			})
		}
	case "account.SsoDisabled", "account.SsoDeleted":
		err = ssoOff()
	case "identity.IdentityCreated":
		var d IdentityCreated
		if d, err = payloadOf[IdentityCreated](payload); err == nil {
			err = k.set(nsIdentities, ikey, IdentityK{AccountID: d.AccountID, Active: true, Memberships: []MembershipK{}})
		}
	case "identity.IdentitySuspended":
		err = kupdate(k, nsIdentities, ikey, func(i *IdentityK) { i.Active = false })
	case "identity.IdentityReactivated":
		err = kupdate(k, nsIdentities, ikey, func(i *IdentityK) { i.Active = true })
	case "identity.MembershipGranted":
		var d MembershipGranted
		if d, err = payloadOf[MembershipGranted](payload); err == nil {
			err = kupdate(k, nsIdentities, ikey, func(i *IdentityK) {
				i.Memberships = append(i.Memberships, MembershipK{MembershipID: d.MembershipID, Group: d.Group, Scope: d.Scope, ExpiresAt: d.ExpiresAt, Active: true})
			})
		}
	case "identity.MembershipSuspended":
		var d MembershipSuspended
		if d, err = payloadOf[MembershipSuspended](payload); err == nil {
			err = membership(d.MembershipID, false)
		}
	case "identity.MembershipResumed":
		var d MembershipResumed
		if d, err = payloadOf[MembershipResumed](payload); err == nil {
			err = membership(d.MembershipID, true)
		}
	case "identity.MembershipRevoked":
		var d MembershipRevoked
		if d, err = payloadOf[MembershipRevoked](payload); err == nil {
			err = kupdate(k, nsIdentities, ikey, func(i *IdentityK) {
				i.Memberships = slices.DeleteFunc(i.Memberships, func(m MembershipK) bool { return m.MembershipID == d.MembershipID })
			})
		}
	case "identity.IdentityDeleted":
		err = k.delete(nsIdentities, ikey)
	case "identity.ApiKeyCreated":
		var d ApiKeyCreated
		if d, err = payloadOf[ApiKeyCreated](payload); err == nil {
			err = k.set(nsAPIKeys, d.KeyID, ApiKeyK{Tenant: e.Tenant, IdentityID: id, ExpiresAt: d.ExpiresAt})
		}
	case "identity.ApiKeyRevoked":
		var d ApiKeyRevoked
		if d, err = payloadOf[ApiKeyRevoked](payload); err == nil {
			err = kupdate(k, nsAPIKeys, d.KeyID, func(key *ApiKeyK) { key.Revoked = true })
		}
	// A deleted group's memberships stop counting at once.
	case "group.GroupCreated":
		var d GroupCreated
		if d, err = payloadOf[GroupCreated](payload); err == nil {
			err = k.set(nsGroups, ikey, d.Permissions)
		}
	case "group.GroupPermissionsSet":
		var d GroupPermissionsSet
		if d, err = payloadOf[GroupPermissionsSet](payload); err == nil {
			err = k.set(nsGroups, ikey, d.Permissions)
		}
	case "group.GroupDeleted":
		err = k.delete(nsGroups, ikey)
	}
	if err != nil {
		return err
	}
	// The relations and parents the app declared, from its events.
	aggregate := e.Domain + "." + e.AggregateType
	var data map[string]any
	_ = json.Unmarshal(payload, &data)
	event := RelationEvent{Data: data, AggregateID: id, Tenant: e.Tenant}
	for _, rule := range relationRules() {
		if rule.event == e.Type && rule.typ == aggregate {
			if err := k.relate(e.Tenant, Target{Type: rule.typ, ID: id}, rule.name, rule.ids(event), rule.set); err != nil {
				return err
			}
		}
	}
	for _, rule := range parentRules() {
		if rule.event == e.Type && rule.typ == aggregate {
			if p := rule.parent(event); p != nil {
				if err := k.set(nsParents, e.Tenant+"/"+rule.typ+"/"+id, p); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// LearnedEvents is every event type the middleware learns from: the kit's and the declared relations'.
func LearnedEvents() []string {
	all := append(slices.Clone(Learned), relationEvents()...)
	slices.Sort(all)
	return slices.Compact(all)
}

// Explain says why a request would be allowed or refused: the middleware's own steps. For
// oneself, with one's real session; for another identity of the tenant (needs group.Explain),
// sessions aside.
type Explain struct {
	IdentityID string `json:"identityId"`
	// The request type, qualified: shop.CancelOrder.
	Type string `json:"type"`
	// The aggregate the request is about: a command's, or a query's target. Unqualified: of the
	// type's domain.
	AggregateType *string         `json:"aggregateType,omitempty"`
	AggregateID   *string         `json:"aggregateId,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"`
}

type Explained struct {
	Allowed bool     `json:"allowed"`
	Code    *string  `json:"code"`
	Steps   []string `json:"steps"`
}

// CheckRequest is the middleware's check of a request: a *Rejected with the refusal's code to
// refuse.
func CheckRequest(store *kernable.Store, c kernable.Check) error {
	if refusal := Decide(Knowledge{KV: kvOf(store)}, c, time.Now().UnixMilli(), nil, false); refusal != nil {
		return kernable.Reject(refusal.Code, refusal.Message)
	}
	return nil
}

// authorizationMiddleware is the authorization listener of the group domain, at order -30,
// learning from every event type in LearnedEvents (declare relations before building the kit's
// domains).
func authorizationMiddleware(d *kernable.Domain) *kernable.Listener {
	storeMu.RLock()
	url := storeURL
	storeMu.RUnlock()
	options := []kernable.ListenerOption{kernable.Order(OrderAuthorization)}
	if url != "" {
		options = append(options, kernable.WithStore(url), kernable.StoreSchema("1"))
	}
	l := d.ReadModel("group.authorization", options...)
	for _, typ := range LearnedEvents() {
		kernable.On(l, typ, func(c *lc, payload json.RawMessage) error {
			return Learn(Knowledge{KV: kvOf(c.Store)}, Seen{Type: typ, Tenant: c.Event.Tenant, AggregateID: c.Event.AggregateID,
				Domain: c.Event.Domain, AggregateType: c.Event.AggregateType}, payload)
		})
	}
	l.Middleware(kernable.MiddlewareOptions{Types: []string{"*"}, Order: OrderAuthorization}, func(c *kernable.MiddlewareContext, check kernable.Check) error {
		return CheckRequest(c.Store, check)
	})
	d.Query("Explain").Target("identity.identity", "identityId")
	kernable.Answer(l, "Explain", func(c *qc, q Explain) (Explained, error) {
		domain, name, ok := strings.Cut(q.Type, ".")
		if !ok || domain == "" || name == "" {
			return Explained{}, rejected("A qualified type: domain.Type", "invalid_type")
		}
		asker := c.Request
		oneself := asker.SenderIdentity == q.IdentityID && asker.SenderTenant == c.Tenant
		request := asker
		if !oneself {
			request = kernable.RequestContext{SenderTenant: c.Tenant, SenderIdentity: q.IdentityID}
		}
		check := kernable.Check{Kind: "query", Tenant: c.Tenant, Domain: domain, Type: name, Data: q.Data, Request: request}
		if len(check.Data) == 0 || string(check.Data) == "null" {
			check.Data = json.RawMessage("{}")
		}
		if q.AggregateID != nil {
			check.Kind = "command"
			check.AggregateID = *q.AggregateID
		}
		if q.AggregateType != nil {
			check.AggregateType = *q.AggregateType
		}
		if q.AggregateType != nil && q.AggregateID != nil && *q.AggregateType != "" && *q.AggregateID != "" {
			t := *q.AggregateType
			if !strings.Contains(t, ".") {
				t = domain + "." + t
			}
			check.TargetType, check.TargetID = t, *q.AggregateID
		}
		var steps []string
		refusal := Decide(Knowledge{KV: kvOf(c.Store)}, check, time.Now().UnixMilli(), &steps, !oneself)
		out := Explained{Allowed: refusal == nil, Steps: steps}
		if refusal != nil {
			out.Code = ptr(refusal.Code)
		}
		if out.Steps == nil {
			out.Steps = []string{}
		}
		return out, nil
	})
	return l
}
