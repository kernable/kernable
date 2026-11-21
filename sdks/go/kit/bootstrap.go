package kit

// The bootstrap: run once, with a key granted privileged for bypass_authorization. It creates
// the platform tenant, the sign-up policy, and the API's service identity with its key.

import (
	"context"
	"encoding/hex"
	"fmt"

	kernable "github.com/kernable/kernable/sdks/go"
)

// APIPermissions is what the API may ask for itself: lookups before anyone is signed in.
var APIPermissions = []string{
	"account.FindAccountByEmail",
	"account.GetAccount",
	"account.FindAccountByLink",
	"account.RegisterAccount",
	"account.RequestVerification",
	"account.RequestPasswordReset",
	"account.NoteRegistrationAttempt",
	"account.SignInWithPasskey",
	"account.ProvePasskey",
	"account.SignInWithProvider",
	"account.LinkProvider",
	"account.VerifySsoDomain",
	"account.FindSsoByDomain",
	"account.GetSsoSecret",
	"account.SetCache",
	"account.GetCache",
	"account.DeleteCache",
	"identity.FindIdentityByAccount",
	"identity.FindApiKey",
	"tenancy.GetTenantBySlug",
	"tenancy.GetSignUpPolicy",
	"invitation.AcceptInvitation",
	"invitation.PeekInvitation",
	"invitation.JoinWithInvitationLink",
	"invitation.PeekInvitationLink",
}

// Admin is the first platform administrator: an owner of the platform tenant.
type Admin struct{ Email, Name, Password string }

// Bootstrapped is what the bootstrap made.
type Bootstrapped struct {
	// The API's identity, its key ID and key: give them to NewKitAPI, keep the key secret.
	APIIdentity string
	APIKeyID    string
	APIKey      string
	// The administrator's account and identity, if asked for; else "".
	AdminAccount, AdminIdentity string
}

var bootstrapAggregates = map[string][2]string{
	"CreateTenant": {"tenancy", "tenant"}, "ChangeSignUpPolicy": {"tenancy", "tenant"}, "AddOwner": {"tenancy", "tenant"},
	"CreateServiceIdentity": {"identity", "identity"}, "GrantMembership": {"identity", "identity"},
	"CreateApiKey": {"identity", "identity"}, "CreateIdentity": {"identity", "identity"},
	"CreateGroup": {"group", "group"}, "RegisterAccount": {"account", "account"},
}

// ok submits a bootstrap command; one that exists already counts as done.
func ok(ctx context.Context, client *kernable.Client, command any, aggregateID, id string) error {
	t := commandType(command)
	at := bootstrapAggregates[t]
	if id == "" {
		id = "bootstrap-" + t + "-" + aggregateID
	}
	o, err := client.Submit(ctx, kernable.Command{Domain: at[0], AggregateType: at[1], AggregateID: aggregateID, Type: t,
		Data: command, ID: id, Tenant: PlatformTenant, Context: &kernable.RequestContext{BypassAuthorization: true}})
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if !o.Committed && o.Code != "exists" {
		return fmt.Errorf("bootstrap: %s: %s", o.Code, o.Message)
	}
	return nil
}

// Bootstrap sets up the platform: idempotent, so it may run again. admin nil: no administrator.
func Bootstrap(ctx context.Context, client *kernable.Client, signUp SignUpPolicy, platformName string, admin *Admin) (Bootstrapped, error) {
	tenant := PlatformTenant
	empty := ""
	steps := []struct {
		command     any
		aggregateID string
	}{
		{CreateTenant{Slug: "platform", Name: platformName, Owner: Owner{Email: &empty}}, tenant},
		{ChangeSignUpPolicy{Tenants: signUp.Tenants, Accounts: signUp.Accounts}, tenant},
		{CreateServiceIdentity{DisplayName: "API"}, "api"},
	}
	for _, s := range steps {
		if err := ok(ctx, client, s.command, s.aggregateID, ""); err != nil {
			return Bootstrapped{}, err
		}
	}
	identity := "api"
	permissions := make([]Permission, len(APIPermissions))
	for i, p := range APIPermissions {
		permissions[i] = Permission{Pattern: p}
	}
	description := "What the API asks for itself"
	if err := ok(ctx, client, CreateGroup{Name: "api", Description: &description, Permissions: permissions}, "api", ""); err != nil {
		return Bootstrapped{}, err
	}
	if err := ok(ctx, client, GrantMembership{MembershipID: "api", Group: "api"}, identity, ""); err != nil {
		return Bootstrapped{}, err
	}
	keyID := hex.EncodeToString(RandomBytes(8))
	secret := RandomSecret(32)
	label := "API"
	key := CreateApiKey{KeyID: keyID, Secret: secret, Label: &label}
	if err := ok(ctx, client, key, identity, hex.EncodeToString(RandomBytes(8))); err != nil {
		return Bootstrapped{}, err
	}
	b := Bootstrapped{APIIdentity: identity, APIKeyID: keyID, APIKey: APIKey(keyID, secret)}
	if admin != nil {
		// Fixed IDs, so a second run finds them: the account (verified), its platform identity, owner.
		account, identityID := "platform-admin", "platform-admin"
		yes, password, name := true, admin.Password, admin.Name
		if err := ok(ctx, client, RegisterAccount{Email: admin.Email, Name: admin.Name, Password: &password, Verified: &yes}, account, ""); err != nil {
			return Bootstrapped{}, err
		}
		if err := ok(ctx, client, CreateIdentity{AccountID: account, DisplayName: &name}, identityID, ""); err != nil {
			return Bootstrapped{}, err
		}
		if err := ok(ctx, client, AddOwner{IdentityID: identityID}, tenant, "bootstrap-admin-owner"); err != nil {
			return Bootstrapped{}, err
		}
		b.AdminAccount, b.AdminIdentity = account, identityID
	}
	return b, nil
}

// KitDomains is every domain of the kit, built anew, ready for an app:
// client.App(kernable.AppOptions{Domains: kit.KitDomains()}).
func KitDomains() []*kernable.Domain {
	return []*kernable.Domain{
		NewTenancy().Domain,
		NewAccountDomain().Domain,
		NewGroupDomain().Domain,
		NewIdentityDomain().Domain,
		NewInvitationDomain().Domain,
	}
}
