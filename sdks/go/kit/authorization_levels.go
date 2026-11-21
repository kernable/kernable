package kit

// Who may send which of the kit's types at all: the levels, kept as data. Types not listed here
// need a group permission (or an owner). Your own types need nothing here: give groups
// permissions for them.
//
// Levels: Public (anyone; the command carries its proof), Authenticated (a session), Internal
// (the kit's reactors), Owners, Platform, Group (a group permission, or an owner). Itself: also
// allowed when the target is the sender itself (its identity, account or session).

type Level int

const (
	LevelPublic Level = iota
	LevelAuthenticated
	LevelInternal
	LevelOwners
	LevelPlatform
	LevelGroup
)

type Rule struct {
	Level Level
	// Also allowed for the sender itself.
	Itself bool
}

func r(l Level) Rule  { return Rule{Level: l} }
func me(l Level) Rule { return Rule{Level: l, Itself: true} }

// Rules are the levels of the kit's types.
var Rules = map[string]Rule{
	"tenancy.CreateTenant":                r(LevelAuthenticated), // self-service; the platform's groups otherwise
	"tenancy.ActivateTenant":              r(LevelPlatform),
	"tenancy.SuspendTenant":               r(LevelPlatform), // owners too, with owner_requested: see the policy
	"tenancy.ReactivateTenant":            r(LevelPlatform), // owners too, after owner_requested
	"tenancy.DeleteTenant":                r(LevelPlatform),
	"tenancy.ChangeSignUpPolicy":          r(LevelPlatform),
	"tenancy.AddOwner":                    r(LevelOwners),
	"tenancy.RemoveOwner":                 r(LevelOwners),
	"tenancy.RequireMfa":                  r(LevelOwners),
	"tenancy.ListTenants":                 r(LevelPlatform),
	"account.RegisterAccount":             r(LevelPublic), // verified only from the API: see the policy
	"account.VerifyEmail":                 r(LevelPublic),
	"account.SignIn":                      r(LevelPublic),
	"account.RefreshSession":              r(LevelPublic),
	"account.ProveSecondFactor":           r(LevelPublic),
	"account.ResetPassword":               r(LevelPublic),
	"account.ConfirmEmailChange":          r(LevelPublic),
	"account.OpenSession":                 r(LevelInternal),
	"account.AddSessionFactor":            r(LevelInternal),
	"account.SuspendAccount":              r(LevelPlatform), // the person too, with owner_requested: see the policy
	"account.ReactivateAccount":           r(LevelPlatform),
	"account.DeleteAccount":               r(LevelPlatform),
	"account.EndSession":                  me(LevelGroup),
	"account.ChangePassword":              me(LevelGroup),
	"account.RequestEmailChange":          me(LevelGroup),
	"account.StartTotp":                   me(LevelGroup),
	"account.ConfirmTotp":                 me(LevelGroup),
	"account.DisableTotp":                 me(LevelGroup),
	"account.RegenerateRecoveryCodes":     me(LevelGroup),
	"account.AddPasskey":                  me(LevelGroup),
	"account.RemovePasskey":               me(LevelGroup),
	"account.UnlinkProvider":              me(LevelGroup),
	"account.CreateSso":                   r(LevelOwners),
	"account.UpdateSso":                   r(LevelOwners),
	"account.AddSsoDomain":                r(LevelOwners),
	"account.RemoveSsoDomain":             r(LevelOwners),
	"account.EnforceSso":                  r(LevelOwners),
	"account.DisableSso":                  r(LevelOwners),
	"account.EnableSso":                   r(LevelOwners),
	"account.DeleteSso":                   r(LevelOwners),
	"account.GetSso":                      r(LevelOwners),
	"account.GetAccount":                  me(LevelGroup),
	"account.ListSessions":                me(LevelGroup),
	"identity.CreateIdentity":             r(LevelInternal),
	"identity.CreateProcessIdentity":      r(LevelInternal),
	"identity.UpdateProfile":              me(LevelGroup),
	"identity.GetIdentity":                me(LevelGroup),
	"identity.DeleteIdentity":             me(LevelGroup),
	"identity.ListMyIdentities":           me(LevelAuthenticated),
	"group.Explain":                       me(LevelGroup),
	"invitation.DeclineInvitation":        r(LevelPublic),
	"invitation.RecordInvitationMailSent": r(LevelInternal),
	"invitation.EraseInvitation":          r(LevelPlatform),
}

// Escalation are the requests for which the sender must hold every permission of the group
// concerned.
var Escalation = []string{
	"identity.GrantMembership",
	"invitation.CreateInvitation",
	"invitation.CreateInvitationLink",
	"group.CreateGroup",
	"group.SetPermissions",
}

// RuleOf is the rule of a type: a group permission (or an owner) if it is not listed.
func RuleOf(t string) Rule {
	if rule, ok := Rules[t]; ok {
		return rule
	}
	return r(LevelGroup)
}
