package kit

// account: one Account per person and a Session per sign-in, in the platform tenant, and a
// tenant's single sign-on (design/kits.md "account"). NewAccountDomain registers the parts.

import kernable "github.com/kernable/kernable/sdks/go"

// AccountDomain is the account domain with its parts.
type AccountDomain struct {
	Domain   *kernable.Domain
	Account  *kernable.Aggregate[Account]
	Session  *kernable.Aggregate[Session]
	Sso      *kernable.Aggregate[Sso]
	Accounts *kernable.Listener
	Ssos     *kernable.Listener
	accounts *accountRows
	ssos     *ssoRows
}

// NewAccountDomain is the account domain, every part registered.
func NewAccountDomain() *AccountDomain {
	d := kernable.NewDomain("account")
	accountVault(d)
	accountCommandVault(d)
	a := &AccountDomain{Domain: d, Account: accountAggregate(d), Session: sessionAggregate(d), Sso: ssoAggregate(d)}
	handleAccount(a.Account, a.Session, a.Sso)
	a.Accounts, a.accounts = accountsReadModel(d)
	a.Ssos, a.ssos = ssosReadModel(d)
	answerAccount(d, a.Accounts, a.accounts, a.Ssos, a.ssos)
	accountReactors(d)
	return a
}
