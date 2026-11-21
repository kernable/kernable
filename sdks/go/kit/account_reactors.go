package kit

// The account reactors: side effects and follow-up commands.

import (
	"encoding/json"
	"fmt"

	kernable "github.com/kernable/kernable/sdks/go"
)

func sendMail(to *string, purpose Purpose, link, subject, text string) {
	if to != nil && *to != "" {
		CurrentMailer().Send(Mail{To: *to, Subject: subject, Text: text, Purpose: purpose, Token: link})
	}
}

// endAll ends every open session of the account, but keep.
func endAll(c *lc, accountID string, reason SessionEnd, keep *string) error {
	answer, err := c.Client().Query(c, kernable.Query{Domain: "account", Type: "ListSessions",
		Data: ListSessions{AccountID: accountID}, Tenant: PlatformTenant, Context: internal(nil)})
	if err != nil {
		return err
	}
	var sessions []SessionRow
	if err := json.Unmarshal(answer, &sessions); err != nil {
		return err
	}
	for _, s := range sessions {
		if !s.Active || (keep != nil && s.ID == *keep) {
			continue
		}
		r := reason
		_, err := c.Submit(kernable.Command{Domain: "account", AggregateType: "session", AggregateID: s.ID, Type: "EndSession",
			Data: EndSession{Reason: &r}, ID: fmt.Sprintf("end-%s-%s", s.ID, reason), Tenant: PlatformTenant, Context: internal(c)})
		// Ended meanwhile (signed out).
		if err != nil && !isRejected(err, "invalid_state") {
			return err
		}
	}
	return nil
}

func accountReactors(d *kernable.Domain) {
	mails := d.Reactor("account.mail", kernable.Order(OrderReactors))
	kernable.On(mails, "VerificationRequested", func(c *lc, e VerificationRequested) error {
		// Erased already: nothing to send.
		if e.Token != nil {
			link := Token(c.Event.AggregateID, e.TokenID, *e.Token)
			sendMail(e.Email, PurposeVerify, link, "Confirm your email address", "Confirm your email address with this code: "+link)
		}
		return nil
	})
	kernable.On(mails, "PasswordResetRequested", func(c *lc, e PasswordResetRequested) error {
		if e.Token != nil {
			link := Token(c.Event.AggregateID, e.TokenID, *e.Token)
			sendMail(e.Email, PurposeReset, link, "Reset your password", "Set a new password with this code: "+link)
		}
		return nil
	})
	kernable.On(mails, "EmailChangeRequested", func(c *lc, e EmailChangeRequested) error {
		if e.Token != nil {
			link := Token(c.Event.AggregateID, e.TokenID, *e.Token)
			sendMail(e.Email, PurposeEmailChange, link, "Confirm your new email address", "Confirm this address with this code: "+link)
		}
		return nil
	})
	kernable.On(mails, "RegistrationAttempted", func(_ *lc, e RegistrationAttempted) error {
		sendMail(e.Email, PurposeAccountExists, "", "You already have an account",
			"Someone registered with this address, which has an account already. Sign in, or reset your password.")
		return nil
	})

	sessions := d.Reactor("account.sessions", kernable.Order(OrderReactors))
	// Opens the session of every sign-in.
	kernable.On(sessions, "SignedIn", func(c *lc, e SignedIn) error {
		_, err := c.Submit(kernable.Command{Domain: "account", AggregateType: "session", AggregateID: e.SessionID, Type: "OpenSession",
			Data: OpenSession{AccountID: c.Event.AggregateID, Method: e.Method, Factors: e.Factors, RefreshTokenHash: e.RefreshTokenHash,
				IP: e.IP, UserAgent: e.UserAgent, At: e.At},
			ID: "open-session-" + e.SessionID, Tenant: PlatformTenant, Context: internal(c)})
		return err
	})
	kernable.On(sessions, "SecondFactorProved", func(c *lc, e SecondFactorProved) error {
		_, err := c.Submit(kernable.Command{Domain: "account", AggregateType: "session", AggregateID: e.SessionID, Type: "AddSessionFactor",
			Data: AddSessionFactor{Factor: string(e.Method), AccountID: c.Event.AggregateID},
			ID:   fmt.Sprintf("factor-%s-%s", e.SessionID, c.Event.CommandID), Tenant: PlatformTenant, Context: internal(c)})
		return err
	})

	ending := d.Reactor("account.ending", kernable.Order(OrderReactors))
	kernable.On(ending, "AccountSuspended", func(c *lc, _ AccountSuspended) error {
		return endAll(c, c.Event.AggregateID, EndAccountSuspended, nil)
	})
	kernable.On(ending, "AccountDeleted", func(c *lc, _ AccountDeleted) error {
		return endAll(c, c.Event.AggregateID, EndAccountDeleted, nil)
	})
	// A new password signs out everywhere else.
	kernable.On(ending, "PasswordChanged", func(c *lc, e PasswordChanged) error {
		return endAll(c, c.Event.AggregateID, EndPasswordChanged, e.KeepSessionID)
	})
	kernable.On(ending, "PasswordReset", func(c *lc, _ PasswordReset) error {
		return endAll(c, c.Event.AggregateID, EndPasswordChanged, nil)
	})
}
