package kit

import (
	"fmt"
	"os"
	"sync"
)

// Purpose is what a mail is for.
type Purpose string

const (
	PurposeVerify        Purpose = "verify"
	PurposeReset         Purpose = "reset"
	PurposeEmailChange   Purpose = "email_change"
	PurposeInvitation    Purpose = "invitation"
	PurposeAccountExists Purpose = "account_exists"
	// PurposeNotice is a notice to a person from the application: no token.
	PurposeNotice Purpose = "notice"
)

// Mail is a mail the kit's reactors send.
type Mail struct {
	To      string
	Subject string
	Text    string
	// What the mail is for, and the token it carries: tests and logs read these.
	Purpose Purpose
	Token   string
}

// Mailer sends mail: a small provider interface; the mail reactors call it.
type Mailer interface {
	Send(Mail)
}

// MailerFunc is a function as a Mailer: kit.UseMailer(kit.MailerFunc(func(m kit.Mail) { … })).
type MailerFunc func(Mail)

func (f MailerFunc) Send(m Mail) { f(m) }

// ConsoleMailer prints mails instead of sending them: for development.
type ConsoleMailer struct{}

func (ConsoleMailer) Send(m Mail) {
	fmt.Fprintf(os.Stderr, "mail to %s: %s\n%s\n", m.To, m.Subject, m.Text)
}

// MemoryMailer keeps mails in memory: for tests.
type MemoryMailer struct {
	mu   sync.Mutex
	Sent []Mail
}

func (m *MemoryMailer) Send(mail Mail) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Sent = append(m.Sent, mail)
}

// Last is the newest mail to an address, if any.
func (m *MemoryMailer) Last(to string) (Mail, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.Sent) - 1; i >= 0; i-- {
		if m.Sent[i].To == to {
			return m.Sent[i], true
		}
	}
	return Mail{}, false
}

var mailerMu sync.RWMutex
var mailer Mailer

// UseMailer sets the mailer the kit's reactors send with (an SMTP one in production).
func UseMailer(m Mailer) {
	mailerMu.Lock()
	defer mailerMu.Unlock()
	mailer = m
}

// CurrentMailer is the mailer the kit sends with: a ConsoleMailer unless UseMailer set another.
func CurrentMailer() Mailer {
	mailerMu.RLock()
	defer mailerMu.RUnlock()
	if mailer == nil {
		return ConsoleMailer{}
	}
	return mailer
}
