// Sending mail: a small provider interface; the mail reactors call it.

export interface Mail {
  to: string;
  subject: string;
  text: string;
  /** What the mail is for, and the token it carries: tests and logs read these. */
  /** `notice`: a notice from the application, without a token. */
  purpose: "verify" | "reset" | "email_change" | "invitation" | "account_exists" | "notice";
  token: string;
}

export interface Mailer {
  send(mail: Mail): Promise<void>;
}

/** Prints mails instead of sending them: for development. */
export class ConsoleMailer implements Mailer {
  async send(mail: Mail): Promise<void> {
    process.stderr.write(`mail to ${mail.to}: ${mail.subject}\n${mail.text}\n`);
  }
}

/** Keeps mails in memory: for tests. */
export class MemoryMailer implements Mailer {
  readonly sent: Mail[] = [];
  async send(mail: Mail): Promise<void> {
    this.sent.push(mail);
  }
  /** The newest mail to `to`, if any. */
  last(to: string): Mail | undefined {
    return this.sent.findLast((m) => m.to === to);
  }
}

let mailer: Mailer = new ConsoleMailer();

/** Sets the mailer the kit's reactors send with (an SMTP one in production). */
export function useMailer(m: Mailer): void {
  mailer = m;
}

export function currentMailer(): Mailer {
  return mailer;
}
