"""Sending mail: a small provider interface; the mail reactors call it."""

import sys
from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass
class Mail:
    to: str
    subject: str
    text: str
    #: What the mail is for, and the token it carries: tests and logs read these.
    # "notice": a notice from the application, without a token.
    purpose: Literal["verify", "reset", "email_change", "invitation", "account_exists", "notice"]
    token: str


class Mailer(Protocol):
    def send(self, mail: Mail) -> None: ...


class ConsoleMailer:
    """Prints mails instead of sending them: for development."""

    def send(self, mail: Mail) -> None:
        print(f"mail to {mail.to}: {mail.subject}\n{mail.text}", file=sys.stderr)


class MemoryMailer:
    """Keeps mails in memory: for tests."""

    def __init__(self) -> None:
        self.sent: list[Mail] = []

    def send(self, mail: Mail) -> None:
        self.sent.append(mail)

    def last(self, to: str) -> Mail | None:
        return next((m for m in reversed(self.sent) if m.to == to), None)


_mailer: Mailer = ConsoleMailer()


def use_mailer(mailer: Mailer) -> None:
    """Sets the mailer the kit's reactors send with (an SMTP one in production)."""
    global _mailer
    _mailer = mailer


def current_mailer() -> Mailer:
    return _mailer
