# Getting started

kernable Cloud is by invitation. This page takes you from the invitation's mail to your first
project.

## 1. Create your account

Open the link in your invitation's mail. It shows the address you were invited with; choose your
name and a password of 12 characters at least. The link works once and expires after the days
the mail names.

Your account comes with a **personal team**, owned by you. If a team invited you instead, you
join that team as well.

## 2. Add a second factor

An owner of a team acts only with a second factor. Set one up before anything else:

- an authenticator app (TOTP), in the console under **Account**, or
- a passkey, in the console under **Account**.

From then on, signing in asks for it. If something needs it while your session has not proved
it yet, the console asks for the code right there.

## 3. Sign in from your terminal

The `kernable` CLI talks to kernable Cloud at `https://api.kernable.cloud`:

```sh
kernable cloud login
```

It shows a code and a page. Open the page in any browser, sign in (with your second factor), and
approve the code. The CLI keeps its own session in your system's keychain.

```sh
kernable cloud whoami
```

## 4. Before your team's first project

A team needs three things before its first project:

```sh
kernable cloud teams accept-terms <team>     # the terms and the Preview Access notice
kernable cloud teams verify-payment <team>   # billing: prints a payment page to open
```

and a verified email, which your invitation already proved. On the payment page (Stripe), the
team's owner subscribes: €149 a month, excluding VAT, with your VAT ID if you have one
([prices](/cloud/usage)). In the console, the same is **Set up billing** on the team's page.
`kernable cloud whoami` lists what is still missing for each team.

## 5. Your first project

```sh
kernable cloud projects create shop
```

A project is your team's record that your kernels link to. Set its root key on its page (the
content of your `root.pub`), then [link your kernels](/cloud/link-kernels).
