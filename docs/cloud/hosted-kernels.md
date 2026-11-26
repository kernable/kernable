# Hosted kernels

If you'd rather not run a kernel yourself, kernable runs it for you: a **hosted kernel** on
kernable's servers in the EU, which your services reach by URL. Everything else stays as with
your own kernels: your root key stays with you, and what any service may do is decided by the
grants you sign.

::: warning A hosted kernel's data is on kernable's servers in plaintext
Everywhere else, kernable Cloud only holds sealed copies it cannot read. A hosted kernel is the
exception: its events, vault values and files are on kernable's servers, processed under the
[Data Processing Agreement](/cloud/security). Keep a copy at your own site if you need your data while
it is unavailable.
:::

## Before you start

- **Your team may run hosted kernels.** In Preview Access, kernable enables them per team (up to
  five): write to [hello@kernable.com](mailto:hello@kernable.com). The project's page says how many
  your team runs.
- **The project has its root key** ([Link your kernels](/cloud/link-kernels#_2-the-project-s-root-key)).
- **A client CA.** Only services with a certificate your CA signed may connect. If you have none,
  `kernable certs new` makes one, with a certificate for each service:

  ```sh
  kernable certs new certs worker api
  # certs/ca.tls.crt: the CA; certs/worker.tls.crt and .tls.key, certs/api.tls.crt and .tls.key
  ```

  It keeps no CA key: for another service later, make a new set and give the hosted kernel the new
  CA. TLS only decides who may connect; your grants decide what they may do.

## Turn it on

On the project's page in the console, under **Hosted kernel**: paste your CA certificate
(`certs/ca.tls.crt`) and turn it on (admins and owners). Within a minute it runs at
`https://<team>-<project>.kernels.kernable.cloud`, and the page shows it running and ready.

## Connect your services

Each service connects as to any kernel over mutual TLS, with its own key and grant:

```sh
export KERNABLE_URL=https://acme-shop.kernels.kernable.cloud:443
export KERNABLE_TLS_CA=kernel-ca.crt            # from the project's page
export KERNABLE_TLS_CERT=certs/worker.tls.crt   # signed by your client CA
export KERNABLE_TLS_KEY=certs/worker.tls.key
export KERNABLE_KEY=keys/worker.key             # granted by your root key, as always
```

Grant keys as for your own kernels (`kernable grant root.key worker.pub 'submit=shop.*' …`); the
hosted kernel trusts your root and nothing else.

## Your root key changes

When you replace the project's root key, the hosted kernel takes the new one beside the old one
until you finish the replacement, as your linked partitions do; **Replace now** ends the old one
at once there too ([Replacing it](/cloud/link-kernels#replacing-it)).

## Limits

Each hosted kernel has 1 vCPU and 1 GB of memory. What it stores counts towards the storage your
team's monthly fee includes, and what it sends to your services towards the GB sent, both shown
as the project's partition `hosted` in its usage ([Usage and prices](/cloud/usage)).

## Turn it off

**Turn off** on the project's page stops it at once: your services can no longer reach it. Its data
is kept 30 days for a final export (write to support), then deleted. When your team's deletion is
asked, its hosted kernels stop at once with everything kept; restoring the team starts them again.

## Not there yet

- **A copy at your own site** (a reader following the hosted kernel through the relay) is built and
  tested, but not offered in the console yet.
- **Backups of hosted kernels** are being built.
