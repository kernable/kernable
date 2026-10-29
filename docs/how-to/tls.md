# Mutual TLS with your own PKI

Use this page when processes on other machines connect to the kernel. On a loopback address (`127.0.0.1:7233`, the default) or a Unix socket, the kernel speaks plain gRPC. On any other address it requires **mutual TLS**, and refuses to start without it: the kernel presents a certificate, and every process must present one signed by your client CA.

TLS decides who may connect; [keys and grants](../concepts/trust#keys-and-grants) decide what they may do. You need both.

## Certificates

| File | Requirements |
|---|---|
| the kernel's certificate | names the host your processes connect to, as a DNS name or an IP address in its subject alternative names; valid for server authentication; with its chain, if any |
| client certificates | signed by the CA in `--tls-client-ca`; valid for client authentication. Any certificate of that CA is let in |
| the CA for processes | the CA that signed the kernel's certificate |

All files are PEM.

For development, `kernable certs new` writes a CA and the certificates in one go:

```sh
kernable certs new certs api shop-worker read-models
# wrote ca.tls.crt, kernel.tls.crt/.tls.key and client certificates to certs
```

The kernel's certificate is valid for `localhost` and `127.0.0.1` only, and the CA key is not kept: to add a client, issue a new set. In production, use your own CA.

## 1. Start the kernel

```sh
kernable start --root keys/root.pub --listen 0.0.0.0:7233 \
  --tls-cert certs/kernel.crt \
  --tls-key certs/kernel.key \
  --tls-client-ca certs/clients-ca.crt
```

The three flags go together, and apply to TCP only; a kernel on a Unix socket refuses them. Each has an environment variable (`KERNABLE_KERNEL_TLS_CERT`, `KERNABLE_KERNEL_TLS_KEY`, `KERNABLE_TLS_CLIENT_CA`), see [kernable start](../reference/configuration). The start banner shows `Listen ... 0.0.0.0:7233 (mutual TLS)`.

Without them, a non-loopback address fails at start: `0.0.0.0:7233 is reachable from other machines: plain TCP only on a loopback address (127.0.0.1:7233); elsewhere, use mutual TLS (--tls-cert, --tls-key, --tls-client-ca)`.

## 2. Configure your processes

Give every process the CA that signed the kernel's certificate, and its own certificate and key. Either set three environment variables:

```sh
export KERNABLE_URL=https://kernel.internal:7233
export KERNABLE_TLS_CA=certs/kernel-ca.crt
export KERNABLE_TLS_CERT=certs/shop-worker.crt
export KERNABLE_TLS_KEY=certs/shop-worker.key
```

or pass them in code:

| SDK | Option |
|---|---|
| TypeScript | `new Kernable({ tls: { ca, cert, key } })` |
| Python | `Kernable(tls=Tls(ca, cert, key))` |
| Rust | `Options::new("shop-worker.key").tls(Tls { ca, cert, key })` (paths) |

Set all three variables or none: in the SDKs, one or two of them is an error. The CLI commands that talk to a running kernel (`kernable apply`, `kernable revoke`, `kernable submit`) read the same variables, but use TLS only when all three are set; with one or two, they quietly connect without TLS, and the kernel refuses them on a non-loopback address.

## Pitfalls

- **Connect by a name in the certificate.** The host in the URL (`kernel.internal`) is checked against the kernel certificate's subject alternative names. A certificate for `kernel.internal` fails when you connect by IP address, and the reverse.
- **`http://` or `https://` makes no difference.** With TLS set, the SDKs and the CLI connect over TLS either way.
- **The metrics and UI addresses are not covered.** `--metrics-listen` serves plain HTTP: bind it to a private address. `--ui-listen` is loopback only ([Monitor kernable](./monitor)).

## Renewing certificates

Replace the files and restart: the kernel reads its certificate at start, processes when they connect. Processes reconnect on their own after the kernel's restart.
