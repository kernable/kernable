# Mutual TLS with your own PKI

Over TCP, the kernel requires mutual TLS: it presents a certificate, and every process must present one signed by your client CA. `kernable certs new` makes development certificates; in production, use your own CA.

## Certificates

- **The kernel's certificate** must name the host your processes connect to, as a DNS name or an IP address in its subject alternative names, and be valid for server authentication.
- **Client certificates** must be signed by the CA in `--tls-client-ca` and be valid for client authentication. Any certificate of that CA is let in: TLS decides who may connect, their [keys and grants](../concepts/trust) decide what they may do.
- Files are PEM.

## The kernel

```sh
kernable start --root keys/root.pub --listen 0.0.0.0:7233 \
  --tls-cert certs/kernel.crt \
  --tls-key certs/kernel.key \
  --tls-client-ca certs/clients-ca.crt
```

`--tls-cert` is the kernel's certificate (with its chain, if any). The three flags go together, and apply to TCP only; a kernel on a Unix socket refuses them. Each has an environment variable (`KERNABLE_KERNEL_TLS_CERT`, `KERNABLE_KERNEL_TLS_KEY`, `KERNABLE_TLS_CLIENT_CA`), see [kernable start](../reference/configuration).

## Your processes

Give every process the CA that signed the kernel's certificate and its own certificate and key: set `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, which the SDKs, `kernable revoke` and `kernable submit` read, or pass them in the code: `tls: { ca, cert, key }` in TypeScript, `Tls(ca, cert, key)` in Python, `Options::tls` in Rust.

## Renewing certificates

Replace the files and restart: the kernel reads its certificate at start, processes when they connect. Processes reconnect on their own after the kernel's restart.
