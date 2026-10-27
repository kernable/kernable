# Mutual TLS with your own PKI

Over TCP, the kernel requires mutual TLS: it presents a certificate, and every process must present one signed by your client CA. `kernable certs new` makes development certificates; in production, use your own CA.

## Certificates

- **The kernel's certificate** must name the host your processes connect to, as a DNS name or an IP address in its subject alternative names, and be valid for server authentication.
- **Client certificates** must be signed by the CA in `client_ca` and be valid for client authentication. Any certificate of that CA is let in: TLS decides who may connect, the [keys and allowlists](../concepts/trust) decide what they may do.
- Files are PEM.

## The kernel

```toml
listen = "0.0.0.0:7233"

[tls]
cert = "certs/kernel.crt"        # the kernel's certificate (with its chain, if any)
key = "certs/kernel.key"
client_ca = "certs/clients-ca.crt"
```

`[tls]` applies to TCP only; a kernel on a Unix socket refuses it.

## Your processes

Give every process the CA that signed the kernel's certificate and its own certificate and key: `tls: { ca, cert, key }` in TypeScript, `Tls(ca, cert, key)` in Python. The [tutorial](../tutorial/shop/8-operate) reads them from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`.

## Renewing certificates

Replace the files and restart: the kernel reads its certificate at start, processes when they connect. Processes reconnect on their own after the kernel's restart.
