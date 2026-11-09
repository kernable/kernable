# Security policy

## Reporting a vulnerability

Please do not report security issues in public issues, discussions or pull requests.

Report them privately, either:

- through GitHub: **Security → Report a vulnerability** on this repository, or
- by email to **security@kernable.com**.

Say what is affected (the kernel, the store, an SDK, a kit, the install script), the version, and
how to reproduce it. We confirm receipt within 3 working days and keep you informed until it is
fixed. We credit reporters in the release notes, unless you prefer not to be named.

## Supported versions

Before 1.0, only the newest release gets security fixes.

## Scope

In scope: everything in this repository, including the install script and the published packages
(`@kernable/sdk`, `kernable-sdk` on PyPI and crates.io). The threat model the kernel is built for
(signed requests, grants, the vault, middleware) is described in the documentation; a way around
any of it is a vulnerability.
