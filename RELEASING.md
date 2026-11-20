# Releasing

One command releases a version, from the version number to a release checked as a user gets
it. Running it is the decision to release; nothing else publishes:

    scripts/release.sh <version>

It runs these steps; each must hold before the next starts. A step that held is noted in
`dist/<version>/.done`, so after a stop the same command goes on where it stopped
(`--from <step>` repeats from a step on). Logs: `dist/<version>/logs/`.

1. **preflight**: on main, nothing uncommitted, up to date with origin, the tag free, both
   servers reachable.
2. **version**: the number in `Cargo.toml`, `kits/rust`, `sdks/rust` (the `=` pins),
   `sdks/typescript/package.json`, `sdks/python/pyproject.toml` and the lock files, committed.
   Before 1.0 the protocol and the store formats stay version 1 and change in place (no
   migrations).
3. **tests**: `make check` here and on the Linux server side by side, `make check-packages`, and
   `make flaky` on Linux (`scripts/check-linux.sh`). GitHub's CI only adds to these.
4. **binaries**: `scripts/build-binaries.sh <version>` (on macOS, with `cargo zigbuild` for
   Linux): `dist/<version>/kernable-<version>-<target>.tar.gz` for aarch64/x86_64 macOS and Linux
   (glibc 2.31 and newer) and `SHA256SUMS`; `IMAGE=1` adds the container images (`Dockerfile`)
   as `image-<arch>.tar`. Windows has no build yet.
5. **notes**: `dist/<version>/NOTES.md`, the release text. Written beforehand, it is taken as it
   is; else it is drafted from the commits since the last tag and the script stops for it to be
   edited. It starts with what the release brings and what an operator must do (nothing before
   1.0), and ends with the SHA-256 sums.
6. **push**: main and the annotated tag `v<version>`.
7. **publish**: the tag starts `.github/workflows/release.yml`, which publishes the crates
   (`kernable-core`, `kernable-sdk-macros`, `kernable-sdk`, in that order), `@kernable/sdk` on
   npm and `kernable-sdk` on PyPI through Trusted Publishing: no token is stored anywhere. It
   waits for approval in the `release` environment; the script prints the link and waits. A
   version a registry already has is skipped, so a failed run can run again. Without GitHub:
   `--tokens` publishes from this machine with the tokens in
   `~/.config/kernable/{crates,npm,pypi}.token`. The kernel, the store and the tools stay off
   crates.io.
8. **downloads**: the archives to `https://kernable.com/download/<version>/`, checked as served,
   then the version into `download/latest`; `scripts/install.sh` as
   `https://kernable.com/install.sh`, which checks every archive against `SHA256SUMS`.
9. **github**: the GitHub release from the tag, with `NOTES.md`, the archives and `SHA256SUMS`.
10. **docs**: the documentation on kernable.com (`make deploy-docs` in the kernable.com
    repository, `../kernable.com` or `SITE_REPO=`).
11. **verify**: `scripts/verify-release.sh <version>`: `install.sh` here and on Linux, the SDKs
    installed from npm, PyPI and crates.io into fresh projects (imported, their kits copied out,
    the Rust one built), the GitHub release with its files.

The servers stay out of this repository: `~/.config/kernable/site.server` (kernable.com) and
`~/.config/kernable/check-linux.server`, each one line `root@<address>`.

Each archive and package carries the MIT license (`LICENSE`); each SDK also carries it for its
kit, as `kit/LICENSE`.

Not automated yet: the container image (`docker load -i image-<arch>.tar`, tag, push as one
multi-arch manifest; the registry is not chosen yet), a Windows build, signing the archives
(beyond SHA256SUMS over HTTPS).

## An example of NOTES.md

The text of 0.1.0, the first public release:

> **kernable 0.1.0, the first public release.** An event sourcing and CQRS kernel: one Rust
> binary that checks, orders, signs and stores commands and events, with your business logic in
> TypeScript, Python or Rust. Read your own writes, a signed and verifiable log, business
> processes, unique keys, deletable personal data, a standby with automatic failover,
> partitions, continuous backups and a web UI. MIT licensed.
>
> Install: `curl -fsSL https://kernable.com/install.sh | sh` · Start here:
> https://kernable.com/docs/tutorial/getting-started · Before 1.0 the API and the formats may
> still change; each release says how.
