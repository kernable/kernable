# Releasing

Nothing here publishes on its own: each step that leaves the machine is run by hand, after the
one before it held.

1. **Version.** `[workspace.package] version` in `Cargo.toml`, `version` in
   `sdks/typescript/package.json` and `sdks/python/pyproject.toml`: the same number. Before 1.0
   the protocol and the store formats stay version 1 and change in place (no migrations).
2. **Checks.** `make check` and `make check-packages` green on the commit to release.
3. **Binaries.** `scripts/release.sh <version>` (on macOS, with `cargo zigbuild` for Linux):
   `dist/<version>/kernable-<version>-<target>.tar.gz` for aarch64/x86_64 macOS and Linux (glibc
   2.31 and newer) and `SHA256SUMS`; `IMAGE=1` adds the container images (`Dockerfile`) as
   `image-<arch>.tar`. Windows has no build yet.
4. **Downloads.** Copy `dist/<version>/` to `https://kernable.com/download/<version>/`, then write
   the version into `https://kernable.com/download/latest`. Serve `scripts/install.sh` as
   `https://kernable.com/install.sh`; it checks every archive against `SHA256SUMS`. Try it on a
   clean machine of each kind before announcing.
5. **Container image.** `docker load -i image-<arch>.tar`, tag, push to the registry (not chosen
   yet), as one multi-arch manifest.
6. **SDKs.** Push the tag `v<version>`: `.github/workflows/release.yml` publishes the crates
   (`kernable-core`, `kernable-sdk-macros`, `kernable-sdk`, in that order), `@kernable/sdk` and
   `kernable-sdk` on PyPI through Trusted Publishing, after approval in the `release`
   environment. A version a registry already has is skipped. The kernel, the store and the
   tools stay off crates.io. By hand (the first release of a package, before its registry
   trusts the workflow):
   - crates.io: `(cd crates/kernable-core && ./pack_proto.sh) && (cd sdks/rust && ./pack_kit.sh)`,
     then `cargo publish --allow-dirty -p kernable-core -p kernable-sdk-macros -p kernable-sdk`
     with a token from `cargo login`.
   - npm: `cd sdks/typescript && npm publish` (logged in with `npm login`).
7. **Licenses.** Each archive and package carries the MIT license (`LICENSE`); each SDK also carries it
   for its kit, as `kit/LICENSE`.
8. **Notes.** What changed, what an operator must do (none before 1.0: no migrations), the
   SHA-256 sums.

Open for later releases: the container registry, a Windows build, signing the archives (beyond
SHA256SUMS over HTTPS). The "not released yet" notes went with 0.1.0.

## The release on GitHub

Create the release from the tag `v<version>` and let GitHub write the notes from the commits
(Generate release notes). Attach the archives and `SHA256SUMS` from `dist/<version>/`. For 0.1.0,
above the generated notes:

> **kernable 0.1.0, the first public release.** An event sourcing and CQRS kernel: one Rust
> binary that checks, orders, signs and stores commands and events, with your business logic in
> TypeScript, Python or Rust. Read your own writes, a signed and verifiable log, business
> processes, unique keys, deletable personal data, a standby with automatic failover,
> partitions, continuous backups and a web UI. MIT licensed.
>
> Install: `curl -fsSL https://kernable.com/install.sh | sh` · Start here:
> https://kernable.com/docs/tutorial/getting-started · Before 1.0 the API and the formats may
> still change; each release says how.
