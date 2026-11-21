# Releasing

One command releases a version. Running it is the decision to release; nothing else publishes:

    scripts/release.sh <version> [--summary "<what this release brings, in a sentence or two>"]

On this machine: the version, every check, the tag. On GitHub, after one approval:
the build, the registries, the release, the downloads, the docs. Last, here and on GitHub: the
release checked as a user gets it. A step that held is noted in `dist/<version>/.done`, so after
a stop the same command goes on where it stopped (`--from <step>` repeats from a step on). Logs:
`dist/<version>/logs/`.

1. **preflight**: on main, nothing uncommitted, up to date with origin, the tag free.
2. **version**: the number in `Cargo.toml`, `kits/rust`, `sdks/rust` (the `=` pins),
   `sdks/typescript/package.json`, `sdks/python/pyproject.toml` and the lock files, committed.
   Before 1.0 the protocol and the store formats stay version 1 and change in place (no
   migrations).
3. **tests**: `make check` (its four parts side by side, `scripts/check.sh`), `make
   check-packages` and `make flaky` (3 rounds), on this machine. A part that passed is noted:
   after a fix, the release run again repeats only the parts that failed (`--from tests` all of
   them). GitHub's CI runs the checks again on Linux.
4. **push**: main and the annotated tag `v<version>`; the summary is the tag's message.
5. **workflow**: the tag starts `.github/workflows/release.yml`; the script prints where to approve
   and waits for it.
   - Without publishing anything: the binaries with `scripts/build-binaries.sh`, both macOS
     targets on a macOS runner, both Linux targets on a Linux runner with `cargo zigbuild`
     (glibc 2.31 and newer: Debian 11, Ubuntu 20.04); `SHA256SUMS`; the notes with
     `scripts/release-notes.py`: the summary, then the commits since the last tag grouped by what
     they changed (kernel, SDKs and kits, documentation, dependencies), those that change only
     tests, scripts or CI left out, then install lines, the protocol's state and the sums.
   - Then it waits for one approval in the `release` environment (Review deployments). After it,
     side by side: the crates (`kernable-core`, `kernable-sdk-macros`, `kernable-sdk`, in that
     order), `@kernable/sdk` on npm and `kernable-sdk` on PyPI through Trusted Publishing (no
     token stored anywhere); the Go module by its tag `sdks/go/v<version>` on the release's
     commit (a Go module in a subdirectory is released by such a tag), fetched once by
     `proxy.golang.org` so `go get` and pkg.go.dev have it; the GitHub release with the notes, the archives, `SHA256SUMS` and
     build provenance for each archive (`gh attestation verify <archive> -R kernable/kernable`);
     the archives to `https://kernable.com/download/<version>/`, checked as served, then
     `download/latest` and `download/install.sh` (served as `https://kernable.com/install.sh`);
     the documentation to `https://kernable.com/docs/`.
   - Last, `scripts/verify-release.sh` on a Linux and a macOS runner.
   A part already done is skipped (a version a registry has, a release that exists), so failed
   jobs can be re-run there; then `--from workflow`.
6. **verify**: `scripts/verify-release.sh <version>` on this machine: `install.sh`,
   the SDKs from npm, PyPI, crates.io and the Go module proxy in fresh projects (imported, their
   kits copied out, the Rust and Go ones built), the GitHub release with its files.

A trial: Actions, Release, Run workflow (on main) builds the binaries and the notes of the
version in `Cargo.toml` as the release would, and publishes nothing.

**Without GitHub's workflow**: `scripts/release.sh <version> --local` does its part from here:
the binaries (`cargo zigbuild` for Linux), the notes, the registries with the tokens in
`~/.config/kernable/{crates,npm,pypi}.token`, the Go module's tag, the GitHub release (without
build provenance), the
downloads and the docs (needs GNU rsync: `brew install rsync`). After a failed workflow:
`--local --from binaries`.

**kernable.com**: the release writes there as the user `release`, which owns only `download/`
and `docs/` in the web root; its key may only run rsync there (`rrsync`). GitHub has it as the
`release` environment's secrets `SITE_SSH_KEY` and `SITE_KNOWN_HOSTS`, this machine as
`~/.config/kernable/release-deploy` (and `.known_hosts`). The server's address is in neither
repository; the key reaches it as `release@kernable.com`.

Each archive and package carries the MIT license (`LICENSE`); each SDK also carries it for its
kit, as `kit/LICENSE`.

Not automated yet: the container image (`IMAGE=1 scripts/build-binaries.sh` writes
`image-<arch>.tar`; the registry is not chosen yet), a Windows build, signing the archives
beyond build provenance and SHA256SUMS.

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
