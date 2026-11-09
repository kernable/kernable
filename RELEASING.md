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
6. **SDKs.**
   - npm: `cd sdks/typescript && npm publish --access public` (`@kernable/sdk`).
   - PyPI: `kernable-sdk`, the wheel and sdist from `sdks/python` (`python -m build`, `twine upload`).
   - crates.io: `kernable-sdk` depends on `kernable-core` and `kernable-sdk-macros`, which go
     first; `publish = false` in `[workspace.package]` has to give way to `publish = true` in
     these three crates only. The kernel, the store and the tools stay off crates.io.
7. **Licenses.** Each archive and package carries the MIT license (`LICENSE`); each SDK also carries it
   for its kit, as `kit/LICENSE`.
8. **Notes.** What changed, what an operator must do (none before 1.0: no migrations), the
   SHA-256 sums.

Open before the first public release: the container registry, a Windows build, signing the
archives (beyond SHA256SUMS over HTTPS), the docs' "not released yet" notes.
