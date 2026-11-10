# Contributing to kernable

Thank you for helping. Bug reports, documentation fixes and pull requests are all welcome.

## Before you start

- **Bugs:** open an issue with the kernable version (`kernable --version`), the SDK and its
  version, what you did, what you expected and what happened. A failing test or a small script
  that reproduces it helps most.
- **Security issues:** never in a public issue. See [SECURITY.md](SECURITY.md).
- **Larger changes** (a new feature, a change to the protocol, the store formats or an SDK's API):
  open an issue first, so we can agree on the design before you write the code. The protocol and
  the three SDKs move together: a change to one is usually a change to all of them.

## Build and test

You need Rust through rustup (`rust-toolchain.toml` pins the version), `protoc`, Node.js 22.18 or newer and Python 3.10 or newer.

```sh
make test     # fmt, clippy and in-process tests (about a minute)
make check    # everything: every storage backend, crash points, the SDKs and the docs
```

Run `make test` before every push, and `make check` before you open a pull request. CI runs both.

## Pull requests

- One topic per pull request, with tests for what it changes. A fix comes with a test that fails
  without it.
- Keep the layout flat: one file per kind and domain, no new folder levels without a reason.
- Code, comments and docs are in English. Comments say why, not what.
- A change users notice updates the documentation in `docs/` in the same pull request. Generated
  reference pages are regenerated with `npm run reference` in `docs/`.
- The commit message says what changed and why, in one short line and, if needed, a paragraph.

## License of contributions

kernable is under the [MIT license](../LICENSE). Before we can merge your first pull request, you
agree to our [Contributor License Agreement](CLA.md) once: before merging, the maintainer asks
you in the pull request to confirm in a comment that you agree to it. You keep the copyright in your contributions; you grant kernable the right to use and
license them, and kernable keeps every contribution it includes available under the project's
open source license.

If you contribute on behalf of your employer, or your employer has rights to what you write,
say so in your pull request: we then need an agreement from your employer as well.
