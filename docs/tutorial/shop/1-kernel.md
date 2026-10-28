# 1. Run a kernel

kernable splits an application in two. The **kernel** stores commands and events, signs and checks every message, runs commands one at a time per aggregate and delivers events in order. **Your processes** hold the business logic: they decide what a command means and what to do with an event. They connect to the kernel over gRPC, in any language.

## Install

Install the kernel as in [Getting started](../getting-started#_1-install-the-kernel):

::: code-group

```sh [macOS, Linux]
curl -fsSL https://kernable.com/install.sh | sh
```

```powershell [Windows]
irm https://kernable.com/install.ps1 | iex
```

:::

## Start the kernel

```sh
mkdir -p ~/shop && cd ~/shop
```

<<< @/examples/shop/dev.sh#start{sh}

It listens on `http://127.0.0.1:7233`, for this machine only, and keeps the event log in `kernable.db` in the current directory. `--dev` creates the keys the shop uses, among them `app.key`, which may do everything here, and turns on the vault for personal data (part 7). Part 8 shows how keys and grants work in production. Leave the kernel running and continue in a second terminal.

## Set up the project

Make the shop a project in the language you write it in, with the SDK:

::: code-group
```sh [TypeScript]
cd ~/shop
npm init -y >/dev/null && npm pkg set type=module
npm install @kernable/sdk
```
```sh [Python]
cd ~/shop
python3 -m venv .venv && .venv/bin/pip install -q kernable
```
```sh [Rust]
cd ~/shop
cargo init -q --lib --name shop
cargo add -q kernable serde_json scrypt
cargo add -q serde --features derive
cargo add -q tokio --features macros,rt-multi-thread
```
:::

In Rust, `domain.rs` (part 2) lives in `src/` and `src/lib.rs` shares it; every process and script is a binary in `src/bin/` (run with `cargo run -q --bin worker`):

<<< @/examples/shop/rs/src/lib.rs

## Connect

Every process of the shop connects with a client and no options:

::: code-group
```ts [TypeScript]
const kernable = new Kernable();
```
```python [Python]
k = Kernable()
```
```rust [Rust]
let kernable = Kernable::new(Options::default())?;
```
:::

The SDK takes the kernel's address from `KERNABLE_URL`, the key from `KERNABLE_KEY` and the TLS files from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`. Without them, it connects to the kernel you just started with `app.key` from the current directory, and the grant `--dev` put next to it. So the processes run unchanged in part 8, where each gets its own key and certificate. Options in the code take precedence over the environment.

Next: [2. Aggregates and commands](./2-commands).
