# 1. Run a kernel

kernable splits an application in two. The **kernel** stores commands and events, signs and checks every message, runs commands one at a time per aggregate and delivers events in order. **Your processes** hold the business logic: they decide what a command means and what to do with an event. They connect to the kernel over gRPC, in any language.

## Install

Install the kernel as in [Getting started](../getting-started#_1-install-the-kernel):

```sh
curl -fsSL https://kernable.com/install.sh | sh
```

The script installs the kernel on macOS and Linux; on Windows, run it in WSL.

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
python3 -m venv .venv && .venv/bin/pip install -q kernable-sdk
```
```sh [Rust]
cd ~/shop
cargo init -q --lib --name shop
cargo add -q kernable-sdk serde_json scrypt
cargo add -q serde --features derive
cargo add -q tokio --features macros,rt-multi-thread
```
```sh [Go]
cd ~/shop
go mod init shop
go get github.com/kernable/kernable/sdks/go golang.org/x/crypto
```
:::

In Rust, `domain.rs` (part 2) lives in `src/` and `src/lib.rs` shares it; every process and script is a binary in `src/bin/` (run with `cargo run -q --bin worker`):

<<< @/examples/shop/rs/src/lib.rs

In Go, `domain.go` (part 2) is the package `shop` at the top of the module; every process and script is a program in a directory of its own, `worker/main.go` say (run with `go run ./worker`).

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
```go [Go]
client, err := kernable.New(kernable.Options{})
```
:::

The SDK takes the kernel's address from `KERNABLE_URL`, the key from `KERNABLE_KEY` and the TLS files from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`. Without them, it connects to the kernel you just started with `app.key` from the current directory, and the grant `--dev` put next to it. So the processes run unchanged in part 8, where each gets its own key and certificate. Options in the code take precedence over the environment.

Workers and listeners print a line to stderr when they connect, with the kernel's address, the key and what they run. `KERNABLE_LOG=debug` adds every command, event and query they handle, `KERNABLE_LOG=warn` keeps only problems, `off` silences them.

Next: [2. Aggregates and commands](./2-commands).
