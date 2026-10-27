# 1. Run a kernel

kernable splits an application in two. The **kernel** stores commands and events, signs and checks every message, runs commands one at a time per aggregate and delivers events in order. **Your processes** hold the business logic: they decide what a command means and what to do with an event. They connect to the kernel over gRPC, in any language.

## Build kernable

```sh
git clone <kernable repository> kernable && cd kernable
cargo build --release
make py                                   # Python SDK: venv and generated stubs
(cd sdks/typescript && npm install && npm pack --pack-destination /tmp)
sdks/python/.venv/bin/pip wheel sdks/python -w /tmp --no-deps
export PATH="$PWD/target/release:$PATH"   # kernable-kernel and kernable
```

## Keys and the kernel's configuration

Every party signs what it sends with its own Ed25519 key, and the kernel's allowlists say which key may do what. In this tutorial one key, `app`, does everything; part 8 shows how to split it.

```sh
mkdir -p ~/shop && cd ~/shop
kernable keys new kernel >/dev/null   # the kernel signs checkpoints of the event log
kernable keys new vault >/dev/null    # encrypts personal data (part 7)
APP=$(kernable keys new app)          # prints the key ID, the public half
```

Save this as `kernable.toml`, then put your key ID in: `sed -i.bak "s/<app>/$APP/g" kernable.toml`.

<<< @/examples/shop/kernable.dev.toml{toml}

Start the kernel and leave it running:

```sh
kernable-kernel kernable.toml
```

## Connect

Every process of the shop connects the same way, so put it in one place. It reads the kernel's address and keys from the environment (part 8) and otherwise uses the kernel you just started:

::: code-group
<<< @/examples/shop/ts/connect.ts [connect.ts]
<<< @/examples/shop/py/connect.py [connect.py]
:::

Install the SDK next to it:

::: code-group
```sh [TypeScript]
cd ~/shop
npm init -y >/dev/null && npm pkg set type=module
npm install /tmp/kernable-sdk-0.1.0.tgz
```
```sh [Python]
cd ~/shop
python3 -m venv .venv && .venv/bin/pip install -q /tmp/kernable-0.1.0-py3-none-any.whl
```
:::

Next: [2. Aggregates and commands](./2-commands).
