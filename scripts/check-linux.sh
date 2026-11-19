#!/usr/bin/env bash
# Runs a make target of this repository on a Linux server over SSH: the same checks as on this
# machine, on Linux and on a slower machine, where timing that never shows here shows. Run it
# before pushing to main, merging a pull request or releasing; GitHub's CI only adds to it.
#
#   scripts/check-linux.sh [<target>] [<source dir>]
#     <target>      a make target (default: check; `flaky` repeats the timing-sensitive tests)
#     <source dir>  the tree to test (default: this repository; e.g. a worktree of a pull request)
#   SERVER=root@<host>     the server, with root over SSH; or the line in
#                          ~/.config/kernable/check-linux.server (no address in the repository)
#   NAME=<name>            its directory there, /srv/kernable-check/<name> (default: from the
#                          source dir); kept between runs so builds are incremental
#
# The first run sets the server up (apt packages, protoc, rustup with the pinned toolchain,
# cargo-audit, Node 22): a few minutes. Exit code: make's.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARGET=${1:-check}
SOURCE=$(cd "${2:-$ROOT}" && pwd)
CONFIG=$HOME/.config/kernable/check-linux.server
SERVER=${SERVER:-$([ -f "$CONFIG" ] && tr -d '[:space:]' < "$CONFIG")}
[ -n "$SERVER" ] || { echo "set SERVER=root@<host> or write it into $CONFIG" >&2; exit 2; }
NAME=${NAME:-$(basename "$SOURCE")}
DIR=/srv/kernable-check/$NAME
NODE=v22.22.1

echo "== $SERVER: setting up (once)"
ssh -o BatchMode=yes "$SERVER" "NODE=$NODE bash -s" <<'SETUP'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
need=()
for p in build-essential pkg-config curl unzip rsync python3-venv git; do
  dpkg -s "$p" >/dev/null 2>&1 || need+=("$p")
done
if [ ${#need[@]} -gt 0 ]; then apt-get -qq update && apt-get -qq install -y "${need[@]}" >/dev/null; fi
if ! protoc --version 2>/dev/null | grep -q 28.3; then
  curl -fsSL --retry 3 -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip
  echo "0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f  /tmp/protoc.zip" | sha256sum -c - >/dev/null
  unzip -q -o /tmp/protoc.zip -d /usr/local bin/protoc 'include/*'
fi
[ -x "$HOME/.cargo/bin/rustup" ] || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null
. "$HOME/.cargo/env"
[ -x "$HOME/.cargo/bin/cargo-audit" ] || cargo install -q --locked cargo-audit
if [ ! -x "/opt/node-$NODE/bin/node" ]; then
  curl -fsSL "https://nodejs.org/dist/$NODE/node-$NODE-linux-x64.tar.xz" | tar -xJ -C /opt
  mv "/opt/node-$NODE-linux-x64" "/opt/node-$NODE"
fi
SETUP

echo "== $SOURCE -> $SERVER:$DIR"
ssh -o BatchMode=yes "$SERVER" "mkdir -p $DIR"
rsync -az --delete \
  --exclude '/target/' --exclude 'node_modules/' --exclude '.venv/' --exclude '/dist/' \
  --exclude '.vitepress/dist/' --exclude '.vitepress/cache/' --exclude '.DS_Store' \
  --exclude '/*.key' --exclude '/*.pub' --exclude '/*.grant' --exclude '/*.db*' \
  "$SOURCE/" "$SERVER:$DIR/"

# The server may serve other things: never fill its disk. Builds without debug info and without
# the incremental cache take about a third of the space; the tests run the same.
free=$(ssh -o BatchMode=yes "$SERVER" "df --output=avail -BG / | tail -1 | tr -dc 0-9")
if [ "$free" -lt 12 ]; then
  echo "only ${free} GB free on $SERVER: needs 12 (rm -rf $DIR/target to start afresh)" >&2
  exit 2
fi

echo "== make $TARGET on $SERVER ($(date -u +%H:%M:%S) UTC, ${free} GB free)"
ssh -o BatchMode=yes "$SERVER" "cd $DIR && . \$HOME/.cargo/env && export PATH=/opt/node-$NODE/bin:\$PATH \
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 && make $TARGET"
