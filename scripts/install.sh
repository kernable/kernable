#!/bin/sh
# Installs the `kernable` binary: served as https://kernable.com/install.sh.
#   curl -fsSL https://kernable.com/install.sh | sh
#   curl -fsSL https://kernable.com/install.sh | KERNABLE_VERSION=0.1.0 sh
# It picks the archive for this system, checks it against the release's SHA256SUMS, and puts the
# binary into KERNABLE_INSTALL (default ~/.local/bin). KERNABLE_DOWNLOAD names where releases
# lie (default https://kernable.com/download): <download>/<version>/ and <download>/latest.
set -eu
DOWNLOAD=${KERNABLE_DOWNLOAD:-https://kernable.com/download}
DEST=${KERNABLE_INSTALL:-$HOME/.local/bin}
fail() { echo "kernable install: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "it needs $1"; }
need curl
need tar
case "$(uname -s)" in
  Darwin) os=apple-darwin ;;
  Linux) os=unknown-linux-gnu ;;
  *) fail "no release for $(uname -s): build it from the source (cargo build --release -p kernable-kernel)" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  arm64 | aarch64) arch=aarch64 ;;
  *) fail "no release for $(uname -m)" ;;
esac
VERSION=${KERNABLE_VERSION:-$(curl -fsSL "$DOWNLOAD/latest")} || fail "cannot reach $DOWNLOAD"
name=kernable-$VERSION-$arch-$os
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "kernable $VERSION for $arch-$os"
curl -fsSL "$DOWNLOAD/$VERSION/$name.tar.gz" -o "$tmp/$name.tar.gz" || fail "no $name.tar.gz in $DOWNLOAD/$VERSION"
curl -fsSL "$DOWNLOAD/$VERSION/SHA256SUMS" -o "$tmp/SHA256SUMS" || fail "no SHA256SUMS in $DOWNLOAD/$VERSION"
want=$(grep " $name.tar.gz\$" "$tmp/SHA256SUMS" | cut -d' ' -f1)
[ -n "$want" ] || fail "SHA256SUMS does not name $name.tar.gz"
if command -v sha256sum >/dev/null 2>&1; then
  got=$(sha256sum "$tmp/$name.tar.gz" | cut -d' ' -f1)
else
  got=$(shasum -a 256 "$tmp/$name.tar.gz" | cut -d' ' -f1)
fi
[ "$got" = "$want" ] || fail "$name.tar.gz does not match SHA256SUMS: not installed"
tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
mkdir -p "$DEST"
install -m 755 "$tmp/$name/kernable" "$DEST/kernable"
echo "installed $("$DEST/kernable" --version) to $DEST/kernable"
case ":$PATH:" in
  *":$DEST:"*) ;;
  *)
    # The file the user's login shell reads: zsh and bash do not read ~/.profile when they have
    # their own, and bash on Linux reads ~/.bashrc in each terminal.
    case "$(basename "${SHELL:-sh}")" in
      zsh) profile="~/.zprofile" ;;
      bash) if [ "$(uname -s)" = Darwin ]; then profile="~/.bash_profile"; else profile="~/.bashrc"; fi ;;
      *) profile="~/.profile" ;;
    esac
    echo "add $DEST to your PATH, e.g. echo 'export PATH=\"$DEST:\$PATH\"' >> $profile" ;;
esac
