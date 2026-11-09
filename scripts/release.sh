#!/usr/bin/env bash
# Builds a release of the kernel into dist/<version>/, uploads nothing:
#   kernable-<version>-<target>.tar.gz   the `kernable` binary, README.md and the licenses
#   SHA256SUMS                           what scripts/install.sh checks
#   image-<arch>.tar                     the container image (Dockerfile), with IMAGE=1
# Targets: aarch64-apple-darwin and x86_64-apple-darwin (cargo, on macOS), x86_64- and
# aarch64-unknown-linux-gnu (cargo zigbuild, linked against glibc 2.31: Debian 11, Ubuntu 20.04
# and newer).
#   scripts/release.sh <version> [<target>...]     (the version must be the workspace's)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
VERSION=${1:?usage: scripts/release.sh <version> [<target>...]}
shift
TARGETS=("$@")
[ ${#TARGETS[@]} -gt 0 ] || TARGETS=(aarch64-apple-darwin x86_64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)
CARGO_VERSION=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' "$ROOT/Cargo.toml")
[ "$VERSION" = "$CARGO_VERSION" ] || { echo "the workspace's version is $CARGO_VERSION, not $VERSION" >&2; exit 2; }
DIST=$ROOT/dist/$VERSION
mkdir -p "$DIST"
cd "$ROOT"

for target in "${TARGETS[@]}"; do
  echo "== $target"
  rustup target add "$target" >/dev/null 2>&1 || true
  case $target in
    *-apple-darwin) cargo build --release --locked -p kernable-kernel --bin kernable --target "$target" ;;
    *-linux-gnu) cargo zigbuild --release --locked -p kernable-kernel --bin kernable --target "$target.2.31" ;;
    *) echo "no way to build $target here" >&2; exit 2 ;;
  esac
  stage=$(mktemp -d)
  name=kernable-$VERSION-$target
  mkdir "$stage/$name"
  cp "target/$target/release/kernable" README.md LICENSE "$stage/$name/"
  COPYFILE_DISABLE=1 tar --no-xattrs -czf "$DIST/$name.tar.gz" -C "$stage" "$name"
  rm -rf "$stage"
done

(cd "$DIST" && shasum -a 256 kernable-*.tar.gz > SHA256SUMS)
echo "== $DIST"
cat "$DIST/SHA256SUMS"

if [ "${IMAGE:-0}" = 1 ]; then
  for arch in amd64 arm64; do
    target=$([ $arch = amd64 ] && echo x86_64 || echo aarch64)-unknown-linux-gnu
    [ -f "target/$target/release/kernable" ] || continue
    echo "== the image for linux/$arch"
    ctx=$(mktemp -d)
    cp Dockerfile "target/$target/release/kernable" "$ctx/"
    docker build -q --platform "linux/$arch" -t "kernable:$VERSION-$arch" "$ctx" >/dev/null
    docker save "kernable:$VERSION-$arch" -o "$DIST/image-$arch.tar"
    rm -rf "$ctx"
  done
fi
