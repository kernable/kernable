#!/bin/sh
# Packs the Rust kit into the SDK crate (kit/) before `cargo publish`, so `kernable-sdk add kit`
# embeds the kit of exactly this version (see build.rs). Run from sdks/rust; remove kit/ after:
#
#   ./pack_kit.sh && cargo publish --allow-dirty && rm -rf kit
set -e
rm -rf kit
mkdir -p kit/tests
cp -R ../../kits/rust/src ../../kits/rust/examples ../../kits/rust/Cargo.toml ../../kits/rust/README.md kit/
cp ../../kits/rust/tests/fixtures.rs ../../kits/rust/tests/authorization.rs kit/tests/
cp -R ../../kits/fixtures kit/fixtures
echo "packed the kit into $(pwd)/kit"
