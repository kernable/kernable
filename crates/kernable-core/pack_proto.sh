#!/bin/sh
# Copies the protocol into the crate (proto/) before `cargo publish`, since a published crate
# holds only its own directory (see build.rs). Run from crates/kernable-core; remove proto/ after:
#
#   ./pack_proto.sh && cargo publish --allow-dirty && rm -rf proto
set -e
rm -rf proto
mkdir -p proto/kernable/v1
cp ../../proto/kernable/v1/*.proto proto/kernable/v1/
echo "packed the protocol into $(pwd)/proto"
