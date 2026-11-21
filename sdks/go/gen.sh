#!/bin/sh
# Generates the protocol's Go code (internal/pb) from proto/kernable/v1/kernable.proto, with
# pinned plugins. The result is committed: users of the SDK need no protoc.
set -eu
cd "$(dirname "$0")"
BIN=$(mktemp -d)
trap 'rm -rf "$BIN"' EXIT
GOBIN=$BIN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
GOBIN=$BIN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
M=Mkernable/v1/kernable.proto=github.com/kernable/kernable/sdks/go/internal/pb
mkdir -p internal/pb
PATH=$BIN:$PATH protoc -I ../../proto \
  --go_out=internal/pb --go_opt=paths=source_relative --go_opt=$M \
  --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative --go-grpc_opt=$M \
  kernable/v1/kernable.proto
mv internal/pb/kernable/v1/*.go internal/pb/
rm -r internal/pb/kernable
# What the code was generated from: a test fails once the proto changes without this script.
shasum -a 256 ../../proto/kernable/v1/kernable.proto | cut -d' ' -f1 > internal/pb/kernable.proto.sha256
