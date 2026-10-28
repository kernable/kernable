.PHONY: check prune py check-packages torture fuzz docs
check: prune py
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	CARGO_TARGET_DIR=target/crash cargo clippy -p kernable-kernel --all-targets --features crash-points -- -D warnings
	cargo test --workspace
	KERNABLE_TEST_STORE=file cargo test --workspace --lib --tests
	CARGO_TARGET_DIR=target/crash cargo test -p kernable-kernel --features crash-points --test crashes
	CARGO_TARGET_DIR=target/crash KERNABLE_TEST_STORE=file cargo test -p kernable-kernel --features crash-points --test crashes
	cargo build --workspace
	cd sdks/typescript && npm install --no-audit --no-fund --silent && npm run -s check \
		&& KERNABLE_TEST_STORE=file npm run -s check
	cd sdks/python && .venv/bin/python -m unittest discover tests \
		&& KERNABLE_TEST_STORE=file .venv/bin/python -m unittest discover tests
	cd docs && npm install --no-audit --no-fund --silent && npm run -s check

# The documentation site at http://localhost:5173; `npm run reference` in docs/ regenerates the
# reference after a change to the code it is generated from.
docs:
	cd docs && npm install --no-audit --no-fund --silent && npm run -s reference && npx vitepress dev .

# Python SDK: venv (needs network the first time) and generated stubs.
py:
	cd sdks/python && { test -d .venv || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }; } \
		&& .venv/bin/python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. kernable/v1/kernable.proto

# Builds the npm and Python packages and runs them, installed, against a kernel (needs network).
check-packages:
	./scripts/check-packages.sh

# Long robustness runs: every crash point ROUNDS times per backend, CASES property cases.
ROUNDS ?= 20
CASES ?= 10000
torture:
	CARGO_TARGET_DIR=target/crash KERNABLE_CRASH_ROUNDS=$(ROUNDS) cargo test -p kernable-kernel --features crash-points --test crashes
	CARGO_TARGET_DIR=target/crash KERNABLE_CRASH_ROUNDS=$(ROUNDS) KERNABLE_TEST_STORE=file cargo test -p kernable-kernel --features crash-points --test crashes
	PROPTEST_CASES=$(CASES) cargo test -p kernable-store --test props
	PROPTEST_CASES=$(CASES) cargo test -p kernable-kernel --test props
	PROPTEST_CASES=$(CASES) KERNABLE_TEST_STORE=file cargo test -p kernable-kernel --test props

# Coverage-guided fuzzing, FUZZ_SECONDS per target (needs nightly Rust and cargo-fuzz).
# Findings land in fuzz/artifacts/<target>; add them to fuzz/seeds/<target> once fixed.
FUZZ_SECONDS ?= 300
fuzz:
	cd fuzz && for t in file_log log_records messages; do \
		mkdir -p corpus/$$t seeds/$$t && \
		cargo +nightly fuzz run $$t corpus/$$t seeds/$$t -- -max_total_time=$(FUZZ_SECONDS) || exit 1; \
	done

# Cargo never removes outdated build artifacts. A fresh build has about 40,000 files in
# target/debug/deps; once it has grown far past that, every test binary starts slowly (the
# directory is on their library search path), so start over.
PRUNE_FILES ?= 150000
prune:
	@n=$$(ls -f target/debug/deps 2>/dev/null | wc -l | tr -d " "); \
	if [ "$$n" -gt $(PRUNE_FILES) ]; then echo "target/debug/deps has $$n files: cargo clean"; cargo clean; fi
