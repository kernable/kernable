.PHONY: test check check-rust check-crash check-sdks check-docs prune py parity check-packages flaky torture fuzz docs soak
# The simple tier: syntax, types and in-process tests, with no kernel or worker process and no
# real waiting. Runs in about a minute; `make check` runs everything.
test: py
	./scripts/check-open.sh
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --lib
	cargo test -p kernable-core -p kernable-store --tests
	cargo test -p kernable-sdk --test store
	cargo test -p kernable-kit --tests
	cd sdks/typescript && npm install --no-audit --no-fund --silent && npm test
	cd sdks/python && .venv/bin/python -m unittest discover -s tests/unit -t .
	cd kits/typescript && npm install --no-audit --no-fund --silent && npm test
	cd kits/python && PYTHONPATH=../../sdks/python ../../sdks/python/.venv/bin/python -m unittest discover -s tests -t .

# Everything, in four parts that CI runs as parallel jobs and scripts/check.sh side by side here
# (logs in target/check; after a failure it names the parts to run again).
check:
	./scripts/check.sh

# Rust: format, known vulnerable dependencies (cargo install cargo-audit; .cargo/audit.toml),
# lints, and every test: with cargo-nextest (cargo install cargo-nextest --locked; .config/nextest.toml),
# each test in its own process and the test binaries side by side, then the doc tests, which
# nextest does not run. RERUN=1 runs again only the tests that failed or did not finish last time.
NEXTEST = NEXTEST_EXPERIMENTAL_RECORD=1 cargo nextest run --user-config-file .config/nextest-record.toml
check-rust: prune
	cargo fmt --all --check
	cargo audit -q
	cargo clippy --workspace --all-targets -- -D warnings
	cargo build -q --workspace --bin kernable
	KERNABLE_TEST_KERNEL=$(CURDIR)/target/debug/kernable $(NEXTEST) --workspace --no-tests=pass $(if $(RERUN),--rerun latest)
	$(if $(RERUN),,cargo test -q --workspace --doc)

# The real kernel aborted at every crash point (--features crash-points).
check-crash: prune
	CARGO_TARGET_DIR=target/crash cargo clippy -p kernable-kernel --all-targets --features crash-points -- -D warnings
	CARGO_TARGET_DIR=target/crash cargo test -p kernable-kernel --features crash-points --test crashes
	CARGO_TARGET_DIR=target/crash cargo test -p kernable-kernel --features crash-points --test replication_crashes
	CARGO_TARGET_DIR=target/crash cargo test -p kernable-kernel --features crash-points --test move_crashes

# The TypeScript and Python SDKs against a real kernel; then the kits' tests
# that need a kernel (the TypeScript kit's `npm run check`, the Python kit's tests/e2e_*.py).
check-sdks: prune py
	cargo build --workspace
	cargo test -p kernable-kit --test e2e -- --ignored
	cd sdks/typescript && npm install --no-audit --no-fund --silent && npm run -s check
	cd sdks/python && .venv/bin/python -m unittest discover tests
	cd kits/typescript && npm install --no-audit --no-fund --silent && npm run -s check
	cd kits/python && PYTHONPATH=../../sdks/python ../../sdks/python/.venv/bin/python -m unittest discover -s tests -t . -p 'e2e_*.py'
	$(MAKE) parity

# Kit parity: one scenario through the TypeScript, Python and Rust kits, the kit's domains hosted
# in one language and driven through another's KitApi, every pair; every run's results, mails and
# stored events must equal the TypeScript/TypeScript run's (kits/parity/README.md).
parity: py
	cargo build -p kernable-kernel
	cargo build -p kernable-kit --example parity
	cd kits/typescript && npm install --no-audit --no-fund --silent
	node kits/parity/scenario.ts
	node --conditions=source --no-warnings kits/parity/run.ts

# The documentation: generated reference up to date, the site builds, every example runs.
check-docs: prune py
	cargo build --workspace
	cd sdks/typescript && npm install --no-audit --no-fund --silent
	cd kits/typescript && npm install --no-audit --no-fund --silent
	cd docs && npm install --no-audit --no-fund --silent && npm run -s check

# The documentation site at http://localhost:5173; `npm run reference` in docs/ regenerates the
# reference after a change to the code it is generated from.
docs:
	cd docs && npm install --no-audit --no-fund --silent && npm run -s reference && npx vitepress dev .

# Python SDK: venv (needs network the first time; with the Python kit's dependencies, for the
# tutorial's part 9) and generated stubs.
py:
	cd sdks/python && { test -d .venv || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt -r ../../kits/python/requirements.txt; }; } \
		&& .venv/bin/python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. kernable/v1/kernable.proto

# Builds the npm and Python packages and runs them, installed, against a kernel (needs network).
check-packages:
	./scripts/check-packages.sh

# The tests that depend on timing, FLAKY times each, with every CPU kept busy meanwhile as on a
# loaded CI machine: a test that fails only sometimes fails here first.
FLAKY ?= 5
flaky: py
	cargo build -q --workspace
	cd kits/typescript && npm install --no-audit --no-fund --silent
	./scripts/flaky.sh $(FLAKY)

# Long robustness runs: every crash point ROUNDS times, KILLS random kills under
# load, SEEDS storage-fault seeds, MODEL_SEEDS model-based runs of 300 steps, CASES property
# cases.
ROUNDS ?= 20
KILLS ?= 200
SEEDS ?= 200
MODEL_SEEDS ?= 100
CASES ?= 10000
torture:
	CARGO_TARGET_DIR=target/crash KERNABLE_CRASH_ROUNDS=$(ROUNDS) KERNABLE_KILLS=$(KILLS) cargo test -p kernable-kernel --features crash-points --test crashes
	CARGO_TARGET_DIR=target/crash KERNABLE_CRASH_ROUNDS=$(ROUNDS) cargo test -p kernable-kernel --features crash-points --test replication_crashes
	KERNABLE_KILLS=$(KILLS) cargo test -p kernable-kernel --test replication_kills
	KERNABLE_FAULT_SEEDS=$(SEEDS) cargo test -p kernable-kernel --test storage
	PROPTEST_CASES=$(CASES) cargo test -p kernable-store --test props
	KERNABLE_MODEL_SEEDS=$(MODEL_SEEDS) KERNABLE_MODEL_STEPS=300 cargo test -p kernable-kernel --test model
	PROPTEST_CASES=$(CASES) cargo test -p kernable-core --test layouts
	PROPTEST_CASES=$(CASES) cargo test -p kernable-kernel --test props

# The soak: the kernel and an app per SDK under load and random faults for SOAK_MINUTES, every
# invariant checked every 10 minutes (crates/kernable-soak; more options in its main.rs).
# SOAK_PAIR=yes runs a pair with a witness instead of one kernel, and fails it over.
# scripts/soak-server.sh runs it on a Linux server, with disk-full and power-loss faults.
SOAK_MINUTES ?= 60
SOAK_BACKEND ?= sqlite
SOAK_PAIR ?= no
soak: py
	cargo build --release -p kernable-kernel -p kernable-soak
	target/release/kernable-soak run --minutes $(SOAK_MINUTES) --backend $(SOAK_BACKEND) --pair $(SOAK_PAIR) --dir target/soak-$$(date +%Y%m%d-%H%M%S)

# Coverage-guided fuzzing, FUZZ_SECONDS per target (needs nightly Rust and cargo-fuzz).
# Findings land in fuzz/artifacts/<target>; add them to fuzz/seeds/<target> once fixed.
FUZZ_SECONDS ?= 300
fuzz:
	cd fuzz && for t in messages config grants http registrations store_files; do \
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
