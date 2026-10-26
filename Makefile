.PHONY: check py check-packages
check: py
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
	KERNABLE_TEST_STORE=file cargo test -p kernable-kernel
	cargo build --workspace
	cd sdks/typescript && npm install --no-audit --no-fund --silent && npm run -s check \
		&& KERNABLE_TEST_STORE=file npm run -s check
	cd sdks/python && .venv/bin/python -m unittest discover tests \
		&& KERNABLE_TEST_STORE=file .venv/bin/python -m unittest discover tests

# Python SDK: venv (needs network the first time) and generated stubs.
py:
	cd sdks/python && { test -d .venv || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }; } \
		&& .venv/bin/python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. kernable/v1/kernable.proto

# Builds the npm and Python packages and runs them, installed, against a kernel (needs network).
check-packages:
	./scripts/check-packages.sh
