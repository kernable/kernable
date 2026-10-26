.PHONY: check check-py
check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
	KERNABLE_TEST_STORE=file cargo test -p kernable-kernel
	cargo build --workspace
	cd sdks/typescript && npm install --no-audit --no-fund --silent && npm run -s check \
		&& KERNABLE_TEST_STORE=file npm run -s check

# Python worker; needs Python 3.10+ and network for the first install.
check-py:
	cargo build --workspace
	cd examples/order-py && python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt \
		&& mkdir -p gen && .venv/bin/python -m grpc_tools.protoc -I../../proto --python_out=gen --grpc_python_out=gen kernable/v1/kernable.proto \
		&& .venv/bin/python -m unittest test_order.py
