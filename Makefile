.PHONY: check
check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
	cargo build --workspace
	cd sdks/typescript && npm install --no-audit --no-fund --silent && npm run -s check
