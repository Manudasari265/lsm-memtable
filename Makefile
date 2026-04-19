.PHONY: build check test fmt clean bench all

all: fmt check build test

build:
	cargo build

check:
	cargo clippy -- -D warnings

test:
	cargo test

test-verbose:
	cargo test -- --nocapture

fmt:
	cargo +nightly fmt --all

bench:
	cargo bench

clean:
	cargo clean
